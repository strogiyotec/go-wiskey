package wiskey

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

type LsmTree struct {
	rwm        sync.RWMutex
	sstableDir string    //directory with sstables
	log        *vlog     //vlog
	memtable   *Memtable //in memory table
	sstables   []string  //list of created sstables,let's change it to set to speed up the search
	deleted    map[string]bool
}

func NewLsmTree(log *vlog, sstableDir string, memtable *Memtable) *LsmTree {
	lsm := &LsmTree{
		log:        log,
		sstableDir: sstableDir,
		memtable:   memtable,
		deleted:    make(map[string]bool),
	}
	//create sstable path if doesn't exist
	if _, err := os.Stat(sstableDir); os.IsNotExist(err) {
		err := os.Mkdir(sstableDir, os.ModeDir|0755)
		if err != nil {
			panic(err)
		}
	}
	lsm.fillSstables()
	err := lsm.restore()
	if err != nil {
		fmt.Print(err.Error())
		panic(err)
	}
	//run job to periodically merge sstables
		go func(tree *LsmTree) {
			fmt.Println("Gc thread was initialized")
			for true {
				time.Sleep(30 * time.Second)
				fmt.Println("SSTABLE GC started")
				err := lsm.Merge()
				if err != nil {
					fmt.Println("Gc encountered an error " + err.Error() + " Stop gc thread")
					return
				}
			}
		}(lsm)
	return lsm
}

func (lsm *LsmTree) Merge() error {
	lsm.rwm.Lock()
	defer lsm.rwm.Unlock()
	var newSstableFiles []string
	index := 0
	if len(lsm.sstables)%2 == 0 {
		for index < len(lsm.sstables) {
			firstReader, _ := os.Open(lsm.sstables[index])
			secondReader, _ := os.Open(lsm.sstables[index+1])
			firstSStable := ReadTable(firstReader, lsm.log)
			secondSStable := ReadTable(secondReader, lsm.log)
			filePath, err := lsm.mergeFiles(firstSStable, secondSStable)
			if err != nil {
				return err
			}
			newSstableFiles = append(newSstableFiles, filePath)
			firstSStable.Close()
			secondSStable.Close()
			err = os.Remove(lsm.sstables[index])
			if err != nil {
				return err
			}
			err = os.Remove(lsm.sstables[index+1])
			if err != nil {
				return err
			}
			index += 2
		}
	}
	lsm.sstables = newSstableFiles
	return nil
}
func (lsm *LsmTree) Get(key []byte) ([]byte, bool) {
	lsm.rwm.RLock()
	defer lsm.rwm.RUnlock()
	_, ok := lsm.deleted[string(key)]
	if ok {
		return nil, false
	}
	meta, found := lsm.memtable.Get(key)
	//first check in memory table
	if found {
		entry, err := lsm.log.Get(*meta)
		if err != nil {
			panic(err)
		}
		if len(entry.value) == len(tombstone) && bytes.Compare(entry.value, []byte(tombstone)) == 0 {
			return nil, false
		} else {
			return entry.value, true
		}
	} else {
		//if not in memory then try to find in sstables
		//multiple sstables can have the same key
		//choose the one with the latest timestamp
		foundEntry, found := lsm.findInSStables(key)
		if !found {
			return nil, false
		} else {
			//if the value in vlog is tombstone it means that value was deleted
			if len(foundEntry.value) == len(tombstone) && bytes.Compare(foundEntry.value, []byte(tombstone)) == 0 {
				return nil, false
			} else {
				return foundEntry.value, true
			}
		}
	}
}

//Save tombstone in vlog
func (lsm *LsmTree) Delete(key []byte) error {
	lsm.rwm.Lock()
	defer lsm.rwm.Unlock()
	_, ok := lsm.deleted[string(key)]
	//already deleted and it's still in memory
	if ok {
		return nil
	}
	lsm.deleted[string(key)] = true
	return lsm.save(DeletedEntry(key))
}

//save entry in vlog first then in sstable
func (lsm *LsmTree) Put(entry *TableEntry) error {
	lsm.rwm.Lock()
	defer lsm.rwm.Unlock()
	//before put let's delete this key from deleted map
	delete(lsm.deleted, string(entry.key))
	return lsm.save(entry)
}

//Flush in memory red black tree to sstable on disk
func (lsm *LsmTree) Flush() error {
	sstablePath := lsm.sstableDir + "/" + RandStringBytes(sstableFileLength) + ".sstable"
	file, err := os.OpenFile(sstablePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	writer := NewWriter(file, uint32(20))
	err = lsm.memtable.Flush(writer)
	if err != nil {
		return err
	}
	err = writer.Close()
	if err != nil {
		return err
	}
	err = lsm.log.FlushHead()
	if err != nil {
		return err
	}
	lsm.sstables = append(lsm.sstables, sstablePath)
	return nil
}

func (lsm *LsmTree) save(entry *TableEntry) error {
	meta, err := lsm.log.Append(entry)
	if err != nil {
		return err
	}
	err = lsm.memtable.Put(entry.key, meta)
	if err != nil {
		return err
	}
	if lsm.memtable.isFull() {
		err := lsm.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}
func (lsm *LsmTree) findInSStables(key []byte) (*SearchEntry, bool) {
	var latestEntry *SearchEntry
	for _, tablePath := range lsm.sstables {
		reader, _ := os.Open(tablePath)
		sstable := ReadTable(reader, lsm.log)
		searchEntry, found := sstable.Get(key)
		if found {
			if latestEntry == nil {
				latestEntry = searchEntry
			} else {
				if searchEntry.timestamp > latestEntry.timestamp {
					latestEntry = searchEntry
				}
			}
		}
	}
	return latestEntry, latestEntry != nil
}

func (lsm *LsmTree) restore() error {
	reader, err := os.OpenFile(lsm.log.checkpoint, os.O_RDONLY, 0666)
	//if file doesn't exist
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else {
		defer reader.Close()
		stat, err := reader.Stat()
		if err != nil {
			return err
		}
		//if empty => skip
		if stat.Size() == int64(0) {
			return nil
		} else {
			headBuffer := make([]byte, uint32Size)
			_, err := reader.Read(headBuffer)
			if err != nil {
				return err
			}
			headOffset := binary.BigEndian.Uint32(headBuffer)
			return lsm.log.RestoreTo(headOffset, lsm.memtable)
		}
	}
}

//save all sstable paths in memory
func (lsm *LsmTree) fillSstables() {
	//if sstable dir exists then try to get all sstable files from it
	if _, err := os.Stat(lsm.sstableDir); !os.IsNotExist(err) {
		err := filepath.Walk(
			lsm.sstableDir,
			func(path string, f os.FileInfo, _ error) error {
				if !f.IsDir() {
					r, err := regexp.MatchString(sstableExtension, f.Name())
					if err == nil && r {
						lsm.sstables = append(lsm.sstables, lsm.sstableDir+"/"+f.Name())
					}
				}
				return nil
			},
		)
		if err != nil {
			panic(err)
		}
	}
}

func (lsm *LsmTree) mergeFiles(first *SSTable, second *SSTable) (string, error) {
	sstablePath := lsm.sstableDir + "/" + RandStringBytes(sstableFileLength) + ".sstable"
	file, err := os.OpenFile(sstablePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return "", err
	}
	writer := NewWriter(file, uint32(20))
	var i1, i2 int
	for i1 < len(first.indexes) && i2 < len(second.indexes) {
		firstReader := NewReader(first.reader, int64(first.indexes[i1].Offset))
		secondReader := NewReader(second.reader, int64(second.indexes[i2].Offset))
		firstKey := string(firstReader.readKey(firstReader.readKeyLength()))
		secondKey := string(secondReader.readKey(secondReader.readKeyLength()))
		compare := strings.Compare(firstKey, secondKey)
		if compare > 0 {
			timestamp := secondReader.readTimestamp()
			offset := secondReader.readValueOffset()
			length := secondReader.readValueLength()
			_, err := writer.WriteEntry(&sstableEntry{key: []byte(secondKey), timeStamp: timestamp, valueOffset: offset, valueLength: length})
			if err != nil {
				return "", err
			}
			i2++
		} else if compare < 0 {
			timestamp := firstReader.readTimestamp()
			offset := firstReader.readValueOffset()
			length := firstReader.readValueLength()
			_, err := writer.WriteEntry(&sstableEntry{key: []byte(firstKey), timeStamp: timestamp, valueOffset: offset, valueLength: length})
			if err != nil {
				return "", err
			}
			i1++
		} else {
			firstTm := firstReader.readTimestamp()
			secondTm := secondReader.readTimestamp()
			if firstTm > secondTm {
				offset := firstReader.readValueOffset()
				length := firstReader.readValueLength()
				_, err := writer.WriteEntry(&sstableEntry{key: []byte(firstKey), timeStamp: firstTm, valueOffset: offset, valueLength: length})
				if err != nil {
					return "", err
				}
			} else {
				offset := secondReader.readValueOffset()
				length := secondReader.readValueLength()
				_, err := writer.WriteEntry(&sstableEntry{key: []byte(firstKey), timeStamp: secondTm, valueOffset: offset, valueLength: length})
				if err != nil {
					return "", err
				}
			}
			i1++
			i2++
		}
	}
	for i1 < len(first.indexes) {
		reader := NewReader(first.reader, int64(first.indexes[i1].Offset))
		key := reader.readKey(reader.readKeyLength())
		timestamp := reader.readTimestamp()
		offset := reader.readValueOffset()
		length := reader.readValueLength()
		_, err := writer.WriteEntry(&sstableEntry{key: key, timeStamp: timestamp, valueOffset: offset, valueLength: length})
		if err != nil {
			return "", err
		}
		i1++
	}
	for i2 < len(second.indexes) {
		reader := NewReader(second.reader, int64(second.indexes[i2].Offset))
		key := reader.readKey(reader.readKeyLength())
		timestamp := reader.readTimestamp()
		offset := reader.readValueOffset()
		length := reader.readValueLength()
		_, err := writer.WriteEntry(&sstableEntry{key: key, timeStamp: timestamp, valueOffset: offset, valueLength: length})
		if err != nil {
			return "", err
		}
		i2++
	}
	err = writer.Close()
	if err != nil {
		return "", err
	}
	return sstablePath, nil
}
