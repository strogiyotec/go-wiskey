package wiskey

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

type LsmTree struct {
	sstableDir string    //directory with sstables
	log        *vlog     //vlog
	memtable   *Memtable //in memory table
	sstables   []string  //list of created sstables
	deleted    map[string]bool
}

func NewLsmTree(log *vlog, sstableDir string, memtable *Memtable) *LsmTree {
	lsm := &LsmTree{
		log:        log,
		sstableDir: sstableDir,
		memtable:   memtable,
		deleted:    make(map[string]bool),
	}
	lsm.fillSstables()
	err := lsm.restore()
	if err != nil {
		fmt.Print(err.Error())
		panic(err)
	}
	return lsm
}

func (lsm *LsmTree) Get(key []byte) ([]byte, bool) {
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
		return entry.value, true
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
	lsm.deleted[string(key)] = true
	return lsm.Put(DeletedEntry(key))
}

//save entry in vlog first then in sstable
func (lsm *LsmTree) Put(entry *TableEntry) error {
	delete(lsm.deleted, string(entry.key))
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

//Flush in memory red black tree to sstable on disk
func (lsm *LsmTree) Flush() error {
	sstablePath := lsm.sstableDir + "/" + RandStringBytes(10) + ".sstable"
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
