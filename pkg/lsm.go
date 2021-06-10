package wiskey

import "os"

type lsmTree struct {
	sstableDir string    //directory with sstables
	log        *vlog     //vlog
	memtable   *Memtable //in memory table
	sstables   []string  //list of created sstables
}

func NewLsmTree(log *vlog, sstableDir string, memtable *Memtable) *lsmTree {
	return &lsmTree{log: log, sstableDir: sstableDir, memtable: memtable}
}

func (lsm *lsmTree) Get(key []byte) ([]byte, bool) {
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
		//choose the one with latest timestamp
		foundEntry, found := lsm.findInSStables(key)
		if !found {
			return nil, false
		} else {
			if foundEntry.deleted == deleted {
				return nil, false
			} else {
				return foundEntry.value, true
			}
		}
	}
}

//save entry in vlog first then in sstable
//TODO: should it be saved in vlog ?
func (lsm *lsmTree) Delete(key []byte) error {
	lsm.memtable.Delete(key)
	if lsm.memtable.isFull() {
		err := lsm.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

//save entry in vlog first then in sstable
func (lsm *lsmTree) Put(entry *TableEntry) error {
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
func (lsm *lsmTree) Flush() error {
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
	lsm.sstables = append(lsm.sstables, sstablePath)
	return nil
}

func (lsm *lsmTree) findInSStables(key []byte) (*SearchEntry, bool) {
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
