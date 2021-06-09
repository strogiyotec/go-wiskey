package wiskey

import "os"

type lsmTree struct {
	sstablePath string
	log         *vlog
	memtable    *Memtable
	sstables    []string //list of created sstables
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

func (lsm *lsmTree) Flush() error {
	sstablePath := lsm.sstablePath + "/" + RandStringBytes(10) + ".sstable"
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
