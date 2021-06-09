package wiskey

import (
	"errors"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"strings"
)

const (
	tombstone = "THOMB" //in lsm tree , when key is deleted the value is marked as tombstone
)

//in memory redblack tree
type Memtable struct {
	tree    *rbt.Tree //red black tree where key is a string and value is ValueMeta that shows where value is stored in vlog
	size    int       // size of in memory redblack tree in bytes
	maxSize int       //max size of the tree before flushing it
}

func NewMemTable(maxSize int) *Memtable {
	return &Memtable{tree: rbt.NewWithStringComparator(), maxSize: maxSize}
}

//Flush in memory table to given sstable writer
func (memtable *Memtable) Flush(writer *SSTableWriter) error {
	iterator := memtable.tree.Iterator()
	iterator.Begin()
	for iterator.Next() {
		key := iterator.Key().(string)
		value := iterator.Value().(*ValueMeta)
		_, err := writer.WriteEntry(NewSStableEntry([]byte(key), value))
		if err != nil {
			return err
		}
	}
	err := writer.Close()
	if err != nil {
		return err
	}
	memtable.tree.Clear()
	return nil
}

func (memtable *Memtable) Put(key []byte, value *ValueMeta) error {
	if strings.Compare(string(key), tombstone) == 0 {
		return errors.New("can't use this key, it's reserved as tombstone")
	}
	memtable.tree.Put(string(key), value)
	memtable.increaseSize(key)
	if memtable.isFull() {

	}
	return nil
}

func (memtable *Memtable) Get(key []byte) (*ValueMeta, bool) {
	value, found := memtable.tree.Get(string(key))
	if found {
		return value.(*ValueMeta), true
	} else {
		return nil, false
	}
}

func (memtable *Memtable) Delete(key []byte) {
	memtable.tree.Put(string(key), tombstone)
}

func (memtable *Memtable) isFull() bool {
	return memtable.size > memtable.maxSize
}

func (memtable *Memtable) increaseSize(key []byte) {
	memtable.size += len(key)
	memtable.size += uint32Size * 2 //add offset + length from the meta
}
