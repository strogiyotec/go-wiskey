package wiskey

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"
)

const (
	deleted     = 1
	non_deleted = 0
)

// SSTABLE Entry
type sstableEntry struct {
	key         []byte //key
	timeStamp   uint64 //when it was created
	deleted     byte   //if it was deleted
	valueOffset uint32 //offset of the value to read
	valueLength uint32 //the length of the value
}

func DeletedSstableEntry(entry TableEntry) *sstableEntry {
	return nil
}

func NewSStableEntry(entry *TableEntry, meta *ValueMeta) *sstableEntry {
	return &sstableEntry{
		key:         entry.key,
		timeStamp:   uint64(time.Now().Unix()),
		deleted:     0,
		valueOffset: meta.offset,
		valueLength: meta.length,
	}
}

//write entry to sstable
//Format [key length + key +  timestamp + meta + offset + length]
func (entry *sstableEntry) writeTo(writer io.Writer) (uint32, error) {
	buffer := bytes.NewBuffer([]byte{})
	//key length
	if err := binary.Write(buffer, binary.BigEndian, uint32(len(entry.key))); err != nil {
		return 0, err
	}
	//key
	if err := binary.Write(buffer, binary.BigEndian, entry.key); err != nil {
		return 0, err
	}
	//timestamp
	if err := binary.Write(buffer, binary.BigEndian, entry.timeStamp); err != nil {
		return 0, err
	}
	//if was deleted just store meta value
	if entry.deleted == 1 {
		if err := binary.Write(buffer, binary.BigEndian, byte(deleted)); err != nil {
			return 0, err
		}
	} else {
		//not deleted
		if err := binary.Write(buffer, binary.BigEndian, byte(non_deleted)); err != nil {
			return 0, err
		}
		//offset
		if err := binary.Write(buffer, binary.BigEndian, entry.valueOffset); err != nil {
			return 0, err
		}
		//length
		if err := binary.Write(buffer, binary.BigEndian, entry.valueLength); err != nil {
			return 0, err
		}
	}
	length, err := writer.Write(buffer.Bytes())
	return uint32(length), err
}

/// TableEntry

// entries that are stored in the vlog file
// key and value are byte arrays so they support anything that
// can be converted to byte array
type TableEntry struct {
	key   []byte
	value []byte
}

func NewEntry(key []byte, value []byte) TableEntry {
	return TableEntry{key: key, value: value}
}

//Write entry to given writer and return the length of the written bytes sequence
func (entry *TableEntry) writeTo(writer io.Writer) (uint32, error) {
	buffer := bytes.NewBuffer([]byte{})
	//key length
	if err := binary.Write(buffer, binary.BigEndian, uint32(len(entry.key))); err != nil {
		return 0, err
	}
	//value length
	if err := binary.Write(buffer, binary.BigEndian, uint32(len(entry.value))); err != nil {
		return 0, err
	}
	//key
	if err := binary.Write(buffer, binary.BigEndian, entry.key); err != nil {
		return 0, err
	}
	//value
	if err := binary.Write(buffer, binary.BigEndian, entry.value); err != nil {
		return 0, err
	}
	length, err := writer.Write(buffer.Bytes())
	return uint32(length), err
}
