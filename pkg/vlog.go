package wiskey

import (
	"encoding/binary"
	"os"
)

type vlog struct {
	file string
	size uint32 // current size of the file,it has to be updated every time you append a new value
}

func (log *vlog) Get(meta ValueMeta) (*TableEntry, error) {
	reader, err := os.OpenFile(log.file, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	reader.Seek(int64(meta.offset), 0)
	buffer := make([]byte, meta.length)
	reader.Read(buffer)
	keyLength := binary.BigEndian.Uint32(buffer[0:4])
	key := buffer[8 : 8+keyLength]
	value := buffer[8+keyLength:]
	return &TableEntry{key: key, value: value}, nil
}

//Run gargabe collector
//Read tailLength entries from the start of vlog
//Check if they were deleted, if no append them to head
func (log *vlog) Gc(tailLength uint) {

}

//Append new entry to the head of vlog
//the binary format for entry is [klength,vlength,key,value]
//we store key in vlog for garbage collection purposes
func (log *vlog) Append(entry *TableEntry) (*ValueMeta, error) {
	writer, err := os.OpenFile(log.file, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	defer writer.Close()
	length, err := entry.writeTo(writer)
	if err != nil {
		return nil, err
	}
	meta := &ValueMeta{length: length, offset: log.size}
	log.size += length
	return meta, nil
}

type ValueMeta struct {
	length uint32 //value length in vlog file
	offset uint32 //value offset in vlog file
}
