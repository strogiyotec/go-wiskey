package wiskey

import (
	binary "encoding/binary"
	"os"
)

type vlog struct {
	file       string
	size       uint32 // current size of the file,it has to be updated every time you append a new value
	checkpoint string //path to the file with checkpoint
}

func NewVlog(file string, checkpoint string) *vlog {
	return &vlog{
		file:       file,
		checkpoint: checkpoint,
		size:       0,
	}
}

//Save the latest vlog head position in the checkpoint file
func (log *vlog) FlushHead() error {
	writer, err := os.OpenFile(log.checkpoint, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer writer.Close()
	err = writer.Truncate(0)
	if err != nil {
		return err
	}
	return binary.Write(writer, binary.BigEndian, log.size)
}

// Example of vlog entry to read
//+------------+--------------+-----+-------+
//| Key Length | Value length | Key | Value |
//+------------+--------------+-----+-------+
func (log *vlog) Get(meta ValueMeta) (*TableEntry, error) {
	reader, err := os.OpenFile(log.file, os.O_RDONLY|os.O_CREATE, 0666)
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

func (log *vlog) RunGc(entries int, lsm *LsmTree) error {
	type toAppend struct {
		keyLength        uint32
		valueLength      uint32
		key              []byte
		value            []byte
		tableWithIndexes []TableWithIndex
	}
	file, err := os.OpenFile(log.file, os.O_RDONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	logFileSize := stat.Size()
	if logFileSize == 0 {
		return nil
	}
	readBytesSize := int64(0) //how many bytes were read from file
	counter := 0
	//list of entries that are still exist
	var stillExist []toAppend
	for readBytesSize < logFileSize && counter < entries {
		keyLengthBuffer := make([]byte, uint32Size)
		//read key length
		_, _ = file.Read(keyLengthBuffer)
		keyLength := binary.BigEndian.Uint32(keyLengthBuffer)
		//read value length
		valueLengthBuffer := make([]byte, uint32Size)
		_, _ = file.Read(valueLengthBuffer)
		valueLength := binary.BigEndian.Uint32(valueLengthBuffer)
		keyBuffer := make([]byte, keyLength)
		valueBuffer := make([]byte, valueLength)
		_, _ = file.Read(keyBuffer)
		_, _ = file.Read(valueBuffer)
		tableWithIndexes := lsm.Exists(keyBuffer)
		if len(tableWithIndexes) != 0 {
			stillExist = append(stillExist, toAppend{keyLength: keyLength, valueLength: valueLength, key: keyBuffer, value: valueBuffer, tableWithIndexes: tableWithIndexes})
		}
		readBytesSize += int64(uint32Size + keyLength + uint32Size + valueLength)
		counter++
	}
	//TODO :  So we have a list of entries that still exist in lsm tree
	// 1. we need to append them to the end of vlog
	// 2. truncate the beginning of vlog by readBytesSize
	// 3. Update sstable path to vlog using new offset that was appended to end of file
	return nil
}

//Restore vlog to given memtable
func (log *vlog) RestoreTo(headOffset uint32, memtable *Memtable) error {
	reader, err := os.OpenFile(log.file, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer reader.Close()
	_, err = reader.Seek(int64(headOffset), 0)
	if err != nil {
		return err
	}
	stat, err := reader.Stat()
	if err != nil {
		return err
	}
	length := stat.Size() - int64(headOffset)
	if length == 0 {
		//head is the tail
		return nil
	}
	buffer := make([]byte, length)
	_, err = reader.Read(buffer)
	if err != nil {
		return err
	}
	lastPosition := 0
	nextOffset := uint32(0)
	for lastPosition != len(buffer) {
		keyLength := binary.BigEndian.Uint32(buffer[lastPosition : lastPosition+4])
		valueLength := binary.BigEndian.Uint32(buffer[lastPosition+4 : lastPosition+8])
		key := buffer[lastPosition+8 : lastPosition+8+int(keyLength)]
		metaLength := uint32Size + uint32Size + int(keyLength) + int(valueLength)
		err := memtable.Put(key, &ValueMeta{length: uint32(metaLength), offset: nextOffset + headOffset})
		if err != nil {
			return err
		}
		nextOffset += uint32(metaLength)
		lastPosition += uint32Size
		lastPosition += uint32Size
		lastPosition += int(keyLength)
		lastPosition += int(valueLength)
	}
	log.size = uint32(stat.Size())
	return nil
}

//Append new entry to the head of vlog
//the binary format for entry is [klength,vlength,key,value]
//we store key in vlog for garbage collection purposes
// Example of signle entry in vlog
//+------------+--------------+-----+-------+
//| Key Length | Value length | Key | Value |
//+------------+--------------+-----+-------+
func (log *vlog) Append(entry *TableEntry) (*ValueMeta, error) {
	writer, err := os.OpenFile(log.file, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
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

//metadata of saved entry in vlog
type ValueMeta struct {
	length uint32 //value length in vlog file
	offset uint32 //value offset in vlog file
}
