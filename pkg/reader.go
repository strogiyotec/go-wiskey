package wiskey

import (
	"encoding/binary"
	"os"
)

const (
	uint32Size = 4
	int64Size = 8
)

type SSTableReader struct {
	reader *os.File
	offset uint32
}

//Create a new sstable reader
//It sets the reader to the given offset
//All methods have to be called in the following order
//1. readKeyLength
//2. readValueLength
//3. readKey
//4. readValue
func NewReader(reader *os.File, offset int64) *SSTableReader {
	reader.Seek(offset, 0)
	return &SSTableReader{reader: reader}
}

func (tableReader *SSTableReader) readKeyLength() uint32 {
	tableReader.offset += uint32Size
	keyLengthBuffer := make([]byte, uint32Size)
	//read key length
	tableReader.reader.Read(keyLengthBuffer)
	return binary.BigEndian.Uint32(keyLengthBuffer)
}

func (tableReader *SSTableReader) readValueLength() uint32 {
	tableReader.offset += uint32Size
	valueLengthBuffer := make([]byte, uint32Size)
	tableReader.reader.Read(valueLengthBuffer)
	return binary.BigEndian.Uint32(valueLengthBuffer)
}

func (tableReader *SSTableReader) readKey(keyLength uint32) []byte {
	tableReader.offset += keyLength
	keyBuffer := make([]byte, keyLength)
	tableReader.reader.Read(keyBuffer)
	return keyBuffer
}

func (tableReader *SSTableReader) readValue(valueLength uint32) []byte {
	tableReader.offset += valueLength
	valueBuffer := make([]byte, valueLength)
	tableReader.reader.Read(valueBuffer)
	return valueBuffer
}

func (tableReader *SSTableReader) readTimestamp() uint64 {
	tableReader.offset += int64Size
	timestamp := make([]byte, int64Size)
	tableReader.reader.Read(timestamp)
	return binary.BigEndian.Uint64(timestamp)
}
