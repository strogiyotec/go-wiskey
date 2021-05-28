package wiskey

import (
	"encoding/binary"
	"os"
)

type SSTableReader struct {
	reader *os.File
	offset uint32
}

func NewReader(reader *os.File, offset int64) *SSTableReader {
	reader.Seek(offset, 0)
	return &SSTableReader{reader: reader}
}

func (tableReader *SSTableReader) readKeyLength() uint32 {
	tableReader.offset += 4
	keyLengthBuffer := make([]byte, 4)
	//read key length
	tableReader.reader.Read(keyLengthBuffer)
	return binary.BigEndian.Uint32(keyLengthBuffer)
}

func (tableReader *SSTableReader) readValueLength() uint32 {
	tableReader.offset += 4
	valueLengthBuffer := make([]byte, 4)
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
