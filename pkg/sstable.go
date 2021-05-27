package wiskey

import (
	"bytes"
	"encoding/binary"
	"io"
)

type indexes []tableIndex

type SSTable struct {
	tableHeader Footer
	indexes     indexes
	reader      interface{}
}

//entries that are stored in the sstable file
// key and value are byte arrays so they support anything that
// can be converted to byte array
type TableEntry struct {
	key   []byte
	value []byte
}

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

//indexes to find an entry in a file
type tableIndex struct {
	Offset      uint32 //Offset of the file where index starts
	BlockLength uint32 //the length of the index
}

func (index *tableIndex) WriteTo(w io.Writer) error {
	buf := bytes.NewBuffer([]byte{})

	if err := binary.Write(buf, binary.BigEndian, index.BlockLength); err != nil {
		return err
	}

	if err := binary.Write(buf, binary.BigEndian, index.Offset); err != nil {
		return err
	}

	_, err := w.Write(buf.Bytes())
	//can be null
	return err
}
