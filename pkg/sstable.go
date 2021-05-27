package wiskey

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

type indexes []tableIndex

type SSTable struct {
	footer  *Footer
	indexes indexes
	reader  *os.File
}

//Constructor
func ReadTable(reader *os.File) *SSTable {
	stats, _ := reader.Stat()
	//read footer
	footer := readFooter(stats, reader)
	indexes := readIndex(stats, reader, *footer)
	return &SSTable{footer: footer, indexes: indexes, reader: reader}
}

func (table *SSTable) Close(){
	table.reader.Close()
}

//TODO: implement binary search
//for now Get works only for the first and the last key in the file
func (table *SSTable) Get(key []byte) ([]byte, bool) {
	//try smallest key
	smallest := table.indexes[0]
	compare, value := table.find(key, smallest)
	if compare == 0 {
		return value, true
	}
	//if smaller than smallest key in file than key is not in the file
	if compare < 0 {
		return nil, false
	}
	//try biggest key
	//TODO: block can contain multiple keys, need to check them all
	biggest := table.indexes[len(table.indexes)-1]
	compare, value = table.find(key, biggest)
	if compare == 0 {
		return value, true
	}
	//if bigger than biggest key in file than key is not in the file
	if compare > 0 {
		return nil, false
	}
	return nil,false
}

func (table *SSTable) find(key []byte, index tableIndex) (uint32, []byte) {
	keyLength := uint32(len(key))
	keyLengthBuffer := make([]byte, 4)
	//read key length
	table.reader.Seek(int64(index.Offset), 0)
	table.reader.Read(keyLengthBuffer)
	fileKeyLength := binary.BigEndian.Uint32(keyLengthBuffer)
	//if keys length are not the same then don't make sense to compare an actual key
	if keyLength != fileKeyLength {
		return keyLength - fileKeyLength, nil
	}
	//read value length
	valueLengthBuffer := make([]byte, 4)
	table.reader.Read(valueLengthBuffer)
	valueLength := binary.BigEndian.Uint32(valueLengthBuffer)
	//read actual key from the file
	keyBuffer := make([]byte, fileKeyLength)
	table.reader.Read(keyBuffer)
	compare := bytes.Compare(key, keyBuffer)
	//they are equal
	if compare == 0 {
		valueBuffer := make([]byte, valueLength)
		table.reader.Read(valueBuffer)
		return 0, valueBuffer
	}
	return uint32(compare), nil

}

//Read the index from the file to in memory slice
func readIndex(stats os.FileInfo, reader *os.File, footer Footer) indexes {
	reader.Seek(int64(footer.indexOffset), 0)
	buffer := make([]byte, stats.Size()-int64(footer.indexOffset)-footerSize)
	reader.Read(buffer)
	start := 0
	end := len(buffer)
	indexes := indexes{}
	for start != end {
		blockLength := binary.BigEndian.Uint32(buffer[start : start+4])
		blockOffset := binary.BigEndian.Uint32(buffer[start+4 : start+8])
		indexes = append(indexes, tableIndex{Offset: blockOffset, BlockLength: blockLength})
		start += 8
	}
	return indexes

}

//Read the footer
func readFooter(stats os.FileInfo, reader *os.File) *Footer {
	buf := make([]byte, footerSize)
	reader.Seek(stats.Size()-footerSize, 0)
	reader.Read(buf)
	return NewFooter(buf)
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
