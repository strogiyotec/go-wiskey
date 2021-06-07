package wiskey

import (
	"bytes"
	"encoding/binary"
	"os"
)

type indexes []tableIndex

type SSTable struct {
	footer  *Footer
	indexes indexes
	reader  *os.File
	log     *vlog
}

type SearchEntry struct {
	key       []byte
	value     []byte
	deleted   byte
	timestamp uint64
}

func NewDeletedEntry(key []byte, timestamp uint64) *SearchEntry {
	return &SearchEntry{
		key:       key,
		deleted:   deleted,
		timestamp: timestamp,
	}
}

//Constructor
func ReadTable(reader *os.File, log *vlog) *SSTable {
	stats, _ := reader.Stat()
	//read footer
	footer := readFooter(stats, reader)
	indexes := readIndexes(stats, reader, *footer)
	return &SSTable{footer: footer, indexes: indexes, reader: reader, log: log}
}

func (table *SSTable) Close() {
	table.reader.Close()
}

//Returns value,timestamp of this value,bool which represents if value exists
func (table *SSTable) Get(key []byte) (*SearchEntry, bool) {
	//try smallest key
	firstIndex := table.indexes[0]
	compare, value := table.find(key, firstIndex)
	if compare == 0 {
		return value, true
	}
	//if smaller than firstIndex key in file than key is not in the file
	if compare < 0 {
		return nil, false
	}
	//try biggest key
	//TODO: block can contain multiple keys, need to check them all
	lastIndex := table.indexes[len(table.indexes)-1]
	compare, value = table.find(key, lastIndex)
	if compare == 0 {
		return value, true
	}
	//if bigger than lastIndex key in file than key is not in the file
	if compare > 0 {
		return nil, false
	}
	return table.binarySearch(key)
}

//Tries to find given key in the sstable
//Returns value byte array or nil if not found
//timestamp of this value or 0 if not found
//bool true if found,false otherwise
func (table *SSTable) binarySearch(key []byte) (*SearchEntry, bool) {
	left := 0
	right := len(table.indexes) - 1
	for left < right {
		middle := (right-left)/2 + left
		index := table.indexes[middle]
		//read key length
		tableReader := NewReader(table.reader, int64(index.Offset))
		fileKeyLength := tableReader.readKeyLength()
		//read actual key from the file
		keyBuffer := tableReader.readKey(fileKeyLength)
		compare := bytes.Compare(key, keyBuffer)
		if compare == 0 {
			return table.fetchFromVlog(tableReader, keyBuffer), true
		} else if compare > 0 {
			left = middle + 1
		} else {
			right = middle - 1
		}
	}
	index := table.indexes[left]
	tableReader := NewReader(table.reader, int64(index.Offset))
	for tableReader.offset != index.BlockLength {
		keyLength := tableReader.readKeyLength()
		fileKey := tableReader.readKey(keyLength)
		if bytes.Compare(key, fileKey) == 0 {
			return table.fetchFromVlog(tableReader, fileKey), true
		}
	}
	return nil, false
}

func (table *SSTable) fetchFromVlog(tableReader *SSTableReader, key []byte) *SearchEntry {
	timestamp := tableReader.readTimestamp()
	meta := tableReader.readMeta()
	if meta == deleted {
		return NewDeletedEntry(key, timestamp)
	}
	offset := tableReader.readValueOffset()
	length := tableReader.readValueLength()
	get, err := table.log.Get(ValueMeta{length: length, offset: offset})
	if err != nil {
		panic(err)
	}
	return &SearchEntry{key: get.key, value: get.value, deleted: nonDeleted, timestamp: timestamp}
}

func (table *SSTable) find(key []byte, index tableIndex) (int, *SearchEntry) {
	searchKeyLength := uint32(len(key))
	tableReader := NewReader(table.reader, int64(index.Offset))
	fileKeyLength := tableReader.readKeyLength()
	//if keys length are not the same then don't make sense to compare an actual key
	if searchKeyLength != fileKeyLength {
		return len(key) - int(fileKeyLength), nil
	}
	//read actual key from the file
	keyBuffer := tableReader.readKey(searchKeyLength)
	compare := bytes.Compare(key, keyBuffer)
	//they are equal
	if compare == 0 {
		return 0, table.fetchFromVlog(tableReader, keyBuffer)
	}
	return compare, nil
}

//Read the index from the file to in memory slice
func readIndexes(stats os.FileInfo, reader *os.File, footer Footer) indexes {
	buffer := make([]byte, stats.Size()-int64(footer.indexOffset)-footerSize)
	reader.ReadAt(buffer, int64(footer.indexOffset))
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
