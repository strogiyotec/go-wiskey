package wiskey

type indexes []keyIndex

type SSTable struct {
	tableHeader Header
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

//indexes to find an entry in a file
type keyIndex struct {
	blockOffset int    //where this block begins in the file
	blockLength int    //the length of this block
	key         []byte //key
}
