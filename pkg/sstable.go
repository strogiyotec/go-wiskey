package wiskey

type index []keyIndex

type SSTable struct {
	tableHeader Header
	indexes     index
	reader      interface{}
}

//index to find an entry in a file
type keyIndex struct {
	blockOffset int    //where this block begins in the file
	blockLength int    //the lenght of this block
	key         []byte //key
}
