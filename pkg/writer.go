package wiskey

import "io"

type SSTableWriter struct {
	indexStore IndexStore
	lastKey    []byte
	writer     io.Writer
}

//create new writer
func NewWriter(w io.Writer) *SSTableWriter {
	return &SSTableWriter{
		indexStore: IndexStore{
			maxBlockLength: 64 * 1024,
			offset:         uint64(0),
			indexes:        indexes{},
		},
		lastKey: nil,
		writer:  w,
	}
}

func (w *SSTableWriter) WriteEntry(e TableEntry) error{
	//write a header
	if w.indexStore.offset == 0{
			header :=DefaultHeader()
			offset :=header.writeTo(w.writer)
			w.indexStore.offset = uint64(offset)
	}
	//TODO: save the entry
	return nil
}

//where indexes are stored in the file
type IndexStore struct {
	maxBlockLength uint32
	offset         uint64
	indexes        indexes
}
