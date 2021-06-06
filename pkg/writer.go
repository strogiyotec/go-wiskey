package wiskey

import "io"

//vlog writer
type SSTableWriter struct {
	maxBlockLength       uint32
	currentBlockPosition uint32
	size                 uint32 //how many bytes were written to file
	writeCloser          io.WriteCloser
	inMemoryIndex        []tableIndex
}

//create new writeCloser
func NewWriter(w io.WriteCloser) *SSTableWriter {
	return &SSTableWriter{
		maxBlockLength: 64 * 1024,
		writeCloser:    w,
	}
}

//close the writeCloser and returns the index Offset in the file
func (w *SSTableWriter) Close() error {
	//if there are still some remaining bytes then save them in the index
	w.closeBlock()
	err := w.writeIndex()
	if err != nil {
		return err
	}
	footer := Footer{indexOffset: w.size}
	footer.writeTo(w.writeCloser)
	return w.writeCloser.Close()
}

//Write entry to the file, all entries have to be sorted in advance
func (w *SSTableWriter) WriteEntry(e TableEntry) (*ValueMeta, error) {
	length, err := e.writeTo(w.writeCloser)
	meta := &ValueMeta{length: length, offset: w.size}
	w.size += length
	if err != nil {
		return nil, err
	}
	//if block is full then create the index for this block
	if w.blockIsFull() {
		w.closeBlock()
	}
	return meta, nil
}

func (w *SSTableWriter) blockIsFull() bool {
	return w.maxBlockLength <= w.blockCapacity()
}

func (w *SSTableWriter) blockCapacity() uint32 {
	return w.size - w.currentBlockPosition
}

func (w *SSTableWriter) closeBlock() {
	//save the block in the index only when there are some bytes between the last saved position and current written length
	if w.size > w.currentBlockPosition {
		w.inMemoryIndex = append(w.inMemoryIndex, tableIndex{Offset: w.currentBlockPosition, BlockLength: w.size - w.currentBlockPosition})
		w.currentBlockPosition = w.size
	}
}

func (w *SSTableWriter) writeIndex() error {
	for _, index := range w.inMemoryIndex {
		err := index.WriteTo(w.writeCloser)
		if err != nil {
			return err
		}
	}
	return nil
}
