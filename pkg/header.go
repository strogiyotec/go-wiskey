package wiskey

import (
	"bytes"
	"encoding/binary"
	"io"
)

const (
	headerSize = 8 //how many bytes are in the header(amountOfBlocks+indexOffset)
)

type Header struct {
	amountOfBlocks uint32 // how many blocks are in the file
	indexOffset    uint32 // the offest where indexes starts
}

func DefaultHeader() *Header{
	return &Header{
		amountOfBlocks: 0,
		indexOffset: 0,
	}
}

func NewHeader(buffer []byte) *Header {
	if len(buffer) != headerSize {
		panic("Invalid header length")
	}
	blocks := binary.BigEndian.Uint32(buffer[:4])
	offset := binary.BigEndian.Uint32(buffer[4:8])
	return &Header{amountOfBlocks: blocks, indexOffset: offset}
}

//save the header in the given writer
func (h *Header) writeTo(writer io.Writer) int {
	offset, err := writer.Write(h.asByteArray())
	if err != nil {
		panic(err)
	}
	return offset
}

//convert header to binary array
func (h *Header) asByteArray() []byte {
	buffer := bytes.NewBuffer(make([]byte, 0, headerSize))
	err := binary.Write(buffer, binary.BigEndian, h)
	if err != nil {
		panic(err)
	}
	return buffer.Bytes()
}
