package wiskey

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestHeaderWriteAndRead(t *testing.T) {
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.Remove(file.Name())
	header := Header{amountOfBlocks: 2, indexOffset: 0}
	writeToFile(file.Name(), header)
	buf := readFromFile(file.Name())
	headerFromFile := NewHeader(buf)

	if headerFromFile.indexOffset != header.indexOffset {
		t.Error("Index offsets don't match")
	}
	if headerFromFile.amountOfBlocks != header.amountOfBlocks {
		t.Error("Amount of blocks don't match")
	}
}

func readFromFile(fileName string) []byte {
	reader, _ := os.Open(fileName)
	buf := make([]byte, headerSize)
	reader.Read(buf)
	reader.Close()
	return buf
}

func writeToFile(fileName string, header Header) {
	writer, _ := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0600)
	header.writeTo(writer)
	writer.Close()
}
