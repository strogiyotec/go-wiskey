package wiskey

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"
)

func TestWriter(t *testing.T) {
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.Remove(file.Name())
	saveFakeEntries(t, file.Name())
	readEntries(file.Name(), t)

}

//TODO: the test is ugly, when we will implement the Reader it will be more compact
func readEntries(fileName string, t *testing.T) {
	reader, _ := os.Open(fileName)
	buf := make([]byte, footerSize)
	stats, _ := reader.Stat()
	reader.Seek(stats.Size()-footerSize, 0)
	reader.Read(buf)
	footer := NewFooter(buf)
	t.Log(footer.indexOffset)
	reader.Seek(0, 0)
	//Read first key
	buf = make([]byte, 4)
	reader.Read(buf)
	keyLength := binary.BigEndian.Uint32(buf)
	buf = make([]byte, 4)
	reader.Read(buf)
	valueLength := binary.BigEndian.Uint32(buf)
	t.Logf("Key length %d, value length %d", keyLength, valueLength)
	buf = make([]byte, keyLength)
	reader.Read(buf)
	t.Logf("Key is %s", string(buf))
	buf = make([]byte, valueLength)
	reader.Read(buf)
	t.Logf("Value is %s", string(buf))
	//read second key
	buf = make([]byte, 4)
	reader.Read(buf)
	keyLength = binary.BigEndian.Uint32(buf)
	buf = make([]byte, 4)
	reader.Read(buf)
	valueLength = binary.BigEndian.Uint32(buf)
	t.Logf("Key length %d, value length %d", keyLength, valueLength)
	buf = make([]byte, keyLength)
	reader.Read(buf)
	t.Logf("Key is %s", string(buf))
	buf = make([]byte, valueLength)
	reader.Read(buf)
	t.Logf("Value is %s", string(buf))
	reader.Close()
}

func saveFakeEntries(t *testing.T, name string) {
	writer, _ := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0600)
	tableWriter := SSTableWriter{maxBlockLength: uint32(20), currentBlockPosition: 0, size: 0, writeCloser: writer, inMemoryIndex: indexes{}}
	entries := []TableEntry{
		{key: []byte("ALMAS"), value: []byte("DEVELOPER")},
		{key: []byte("ANITA"), value: []byte("DEVELOPER2")},
	}
	for _, entry := range entries {
		err := tableWriter.WriteEntry(entry)
		if err != nil {
			t.Error(err)
		}
	}
	err := tableWriter.Close()
	if err != nil {
		t.Error(err)
	}
}
