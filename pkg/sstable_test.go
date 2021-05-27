package wiskey

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

const (
	firstKey   = "ALMAS"
	firstValue = "DEVELOPER"
)

func TestSSTable(t *testing.T) {
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.Remove(file.Name())
	saveFakeEntries(t, file.Name())
	reader, _ := os.Open(file.Name())
	table := ReadTable(reader)
	value, found := table.Get([]byte(firstKey))
	if !found {
		t.Error("Key was not found")
	}
	if strings.Compare(string(value), firstValue) != 0 {
		t.Error("Value is not correct")
	}
	table.Close()

}

//write sorted entries to the file
func saveFakeEntries(t *testing.T, name string) {
	writer, _ := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0600)
	tableWriter := SSTableWriter{maxBlockLength: uint32(20), currentBlockPosition: 0, size: 0, writeCloser: writer, inMemoryIndex: indexes{}}
	entries := []TableEntry{
		{key: []byte(firstKey), value: []byte(firstValue)},
		{key: []byte("BNITA"), value: []byte("DEVELOPER3")},
		{key: []byte("GNITA"), value: []byte("DEVELOPER4")},
		{key: []byte("NNITA"), value: []byte("DEVELOPER5")},
		{key: []byte("TNITA"), value: []byte("DEVELOPER6")},
		{key: []byte("WNITA"), value: []byte("DEVELOPER7")},
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
