package wiskey

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestSSTable(t *testing.T) {
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.Remove(file.Name())
	entries := []TableEntry{
		{key: []byte("ANITA"), value: []byte("DEVELOPER")},
		{key: []byte("BNITA"), value: []byte("DEVELOPER3")},
		{key: []byte("GNITA"), value: []byte("DEVELOPER4")},
		{key: []byte("NNITA"), value: []byte("DEVELOPER5")},
		{key: []byte("TNITA"), value: []byte("DEVELOPER6")},
		{key: []byte("WNITA"), value: []byte("DEVELOPER7")},
	}
	saveFakeEntries(t, file.Name(), entries)
	reader, _ := os.Open(file.Name())
	table := ReadTable(reader)
	for _, entry := range entries {
		value, found := table.Get(entry.key)
		if !found {
			t.Error("Key was not found")
		}
		if strings.Compare(string(value), string(entry.value)) != 0 {
			t.Error("Value is not correct")
		}
		t.Logf("Key is %s, Value is %s ", string(entry.key), string(entry.value))
	}
	table.Close()
}

//write sorted entries to the file
func saveFakeEntries(t *testing.T, name string, entries []TableEntry) {
	writer, _ := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0600)
	tableWriter := SSTableWriter{maxBlockLength: uint32(20), currentBlockPosition: 0, size: 0, writeCloser: writer, inMemoryIndex: indexes{}}
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
