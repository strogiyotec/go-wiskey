package wiskey

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSSTable(t *testing.T) {
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.Remove(file.Name())
	entries := []TableEntry{
		NewEntry([]byte("ANITA"), []byte("DEVELOPER")),
		NewEntry([]byte("BNITA"), []byte("DEVELOPER2")),
		NewEntry([]byte("GNITA"), []byte("DEVELOPER3")),
		NewEntry([]byte("NNITA"), []byte("DEVELOPER4")),
		NewEntry([]byte("TNITA"), []byte("DEVELOPER5")),
		NewEntry([]byte("WNITA"), []byte("DEVELOPER6")),
	}
	saveFakeEntries(t, file.Name(), entries)
	reader, _ := os.Open(file.Name())
	table := ReadTable(reader)
	currentTime := uint64(time.Now().Unix())
	for _, entry := range entries {
		value, timestamp, found := table.Get(entry.key)
		if !found {
			t.Error("Key was not found")
		}
		if strings.Compare(string(value), string(entry.value)) != 0 {
			t.Error("Value is not correct")
		}
		if currentTime < timestamp {
			t.Error("Timestamp has to be bigger than current time")
		}
		t.Logf("Key is %s, Value is %s ", string(entry.key), string(entry.value))
	}
	tryToFindNonExistingKey(t, table)
	table.Close()
}

func tryToFindNonExistingKey(t *testing.T, table *SSTable) {
	_, _, found := table.Get([]byte("Non existing key"))
	if found {
		t.Error("This key should not exist in sstable")
	}
}

//write sorted entries to the file
func saveFakeEntries(t *testing.T, name string, entries []TableEntry) {
	writer, _ := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0600)
	tableWriter := SSTableWriter{maxBlockLength: uint32(20), currentBlockPosition: 0, size: 0, writeCloser: writer, inMemoryIndex: indexes{}}
	for _, entry := range entries {
		_,err := tableWriter.WriteEntry(entry)
		if err != nil {
			t.Error(err)
		}
	}
	err := tableWriter.Close()
	if err != nil {
		t.Error(err)
	}
}
