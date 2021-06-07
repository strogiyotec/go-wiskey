package wiskey

import (
	"io/ioutil"
	"os"
	"testing"
)

func FakeEntries() []TableEntry {
	return []TableEntry{
		NewEntry([]byte("ANITA"), []byte("DEVELOPER")),
		NewEntry([]byte("BNITA"), []byte("DEVELOPER2")),
		NewEntry([]byte("GNITA"), []byte("DEVELOPER3")),
		NewEntry([]byte("NNITA"), []byte("DEVELOPER4")),
		NewEntry([]byte("TNITA"), []byte("DEVELOPER5")),
		NewEntry([]byte("WNITA"), []byte("DEVELOPER6")),
	}
}
func TestLsmTree_Put(t *testing.T) {
	sstableFile, _ := ioutil.TempFile("", "")
	vlogFile, _ := ioutil.TempFile("", "")
	defer os.Remove(sstableFile.Name())
	defer os.Remove(vlogFile.Name())
	vlog := &vlog{file: vlogFile.Name(), size: 0}
	writer, _ := os.OpenFile(sstableFile.Name(), os.O_APPEND|os.O_WRONLY, 0666)
	tableWriter := &SSTableWriter{maxBlockLength: uint32(20), currentBlockPosition: 0, size: 0, writeCloser: writer, inMemoryIndex: indexes{}}
	tree := lsmTree{log: vlog, tableWriter: tableWriter}
	entries := FakeEntries()
	//save entries
	for _, entry := range entries {
		err := tree.Put(&entry)
		if err != nil {
			t.Error(err)
		}
	}
	tree.tableWriter.Close()
	reader, _ := os.Open(sstableFile.Name())
	sstable := ReadTable(reader,vlog)
	//fetch entries
	for _, entry := range entries {
		result, found := sstable.Get(entry.key)
		if !found {
			t.Error("Key wasn't found in sstable")
		}
		if string(result.key) != string(entry.key) {
			t.Error("Key in sstable doesn't match an actual key")
		}
		if string(result.value) != string(entry.value) {
			t.Error("Value in sstable doesn't match an actual value")
		}
	}
	//try to find non existing key
	_, found := sstable.Get([]byte("NON EXISTING KEY"))
	if found {
		t.Error("Found non existing key")
	}

}
