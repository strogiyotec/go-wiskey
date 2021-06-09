package wiskey

import (
	"io/ioutil"
	"os"
	"testing"
)

//unsorted list of entries
func FakeEntries() []TableEntry {
	return []TableEntry{
		NewEntry([]byte("WNITA"), []byte("DEVELOPER6")),
		NewEntry([]byte("GNITA"), []byte("DEVELOPER3")),
		NewEntry([]byte("ANITA"), []byte("DEVELOPER")),
		NewEntry([]byte("BNITA"), []byte("DEVELOPER2")),
		NewEntry([]byte("NNITA"), []byte("DEVELOPER4")),
		NewEntry([]byte("TNITA"), []byte("DEVELOPER5")),
	}
}
func TestLsmTree_Put(t *testing.T) {
	tempDir, _ := ioutil.TempDir("", "")
	vlogFile, _ := ioutil.TempFile("", "")
	defer os.RemoveAll(tempDir)
	defer os.Remove(vlogFile.Name())
	vlog := &vlog{file: vlogFile.Name(), size: 0}
	tree := lsmTree{log: vlog, sstablePath: tempDir,memtable: NewMemTable(100)}
	entries := FakeEntries()
	//save entries in unsorted order, it will be sorted by memtable
	for _, entry := range entries {
		err := tree.Put(&entry)
		if err != nil {
			t.Error(err)
		}
	}
	err := tree.Flush()
	if err != nil {
		t.Error(err)
	}
	reader, _ := os.Open(tree.sstables[0])
	sstable := ReadTable(reader, vlog)
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
