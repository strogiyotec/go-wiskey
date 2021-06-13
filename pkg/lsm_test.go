package wiskey

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"
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

func InitTestLsmWithMeta() *LsmTree {
	tempDir, _ := ioutil.TempDir("", "")
	vlogFile, _ := ioutil.TempFile("", "")
	checkpoint, _ := ioutil.TempFile("", "")
	vlog := NewVlog(vlogFile.Name(), checkpoint.Name())
	return NewLsmTree(vlog, tempDir, NewMemTable(100))
}

func TestLsmTree_GetDeletedValue(t *testing.T) {
	tree := InitTestLsmWithMeta()
	defer os.RemoveAll(tree.sstableDir)
	defer os.Remove(tree.log.file)
	defer os.Remove(tree.log.checkpoint)
	key := []byte("ANITA")
	value := []byte("DEVELOPER")
	//save entry and flush to sstable
	err := tree.Put(&TableEntry{key: key, value: value})
	if err != nil {
		t.Fatal(err)
	}
	err = tree.Flush()
	if err != nil {
		t.Fatal(err)
	}
	//sleep for a second and delete a key
	time.Sleep(1 * time.Second)
	err = tree.Delete(key)
	if err != nil {
		t.Fatal(err)
	}
	err = tree.Flush()
	if err != nil {
		t.Fatal(err)
	}
	_, found := tree.Get(key)
	if found {
		t.Fatal("Deleted key was found")
	}
}

func TestLsmTree_PutAndGetFromSSTable(t *testing.T) {
	tree := InitTestLsmWithMeta()
	defer os.RemoveAll(tree.sstableDir)
	defer os.Remove(tree.log.file)
	defer os.Remove(tree.log.checkpoint)
	entries := FakeEntries()
	//save entries in unsorted order, it will be sorted by memtable
	for _, entry := range entries {
		err := tree.Put(&entry)
		if err != nil {
			t.Fatal(err)
		}
	}
	err := tree.Flush()
	if err != nil {
		t.Fatal(err)
	}
	//fetch entries
	for _, entry := range entries {
		result, found := tree.Get(entry.key)
		if !found {
			t.Fatal("Key wasn't found in sstable")
		}
		if bytes.Compare(result, entry.value) != 0 {
			t.Fatal("Value in sstable doesn't match an actual value")
		}
	}
	//try to find non existing key
	_, found := tree.Get([]byte("NON EXISTING KEY"))
	if found {
		t.Error("Found non existing key")
	}
}

//Test get when in memory
func TestLsmTree_GetInMemory(t *testing.T) {
	tree := InitTestLsmWithMeta()
	defer os.RemoveAll(tree.sstableDir)
	defer os.Remove(tree.log.file)
	defer os.Remove(tree.log.checkpoint)
	entries := FakeEntries()
	//save entries but don't flush
	for _, entry := range entries {
		err := tree.Put(&entry)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, entry := range entries {
		value, found := tree.Get(entry.key)
		if !found {
			t.Fatal("Value was not found in lsm tree")
		}
		if bytes.Compare(value, entry.value) != 0 {
			t.Fatal("Values don't match")
		}
	}
}

func TestLsmTree_Restore(t *testing.T) {
	tree := InitTestLsmWithMeta()
	defer os.RemoveAll(tree.sstableDir)
	defer os.Remove(tree.log.file)
	defer os.Remove(tree.log.checkpoint)
	entries := FakeEntries()
	//save entries but don't flush
	for index, entry := range entries {
		err := tree.Put(&entry)
		if err != nil {
			t.Fatal(err)
		}
		//flush the first half of the tree
		if index == len(entries)/2 {
			err := tree.Flush()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	//now before flush we create a new lsm tree
	vlog := NewVlog(tree.log.file, tree.log.checkpoint)
	//this tree has to have last half of entries restored from the vlog
	newTree := NewLsmTree(vlog, tree.sstableDir, NewMemTable(100))
	for index := len(entries)/2 + 1; index < len(entries); index++ {
		_, found := newTree.Get(entries[index].key)
		if !found {
			t.Fatal("Didn't restore the key from vlog")
		}
	}
	//now if we try to restore it again , memtable has to be empty cause it was restored previously
	vlog = NewVlog(tree.log.file, tree.log.checkpoint)
	newTree = NewLsmTree(vlog, tree.sstableDir, NewMemTable(100))
	if newTree.memtable.Size() != 0 {
			t.Fatal("Should not restore any entries")
	}
}
