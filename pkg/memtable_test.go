package wiskey

import "testing"

func TestMemtable_Put(t *testing.T) {
	table := NewMemTable(100)
	key := []byte("myKey")
	value := &ValueMeta{length: 100, offset: 100}
	err := table.Put(key, value)
	if err != nil {
		t.Error(table)
	}
	foundValue, found := table.Get(key)
	if !found {
		t.Error("Key was not found in memtable")
	}
	if foundValue != value {
		t.Error("Wrong value in memtable")
	}
}

func TestMemtable_GetNonExistingKey(t *testing.T) {
	table := NewMemTable(100)
	_, found := table.Get([]byte("Non existing key"))
	if found {
		t.Error("Found non existing key in memtable")
	}
}

func TestMemtable_PutTomb(t *testing.T) {
	table := NewMemTable(100)
	err := table.Put([]byte(tombstone), &ValueMeta{offset: 100, length: 100})
	if err == nil {
		t.Error("Should not allow to save a tomb")
	}
}
