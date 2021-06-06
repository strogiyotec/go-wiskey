package wiskey

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestVlog_Append(t *testing.T) {
	file, _ := ioutil.TempFile("", "")
	defer file.Close()
	defer os.Remove(file.Name())
	writer, _ := os.OpenFile(file.Name(), os.O_APPEND|os.O_WRONLY, 0600)
	vlog := vlog{readAndWrite: writer, size: 0}
	//test entries
	entries := []TableEntry{
		NewEntry([]byte("ANITA"), []byte("DEVELOPER")),
		NewEntry([]byte("BNITA"), []byte("DEVELOPER2")),
		NewEntry([]byte("GNITA"), []byte("DEVELOPER3")),
		NewEntry([]byte("NNITA"), []byte("DEVELOPER4")),
		NewEntry([]byte("TNITA"), []byte("DEVELOPER5")),
		NewEntry([]byte("WNITA"), []byte("DEVELOPER6")),
	}
	for _, entry := range entries {
		length := uint32(uint32Size /*key length*/ + uint32Size /*value length*/ + len(entry.key) /*ANITA takes 5 bytes*/ + len(entry.value) /*DEVELOPER takes 8 bytes*/)
		meta, err := vlog.Append(entry)
		if err != nil {
			t.Error(err)
		}
		if meta.length != length {
			t.Error("The length doesn't match")
		}
	}
}
