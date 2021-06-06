package wiskey

import "io"

type vlog struct {
	readAndWrite io.ReadWriteCloser //read from and write to vlog
	size         uint32             // current size of the file,it has to be updated every time you append a new value
}

//Run gargabe collector
//Read tailLength entries from the start of vlog
//Check if they were deleted, if no append them to head
func (log *vlog) Gc(tailLength uint) {

}

//Append new entry to the head of vlog
//the binary format for entry is [klength,vlength,key,value]
//we store key in vlog for garbage collection purposes
func (log *vlog) Append(entry TableEntry) (*ValueMeta, error) {
	length, err := entry.writeTo(log.readAndWrite)
	if err != nil {
		return nil, err
	}
	meta := &ValueMeta{length: length, offset: log.size}
	log.size += length
	return meta, nil
}

type ValueMeta struct {
	length uint32 //value length in vlog file
	offset uint32 //value offset in vlog file
}
