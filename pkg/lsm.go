package wiskey

type lsmTree struct {
	tableWriter *SSTableWriter
	log         *vlog
}

//save entry in vlog first then in sstable
func (lsm *lsmTree) Put(entry *TableEntry) error {
	meta, err := lsm.log.Append(entry)
	if err != nil {
		return err
	}
	_, err = lsm.tableWriter.WriteEntry(NewSStableEntry(entry, meta))
	if err != nil {
		return err
	}
	return nil
}
