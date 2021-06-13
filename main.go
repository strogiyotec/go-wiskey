package main

import (
	"wiskey/cmd"
	"wiskey/http"
	. "wiskey/pkg"
)

func main() {
	parse, err := cmd.Parse()
	if err != nil {
		panic(err)
	}
	vlog := NewVlog(parse.Vlog, parse.Checkpoint)
	memtable := NewMemTable(20)
	tree := NewLsmTree(vlog, parse.SStablePath, memtable)
	http.Start(tree)

}
