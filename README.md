# go-wiskey

Golang implementation
of [Wiskey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)
paper

# Description

Dead simple lsm implementation which stores values in the vlog which makes
decreases write amplification of lsm tree during merging

# Things that are implemented

1. [X] SSTable
    - [X] Create sstable
    - [X] Read from sstable
2. [X] Memtable(in memory redblack tree that stores the data and flushes it once memory is full)
    - [X] Put
    - [X] Delete
    - [X] Get
3. [o] Lsm tree
    - [X] Put 
    - [X] Get
    - [ ] Delete(partially implemented)
4. Http interface 
   
# TODO
Reimplement delete to store deleted values in vlog instead of meta byte in sstable


    
