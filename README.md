# go-wiskey

Golang implementation
of [Wiskey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)
paper

# Description

Dead simple lsm implementation which stores values in the vlog which
decreases write amplification of lsm tree during merging

# Things that are implemented

1. [X] SSTable
    - [X] Create sstable
    - [X] Read from sstable
2. [X] Memtable(in memory redblack tree that stores the data and flushes it once memory is full)
    - [X] Put
    - [X] Delete
    - [X] Get
3. [X] Lsm tree
    - [X] Put 
    - [X] Get
    - [X] Delete
4. [X] Http interface 
    - [X] Http Get
    - [X] Http Put
    - [X] Http Delete
5. [X] Crash recovery
    - [X] Store the last head position in the separate file
    - [X] Store al values from head to tail into the memtable during recovery
6. [ ] Merge sstable files
7. [o] Cli interface
    - [X] specify sstable path 
    - [X] specify vlog path
    - [X] specify checkpoint path
    - [ ] specify memtable size
    - [ ] specify sstable size
      

    
