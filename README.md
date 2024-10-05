# Dependencies:
1. refer to README-forMem.md
2. pmdk


```console
sudo apt-get install libevent-dev
sudo apt-get install libconfig-dev
sudo apt-get install libpmem1 librpmem1 libpmemblk1 libpmemlog1 libpmemobj1 libpmempool1
sudo apt-get install libpmem-dev librpmem-dev libpmemblk-dev libpmemlog-dev libpmemobj-dev libpmempool-dev libpmempool-dev
sudo apt-get install libpmem1-debug librpmem1-debug libpmemblk1-debug libpmemlog1-debug libpmemobj1-debug libpmempool1-debug
```

# Major Modifications

- memcached.h
    - modifications about `item` struct, temperature levels definition
- item.c, slab.c
    - modifications about Lazy LRU and partitioned memory management
    - adding NVM slabs

- storage.c
    - modifications about data swapping: 
        - data eviction
            - nvm_write_thread: DRAM to NVM
            - storage_write_thread: NVM to SSD
        - data fecthing
            - nvm_swap_thread: DRAM to NVM
            - _swap_thread: SSD to NVM or DRAM

- extstore,c
    - encapsulation of SSD operations


- memcached.c:
    - add `test` method to run the YCSB benchmark

# Compile
```console
./autogen.sh
./configure  heat_count=2 clock_interval=10000 nvm_dram=8
./make
```
parameters for configure:

- nvm_as_dram=yes : the Anti-2 variant
- mem_mod=yes     : the Anti-NVM variant

# Genenrate data

```
./memcached_client/workloads/generate_all_workloads.sh 
```

# Config file
config.cfg: which will be read by the program automatically and should be in the same path as the executable file. The following is an example.
```
# recordcount in YCSB config file
LOAD_SIZE = 50000000

# operationcount in YCSB config file
TXN_SIZE = 100000000

# path of the data files generated in the former step
LOAD_FILE = "memcached_client/workloads/data/loadc_zipf_int_50M.dat" 

TXN_FILE = "memcached_client/workloads/data/txnsc_zipf_int_50M.dat"
```

# run

```console
./memcached -m 4096 -o nvm_path=nvm.bin:32G -o ext_path=ssd.bin:120G  -t 32 -o ext_threads=8 2>out
```
parameters:

-m                          : DRAM size in MB (-m 4096--> 4GB DRAM)

-o nvm_path                 : location and size of NVM file (nvm.bin:16G --> 16GB NVM file located at nvm.bin)

-o ext_path                 : location and size of SSD file (ssd.bin:120G --> 120GB SSD file located at ssd.bin)

-t                          : number of workers

-o ext_threads              : number of evicting threads


others                      : Memcached parameters, please refer to     README-forMem.md

result can be found in ./log file

# Bibliography
```
@inproceedings{DBLP:conf/edbt/JiH0HT24,
  author       = {***},
  editor       = {***},
  title        = {***},
  booktitle    = {Proceedings 27th International Conference on Extending Database Technology,
                  {EDBT} 2024, Paestum, Italy, March 25 - March 28},
  pages        = {474--487},
  publisher    = {OpenProceedings.org},
  year         = {2024},
  timestamp    = {Fri, 22 Mar 2024 11:12:28 +0100}
}
```
