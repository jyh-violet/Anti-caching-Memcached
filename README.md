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

# Compile
```console
./autogen.sh
./configure  heat_count=2 clock_interval=1000000
./make
```
parameters:

heat_count      : heatConst to define temperature of tuples

clock_interval  : interval of global timers in microseconds

# Config file
config.cfg
```
LOAD_SIZE = 50000000

TXN_SIZE = 100000000

LOAD_FILE = "loadc_zipf_int_50M.dat"

TXN_FILE = "txnsc_zipf_int_50M.dat"
```


# run

```console
./memcached -m 4096 -I 2048 -o slab_automove_freeratio=0.1 -o nvm_path=nvm.bin:16G -o ext_path=ssd.bin:120G  -o slab_chunk_max=2048 -t 32 -o ext_threads=16 -o hashpower=28 -P /tmp/memcached.pid 2>out
```

parameters:

-m                          : DRAM size in MB (-m 4096--> 4GB DRAM)

-o nvm_path                 : location and size of NVM file (nvm.bin:16G --> 16GB NVM file located at nvm.bin)

-o ext_path                 : location and size of SSD file (ssd.bin:120G --> 120GB NVM file located at ssd.bin)

-t                          : number of workers

-o ext_threads              : number of evicting threads

-o slab_automove_freeratio  : freeRatio

others                      : Memcached parameters, please refer to     README-forMem.md

