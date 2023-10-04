
```console
sudo apt-get install libevent-dev
sudo apt-get install libconfig-dev
sudo apt-get install libpmem1 librpmem1 libpmemblk1 libpmemlog1 libpmemobj1 libpmempool1
sudo apt-get install libpmem-dev librpmem-dev libpmemblk-dev libpmemlog-dev libpmemobj-dev libpmempool-dev libpmempool-dev
sudo apt-get install libpmem1-debug librpmem1-debug libpmemblk1-debug libpmemlog1-debug libpmemobj1-debug libpmempool1-debug
git clone https://github.com/pmem/pmdk-examples.git

./memcached -m 4196 -vv -u root -l 127.0.0.1 -p 11211 -c 512 -o slab_automove_ratio=0.5 -o nvm_path=/optane/jyh/hmm/mem.bin:8G -o ext_path=/home/jyh/anti-memcached/extern_file:100G -o no_slab_reassign -P /tmp/memcached.pid 2>out

./memcached -m 4196 -u root -l 127.0.0.1 -p 11211 -c 512 -I 2048 -o slab_automove_ratio=0.8 -o nvm_path=/optane/jyh/hmm/mem.bin:8G -o ext_path=/home/jyh/anti-memcached/extern_file:100G -o no_slab_reassign -o slab_chunk_max=2048 -P /tmp/memcached.pid 2>out
```