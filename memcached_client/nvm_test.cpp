//
// Created by workshop on 6/1/2022.
//

#include <cstdio>
#include <libpmem.h>
#include <cstdlib>
#include <cerrno>
#include <unistd.h>

#define ENABLE_NVM

inline char* mallocNVM(const char* filename, size_t size){
    char* str;
#ifdef ENABLE_NVM
    if(access(filename, F_OK) == 0){
        remove(filename);
    }
    size_t mapped_len;
    int is_pmem = 1;
    str = (char*) pmem_map_file(filename, size,
                                PMEM_FILE_CREATE|PMEM_FILE_EXCL,
                                0666, &mapped_len, &is_pmem);
    if(str == NULL){
        fprintf(stderr, "mallocNVM pmem_map_file:%s error. size:%ld, mapped_len:%ld, is_pmem:%d, errno:%d  ",
                filename, size, mapped_len, is_pmem, errno);
        exit(1);
    }
#else
    posix_memalign((void**)(&str), 64, size);
#endif
    return str;
}

void test_NVM(){

}

int main(){

}