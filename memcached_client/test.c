//
// Created by workshop on 7/13/2022.
//

#include <stdio.h>
#include <pthread.h>
#include "stdint.h"
#include <unistd.h>
#include <fcntl.h>
_Atomic int n = 0;

void increase(){
    for (int i = 0; i < 100; ++i) {
        n++;
    }
}

typedef struct Attributes{
    int threadId;
    double usedTime;
    int fd;
}Attributes;

uint64_t total = 1L << 34;
uint64_t page_size = 64 * 1024 * 1024;
uint64_t bufLen = 64 * 1024;
int threadNum = 4;
void no_intersect(Attributes* attributes){
    char src[bufLen];
    memset(src, 'a', bufLen);
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    for (uint64_t i = 0; i < total / page_size / threadNum; ++i) {
        for (uint64_t j = 0; j < page_size / bufLen; ++j) {
            uint64_t off = (i * threadNum + attributes->threadId) * page_size + j * bufLen;
            pwrite(attributes->fd, src , bufLen, off);
        }
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
}

void intersect(Attributes* attributes){
    char src[bufLen];
    memset(src, 'a', bufLen);
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    for (uint64_t i = 0; i < total / page_size ; ++i) {
        for (uint64_t j = 0; j < page_size / bufLen / threadNum; ++j) {
            uint64_t off = i * page_size + (j * threadNum + attributes->threadId) * bufLen;
            pwrite(attributes->fd, src , bufLen, off);
        }
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
}

int main(){
    pthread_t thread[64];
    Attributes attributes[64];
    int threadNums[6] = {2, 4, 8, 16, 32, 64};
    uint64_t bufLens[6] = {1024*16, 1024*32, 64*1024, 128 * 1024, 256*1024, 512 * 1024};
    int fd = open("test_f", O_RDWR | O_CREAT, 0644);
    double no_intersect_time = 0, intersect_time = 0;
    for (int k = 0; k < 6; ++k) {
        for (int m = 0; m < 6; ++m) {
            threadNum = threadNums[k];
            bufLen = bufLens[m];
            for (int i = 0; i < threadNum; ++i) {
                attributes[i].threadId = i;
                attributes[i].fd = fd;
                pthread_create(&thread[i], NULL,no_intersect,  (void *)&attributes[i]);
            }
            no_intersect_time = 0;
            for (int i = 0; i < threadNum; ++i) {
                pthread_join(thread[i], NULL);
                no_intersect_time += attributes[i].usedTime;
            }
            for (int i = 0; i < threadNum; ++i) {
                attributes[i].threadId = i;
                attributes[i].fd = fd;
                pthread_create(&thread[i], NULL,intersect,  (void *)&attributes[i]);
            }
            intersect_time = 0;
            for (int i = 0; i < threadNum; ++i) {
                pthread_join(thread[i], NULL);
                intersect_time += attributes[i].usedTime;
            }
            printf("threadNum:%d, bufLen:%ld no_intersect:%.2fMB/s, intersect:%.2fMB/s\n",
                   threadNum, bufLen,  total / no_intersect_time / 1024/1024 * threadNum, total / intersect_time / 1024/1024 * threadNum);
        }
    }



    return 0;
}