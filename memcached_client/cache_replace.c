//
// Created by workshop on 9/13/2022.
//

#include <stdint-gcc.h>
#include <bits/types/FILE.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include "pthread.h"
#include "config.h"

uint64_t* txn_keys;
uint64_t RUN_SIZE;
uint64_t KEY_COUNT;

typedef uint32_t rel_time_t;
#define HOT_ITEM_R_COUNT 2
#define IsHot(node) (node->time == current_time && node->read_count >= HOT_ITEM_R_COUNT)
//#define CLOCK_INTERVAL 1000000

typedef enum Operation{
    INSERT,
    READ,
    UPDATE,
    SCAN
}Operation;

typedef enum Strategy{
    LRU,
    Lazy_LRU,
    ALRU,
    SAMPLE_LRU,
    FIFO
}Strategy;


Operation* operations;


typedef struct LRU_node{
    volatile struct LRU_node *next;
    volatile struct LRU_node *prev;
    pthread_mutex_t node_lock;

    struct {
        volatile rel_time_t time : 32;
        uint8_t read_count : 8;
        uint8_t flag : 8;
        uint8_t lru_id : 8;
    };

}LRU_node __attribute__ ((aligned (64)));

#define MAX_THREAD 64

LRU_node* lru_head[MAX_THREAD];
LRU_node* lru_tail[MAX_THREAD];
pthread_mutex_t lru_lock[MAX_THREAD];
size_t lru_size[MAX_THREAD];
LRU_node* lruNodes;
char* data;

#define value_len 64
double cache_ratio = 0.1;
int ThreadNum;
uint64_t the_n;
double zipf_theta;
double denom;
double zeta_2_theta;

int GenerateInteger(const int min, const int max) {
    return rand() % (max - min + 1) + min;
}

double zeta(uint64_t n, double theta) {
    double sum = 0;
    for (uint64_t i = 1; i <= n; i++) sum += pow(1.0 / i, theta);
    return sum;
}

void ZipfDistribution(const uint64_t n, const double theta){
    // range: 1-n
    the_n = n;
    zipf_theta = theta;
    zeta_2_theta = zeta(2, zipf_theta);
    denom = zeta(the_n, zipf_theta);
}





uint64_t GetNextNumber() {
    double alpha = 1 / (1 - zipf_theta);
    double zetan = denom;
    double eta =
            (1 - pow(2.0 / the_n, 1 - zipf_theta)) / (1 - zeta_2_theta / zetan);
    double u = ((double )rand()) / INT32_MAX;
    double uz = u * zetan;
    if (uz < 1) return 1;
    if (uz < 1 + pow(0.5, zipf_theta)) return 2;
    return 1 + (uint64_t) (the_n * pow(eta * u - eta + 1, alpha));
}



void ycsb_load_run_randint(const char* txn_file)
{
    for (int i = 0; i < RUN_SIZE; ++i) {
        txn_keys[i] = GetNextNumber();
    }
/*
    FILE *infile_txn = fopen(txn_file, "r");
    if(infile_txn == NULL){
        fprintf(stderr,"open txn_file error:%s \n", txn_file);
        exit(0);
    }
    uint64_t count = 0;
    char line[1000];

    char* insert = "INSERT";
    char* read = "READ";
    char* scan = "SCAN";
    char* update = "UPDATE";

    while ((count < RUN_SIZE) && !(feof(infile_txn))) {
        memset(line, 0, 1000);
        if(fgets(line, 1000, infile_txn) == NULL){
            fprintf(stderr,"infile_txn read error\n");
        }
        char * op = strtok(line, " ");
        if (strcmp(op, insert) == 0) {
            operations[count] = INSERT;
        } else if (strcmp(op, read) == 0) {
            operations[count] = READ;
        } else if (strcmp(op, scan) == 0) {
            operations[count] = SCAN;
        } else if (strcmp(op, update) == 0){
            operations[count] = UPDATE;
        } else{
            printf("UNRECOGNIZED!\n");
            return;
        }
        char* num = strtok(NULL, " ");
#ifdef strKey
        strcpy(txn_keys[count], prefix);
        strcat(txn_keys[count], num);
        for (int i = 0; i < keyMaxLen - strlen(prefix) - strlen(num); ++i) {
            strcat(txn_keys[count], "0");
        }
#else
        txn_keys[count] = atol(num);
#endif
        count++;
    }

    if(count < RUN_SIZE){
        fprintf(stderr, "load txn error, count:%ld, RUN_SIZE:%ld\n", count, RUN_SIZE);
        exit(0);
    }
    */
}

typedef struct Attributes{
    int threadId;
    double usedTime;
    uint64_t read;
    uint64_t cache_hit;
}Attributes;
uint64_t taskNum = 0;
uint64_t perTask = 100000;
#define IN_DRAM 1
#define LINKED 2
int current_time = 0;

volatile int workEnd = 0;


void clock_handler(){
    while (!workEnd){
        current_time ++;
        usleep(CLOCK_INTERVAL);
    }
}

void unlink_node_q(LRU_node* node){
    if((node->flag & LINKED) == 0){
        fprintf(stderr, "unlink_node:%p error, flag:%d\n", (void *)node,  node->flag);
        exit(0);
    }
    int lru_id = node->lru_id;
    LRU_node** head_p = &lru_head[lru_id];
    LRU_node** tail_p = &lru_tail[lru_id];
    if(*head_p == node){
        *head_p = (LRU_node*)node->next;
    }
    if(*tail_p == node){
        *tail_p =  (LRU_node*)node->prev;
    }
    if(node->prev){
        node->prev->next = node->next;
    }
    if(node->next){
        node->next->prev = node->prev;
    }
    node->flag &= (~LINKED);
    lru_size[lru_id]--;
}

void unlink_node(LRU_node* node){
    int lru_id = node->lru_id;
    pthread_mutex_lock(&lru_lock[lru_id]);
    unlink_node_q(node);
    pthread_mutex_unlock(&lru_lock[lru_id]);
}

void link_node_q(LRU_node* node, int lru_id){
    if((node->flag & LINKED)){
        fprintf(stderr, "link_node error, flag:%d\n", node->flag);
        return;
    }
    LRU_node** head_p = &lru_head[lru_id];
    LRU_node** tail_p = &lru_tail[lru_id];
    node->prev = 0;
    node->next = (volatile LRU_node*) (*head_p);
    if(node->next){
        node->next->prev =  (volatile LRU_node*)node;
    }
    *head_p = node;
    if(*tail_p == 0){
        *tail_p = node;
    }
    node->lru_id = lru_id;
    node->flag |= (LINKED);
    lru_size[lru_id]++;
}

int link_node(LRU_node* node, int lru_id, int capacity){
    retry:
    pthread_mutex_lock(&lru_lock[lru_id]);
    if(lru_size[lru_id] + 1 > capacity){
        int tries = 10;
        LRU_node * next = NULL;
        LRU_node *search = lru_tail[lru_id];
        for (; tries >=0 && search != NULL; tries --, search = next){
            next = (LRU_node*)search->prev;
            if(pthread_mutex_trylock(&(search->node_lock)) == 0){
                unlink_node_q(search);
                pthread_mutex_unlock(&(search->node_lock));
                break;
            }
        }
    }
    if(lru_size[lru_id] + 1 > capacity){
        pthread_mutex_unlock(&lru_lock[lru_id]);
        goto retry;
    }
    link_node_q(node, lru_id);
    pthread_mutex_unlock(&lru_lock[lru_id]);
    return 1;
}

void lru_worker(Attributes* attributes){
    int capacity = KEY_COUNT * cache_ratio / ThreadNum;
    attributes->read = 0;
    attributes->cache_hit = 0;
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);

    char value[value_len];

    uint64_t tNum = __sync_fetch_and_add(&taskNum, 1);
    while (tNum * perTask < RUN_SIZE){
        for (uint64_t i = 0; i < perTask; ++i) {
            attributes->read ++;
            uint64_t key =  txn_keys[i + tNum*perTask] % KEY_COUNT;
            key = key % KEY_COUNT;
            pthread_mutex_lock(&(lruNodes[key].node_lock));
            if(lruNodes[key].time == current_time){
                if(lruNodes[key].read_count <= (HOT_ITEM_R_COUNT <<2)){
                    lruNodes[key].read_count++;
                }
            } else{
                lruNodes[key].time = current_time;
                lruNodes[key].read_count =  (lruNodes[key].read_count >> 1) +1;
            }
            if((lruNodes[key].flag & LINKED)){
                attributes->cache_hit ++;
                unlink_node(&lruNodes[key]);
                link_node(&(lruNodes[key]), attributes->threadId, capacity);
            } else{
                link_node(&(lruNodes[key]), attributes->threadId, capacity);
            }
            pthread_mutex_unlock(&(lruNodes[key].node_lock));
            strncpy(value, data + key * value_len, value_len);
        }
        tNum = __sync_fetch_and_add(&taskNum, 1);
//        printf("thread:%d, tNum:%ld\n", attributes->threadId, tNum);
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
}

void test_lru(int threadNum){
    pthread_t clock_tid;
    pthread_create(&clock_tid, NULL,  (void *(*)(void *)) clock_handler, NULL);

    Attributes attributes[MAX_THREAD];
    pthread_t thread_id[MAX_THREAD];
    ThreadNum = threadNum;
    for (int i = 0; i < threadNum; ++i) {
        attributes[i].threadId = i;
        pthread_create(&thread_id[i], NULL,  (void *(*)(void *)) lru_worker, &attributes[i]);
    }
    uint64_t read = 0;
    uint64_t cache_hit = 0;
    double time = 0;
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(thread_id[i], NULL);
        read += attributes[i].read;
        cache_hit += attributes[i].cache_hit;
        time += attributes[i].usedTime;
    }
    fprintf(stderr, "%d, %d, %ld,%ld, %d,%d, %.2f,  %.3f, %.2f, %d, %.2f\n",
            LRU, threadNum, read, cache_hit, CLOCK_INTERVAL/1000, HOT_ITEM_R_COUNT,
            cache_ratio, ((double)cache_hit) / read, read / time * threadNum, 0, zipf_theta);

    workEnd = 1;
    pthread_join(clock_tid, NULL);
}

int MaintainThreads = 4;
#define PerMaintain 8
void lru_maintainer(Attributes* attributes){
    int perThread = ThreadNum / MaintainThreads;
    if(perThread < PerMaintain){
        perThread = PerMaintain;
    }
    if(perThread > ThreadNum){
        perThread = ThreadNum;
    }
    int startThread =  perThread * attributes->threadId;
    if(attributes->threadId < ThreadNum % MaintainThreads){
        startThread += attributes->threadId;
        perThread += 1;
    } else{
        startThread += ThreadNum % MaintainThreads;
    }
    if(startThread >= ThreadNum){
        return;
    }
    while (!workEnd){
        for (int i = startThread; i < startThread + perThread; ++i) {
            int tries = 10;
            LRU_node * next = NULL;
            pthread_mutex_lock(&lru_lock[i]);
            LRU_node * node = lru_tail[i];
            for (; tries >=0 && node != NULL; tries --, node = next){
                next = (LRU_node*)node->prev;
                if(IsHot(node)){
                    if(pthread_mutex_trylock(&(node->node_lock)) == 0){
                        unlink_node_q(node);
                        link_node_q(node, i);
                        pthread_mutex_unlock(&(node->node_lock));
                        break;
                    }
                }
            }
            pthread_mutex_unlock(&lru_lock[i]);
        }
    }
}

void lazy_lru_worker(Attributes* attributes){
    int capacity = KEY_COUNT * cache_ratio / ThreadNum;
    attributes->read = 0;
    attributes->cache_hit = 0;
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    char value[value_len];

    uint64_t tNum = __sync_fetch_and_add(&taskNum, 1);
    while (tNum * perTask < RUN_SIZE){
        for (uint64_t i = 0; i < perTask; ++i) {
            attributes->read ++;
            uint64_t key =  txn_keys[i + tNum*perTask] % KEY_COUNT;
            key = key % KEY_COUNT;
            pthread_mutex_lock(&(lruNodes[key].node_lock));
            if(lruNodes[key].time == current_time){
                if(lruNodes[key].read_count <= (HOT_ITEM_R_COUNT <<2)){
                    lruNodes[key].read_count++;
                }
            } else{
                lruNodes[key].time = current_time;
                lruNodes[key].read_count =  (lruNodes[key].read_count >> 1) +1;
            }
            if((lruNodes[key].flag & LINKED)){
                attributes->cache_hit ++;
            } else{
                link_node(&(lruNodes[key]), attributes->threadId, capacity);
            }
            pthread_mutex_unlock(&(lruNodes[key].node_lock));
            strncpy(value, data + key * value_len, value_len);
        }
        tNum = __sync_fetch_and_add(&taskNum, 1);
//        printf("thread:%d, tNum:%ld\n", attributes->threadId, tNum);
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
}

void test_lazy_lru(int threadNum){
    pthread_t clock_tid, lru_tid[MaintainThreads];
    pthread_create(&clock_tid, NULL,  (void *(*)(void *)) clock_handler, NULL);
    Attributes m_attributes[MaintainThreads];
    for (int i = 0; i < MaintainThreads; ++i) {
        m_attributes[i].threadId = i;
        pthread_create(&lru_tid[i], NULL,  (void *(*)(void *)) lru_maintainer, &m_attributes);
    }


    Attributes attributes[MAX_THREAD];
    pthread_t thread_id[MAX_THREAD];
    ThreadNum = threadNum;
    for (int i = 0; i < threadNum; ++i) {
        attributes[i].threadId = i;
        pthread_create(&thread_id[i], NULL,  (void *(*)(void *)) lazy_lru_worker, &attributes[i]);
    }
    uint64_t read = 0;
    uint64_t cache_hit = 0;
    double time = 0;
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(thread_id[i], NULL);
        read += attributes[i].read;
        cache_hit += attributes[i].cache_hit;
        time += attributes[i].usedTime;
    }
    fprintf(stderr, "%d, %d, %ld,%ld, %d,%d, %.2f,  %.3f, %.2f, %d, %.2f\n",
            Lazy_LRU, threadNum, read, cache_hit, CLOCK_INTERVAL/1000, HOT_ITEM_R_COUNT,
            cache_ratio, ((double)cache_hit) / read, read / time * threadNum, MaintainThreads, zipf_theta);
    workEnd = 1;
    pthread_join(clock_tid, NULL);
    for (int i = 0; i < MaintainThreads; ++i) {
        pthread_join(lru_tid[i], NULL);
    }

}
#define SAMPLE_NUM 10
void alru_worker(Attributes* attributes){
    attributes->read = 0;
    attributes->cache_hit = 0;
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    char value[value_len];

    volatile LRU_node* sampled[SAMPLE_NUM];
    uint64_t tNum = __sync_fetch_and_add(&taskNum, 1);
    while (tNum * perTask < RUN_SIZE){
        for (uint64_t i = 0; i < perTask; ++i) {
            attributes->read ++;
            uint64_t key =  txn_keys[i + tNum*perTask] % KEY_COUNT;
            key = key % KEY_COUNT;
            pthread_mutex_lock(&(lruNodes[key].node_lock));
            if(lruNodes[key].time == current_time){
                if(lruNodes[key].read_count <= (HOT_ITEM_R_COUNT <<2)){
                    lruNodes[key].read_count++;
                }
            } else{
                lruNodes[key].time = current_time;
                lruNodes[key].read_count =  (lruNodes[key].read_count >> 1) +1;
            }
            int local_sample;
            if((lruNodes[key].flag & LINKED)){
                attributes->cache_hit ++;
            } else{
                pthread_mutex_lock(&lru_lock[attributes->threadId]);
                if(lru_size[attributes->threadId] + 1 > KEY_COUNT * cache_ratio / ThreadNum){
                    memset(sampled, 0, sizeof (sampled));
                    retry_evict:
                    local_sample= 0;
                    volatile LRU_node *search = lru_tail[attributes->threadId], *next = NULL;
                    while (local_sample < SAMPLE_NUM && search != NULL){
                        next = search->prev;
                        sampled[local_sample ++] = search;
                        search = next;
                    }
                    int evictIndex = 0;
                    for (int j = 1; j < local_sample; ++j) {
                        if (sampled[j]->time < sampled[evictIndex]->time){
                            evictIndex = j;
                        }
                    }
                    if(sampled[evictIndex]!= NULL && pthread_mutex_trylock(&(sampled[evictIndex]->node_lock)) == 0){
                        unlink_node_q(sampled[evictIndex]);
                        pthread_mutex_unlock(&(sampled[evictIndex]->node_lock));
                    } else{
                        goto retry_evict;
                    }
                }
                link_node_q(&lruNodes[key], attributes->threadId);
                pthread_mutex_unlock(&lru_lock[attributes->threadId]);
            }
            pthread_mutex_unlock(&(lruNodes[key].node_lock));
            strncpy(value, data + key * value_len, value_len);
        }
        tNum = __sync_fetch_and_add(&taskNum, 1);
//                printf("thread:%d, tNum:%ld\n", attributes->threadId, tNum);
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
}

void test_alru(int threadNum){
    pthread_t clock_tid;
    pthread_create(&clock_tid, NULL,  (void *(*)(void *)) clock_handler, NULL);

    Attributes attributes[MAX_THREAD];
    pthread_t thread_id[MAX_THREAD];
    ThreadNum = threadNum;
    for (int i = 0; i < threadNum; ++i) {
        attributes[i].threadId = i;
        pthread_create(&thread_id[i], NULL,  (void *(*)(void *)) alru_worker, &attributes[i]);
    }
    uint64_t read = 0;
    uint64_t cache_hit = 0;
    double time = 0;
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(thread_id[i], NULL);
        read += attributes[i].read;
        cache_hit += attributes[i].cache_hit;
        time += attributes[i].usedTime;
    }
    fprintf(stderr, "%d, %d, %ld,%ld, %d,%d, %.2f,  %.3f, %.2f, %d, %.2f\n",
            ALRU, threadNum, read, cache_hit, CLOCK_INTERVAL/1000, HOT_ITEM_R_COUNT,
            cache_ratio, ((double)cache_hit) / read, read / time * threadNum, SAMPLE_NUM, zipf_theta);
    workEnd = 1;
    pthread_join(clock_tid, NULL);
}

void sample_lru_worker(Attributes* attributes){
    int capacity = KEY_COUNT * cache_ratio / ThreadNum;
    attributes->read = 0;
    attributes->cache_hit = 0;
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    char value[value_len];

    uint64_t tNum = __sync_fetch_and_add(&taskNum, 1);
    while (tNum * perTask < RUN_SIZE){
        for (uint64_t i = 0; i < perTask; ++i) {
            attributes->read ++;
            uint64_t key =  txn_keys[i + tNum*perTask] % KEY_COUNT;
            key = key % KEY_COUNT;
            pthread_mutex_lock(&(lruNodes[key].node_lock));
            if(lruNodes[key].time == current_time){
                if(lruNodes[key].read_count <= (HOT_ITEM_R_COUNT <<2)){
                    lruNodes[key].read_count++;
                }
            } else{
                lruNodes[key].time = current_time;
                lruNodes[key].read_count =  (lruNodes[key].read_count >> 1) +1;
            }
            if((lruNodes[key].flag & LINKED)){
                attributes->cache_hit ++;
                if((i + tNum*perTask) % SAMPLE_INTERVAL == 0){
                    unlink_node(&lruNodes[key]);
                    link_node(&(lruNodes[key]), attributes->threadId, capacity);
                }
            } else{
                link_node(&(lruNodes[key]), attributes->threadId,capacity);
            }
            pthread_mutex_unlock(&(lruNodes[key].node_lock));
            strncpy(value, data + key * value_len, value_len);
        }
        tNum = __sync_fetch_and_add(&taskNum, 1);
//                printf("thread:%d, tNum:%ld\n", attributes->threadId, tNum);
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
}


void test_sample_lru(int threadNum){
    pthread_t clock_tid;
    pthread_create(&clock_tid, NULL,  (void *(*)(void *)) clock_handler, NULL);

    Attributes attributes[MAX_THREAD];
    pthread_t thread_id[MAX_THREAD];
    ThreadNum = threadNum;
    for (int i = 0; i < threadNum; ++i) {
        attributes[i].threadId = i;
        pthread_create(&thread_id[i], NULL,  (void *(*)(void *)) sample_lru_worker, &attributes[i]);
    }
    uint64_t read = 0;
    uint64_t cache_hit = 0;
    double time = 0;
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(thread_id[i], NULL);
        read += attributes[i].read;
        cache_hit += attributes[i].cache_hit;
        time += attributes[i].usedTime;
    }
    fprintf(stderr, "%d, %d, %ld,%ld, %d,%d, %.2f,  %.3f, %.2f, %d, %.2f\n",
            SAMPLE_LRU, threadNum, read, cache_hit, CLOCK_INTERVAL/1000, HOT_ITEM_R_COUNT,
            cache_ratio, ((double)cache_hit) / read, read / time * threadNum, SAMPLE_INTERVAL, zipf_theta);

    workEnd = 1;
    pthread_join(clock_tid, NULL);
}

void fifo_worker(Attributes* attributes){
    int capacity = KEY_COUNT * cache_ratio / ThreadNum;
    attributes->read = 0;
    attributes->cache_hit = 0;
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    char value[value_len];

    uint64_t tNum = __sync_fetch_and_add(&taskNum, 1);
    while (tNum * perTask < RUN_SIZE){
        for (uint64_t i = 0; i < perTask; ++i) {
            attributes->read ++;
            uint64_t key =  txn_keys[i + tNum*perTask] % KEY_COUNT;
            key = key % KEY_COUNT;
            pthread_mutex_lock(&(lruNodes[key].node_lock));
            if(lruNodes[key].time == current_time){
                if(lruNodes[key].read_count <= (HOT_ITEM_R_COUNT <<2)){
                    lruNodes[key].read_count++;
                }
            } else{
                lruNodes[key].time = current_time;
                lruNodes[key].read_count =  (lruNodes[key].read_count >> 1) +1;
            }
            if((lruNodes[key].flag & LINKED)){
                attributes->cache_hit ++;
            } else{
                link_node(&(lruNodes[key]), attributes->threadId,capacity);
            }
            pthread_mutex_unlock(&(lruNodes[key].node_lock));
            strncpy(value, data + key * value_len, value_len);
        }
        tNum = __sync_fetch_and_add(&taskNum, 1);
        //                printf("thread:%d, tNum:%ld\n", attributes->threadId, tNum);
    }
    clock_gettime(CLOCK_REALTIME, &endTmp);
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
}

void test_FIFO(int threadNum){
    pthread_t clock_tid;
    pthread_create(&clock_tid, NULL,  (void *(*)(void *)) clock_handler, NULL);

    Attributes attributes[MAX_THREAD];
    pthread_t thread_id[MAX_THREAD];
    ThreadNum = threadNum;
    for (int i = 0; i < threadNum; ++i) {
        attributes[i].threadId = i;
        pthread_create(&thread_id[i], NULL,  (void *(*)(void *)) fifo_worker, &attributes[i]);
    }
    uint64_t read = 0;
    uint64_t cache_hit = 0;
    double time = 0;
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(thread_id[i], NULL);
        read += attributes[i].read;
        cache_hit += attributes[i].cache_hit;
        time += attributes[i].usedTime;
    }
    fprintf(stderr, "%d, %d, %ld,%ld, %d,%d, %.2f,  %.3f, %.2f, %d, %.2f\n",
            FIFO, threadNum, read, cache_hit, CLOCK_INTERVAL/1000, HOT_ITEM_R_COUNT,
            cache_ratio, ((double)cache_hit) / read, read / time * threadNum, 0, zipf_theta);

    workEnd = 1;
    pthread_join(clock_tid, NULL);
}

int main(int argc, char **argv){
    ThreadNum = 1;
    RUN_SIZE = 100000000;
    KEY_COUNT = 50000000;
    srand((unsigned)time(NULL));

    if (argc > 1){
        ThreadNum = atoi(argv[1]);
    }
    int lru_type = 0;
    if(argc > 2){
        lru_type = atoi(argv[2]);
    }
    if(argc > 3){
        cache_ratio = atof(argv[3]);
    }
    if(argc > 4){
        double theata = atof(argv[4]);
        ZipfDistribution(KEY_COUNT, theata);
    }

    char* txn_file="workloads/data/txnsc_zipf_int_50M.dat";
    txn_keys = (uint64_t*)malloc(sizeof(uint64_t) * RUN_SIZE);
    operations = (Operation*) malloc(sizeof(Operation) * RUN_SIZE);
    for (int i = 0; i < MAX_THREAD; ++i) {
        pthread_mutex_init(&lru_lock[i], NULL);
    }
    lruNodes = malloc(sizeof (LRU_node) * RUN_SIZE);
    memset(lruNodes, 0,  sizeof (LRU_node) * RUN_SIZE);
    data = malloc(value_len * RUN_SIZE);
    memset(lruNodes, 0,  sizeof (LRU_node) * RUN_SIZE);
    for (uint64_t i = 0; i < RUN_SIZE; ++i) {
        pthread_mutex_init(&lruNodes[i].node_lock, NULL);
    }
    ycsb_load_run_randint(txn_file);
    switch (lru_type) {
        case 0:
            test_lru(ThreadNum);
            break;
        case 1:
            test_lazy_lru(ThreadNum);
            break;
        case 2:
            test_alru(ThreadNum);
            break;
        case 3:
            test_sample_lru(ThreadNum);
            break;
        case 4:
            test_FIFO(ThreadNum);
            break;
    }
//    test_lru(ThreadNum);
////    memset(lruNodes, 0,  sizeof (LRU_node) * RUN_SIZE);
////    for (uint64_t i = 0; i < RUN_SIZE; ++i) {
////        pthread_mutex_init(&lruNodes[i].node_lock, NULL);
////    }
////    memset(lru_tail, 0,  sizeof (lru_tail));
////    memset(lru_tail, 0,  sizeof (lru_head));
////    memset(lru_size, 0,  sizeof (lru_size));
////    taskNum = 0;
//    test_lazy_lru(ThreadNum);
    return 0;
}
