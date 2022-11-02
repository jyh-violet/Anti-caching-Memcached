#include <iostream>
 #include <libmemcached/memcached.h>
#include <atomic>

#define MaxThreadNum 64


uint64_t* keys;
int threadNum = 1;
uint64_t total = 100000;
uint64_t perTask = 10000;
typedef struct IndexAttributes{
    uint64_t addValue;
    int threadId;
    double usedTime;
}IndexAttributes;
std::atomic_int64_t taskNum;

std::string value = "";
std::string prefix = "key";

void setValue(IndexAttributes* attributes){
    memcached_st *memc;
    memcached_return rc;
    memcached_server_st *server;
    time_t expiration;
    uint32_t  flags;
    memc = memcached_create(NULL);
    server = memcached_server_list_append(NULL,"localhost",11211,&rc);
    rc=memcached_server_push(memc,server);
    memcached_server_list_free(server);


    //    printf("thread%d: (%d, %d)\n ", attributes->threadId,  attributes->startPos, attributes->endPos);
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    uint64_t tNum = taskNum.fetch_add(1);
    while ((tNum + 1) * perTask <= total){
        for (uint64_t i = 0; i < perTask; ++i) {
            std::string key = prefix + std::to_string(keys[i + tNum*perTask]);
            rc=memcached_set(memc,key.c_str(),key.length(),value.c_str(),value.length(),0,0);
            if(rc !=MEMCACHED_SUCCESS){
                std::cout<<"Save ("<<key << "," << value<< ") fail!" << rc <<std::endl;
            }
        }
        tNum = taskNum.fetch_add(1);
    }


    clock_gettime(CLOCK_REALTIME, &endTmp);
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    memcached_free(memc);

}

void readValue(IndexAttributes* attributes){
    memcached_st *memc;
    memcached_return rc;
    memcached_server_st *server;
    time_t expiration;
    uint32_t  flags;
    memc = memcached_create(NULL);
    server = memcached_server_list_append(NULL,"localhost",11211,&rc);
    rc=memcached_server_push(memc,server);
    memcached_server_list_free(server);
    size_t value_length;


    //    printf("thread%d: (%d, %d)\n ", attributes->threadId,  attributes->startPos, attributes->endPos);
    struct timespec startTmp, endTmp;
    clock_gettime(CLOCK_REALTIME, &startTmp);
    uint64_t tNum = taskNum.fetch_add(1);
    while ((tNum + 1) * perTask <= total){
        for (uint64_t i = 0; i < perTask; ++i) {
            std::string key = prefix + std::to_string(keys[i + tNum*perTask]);
            char* result = memcached_get(memc,key.c_str(),key.length(),&value_length,&flags,&rc);
            if(rc != MEMCACHED_SUCCESS){
                std::cout<<"Get " << key << "  fail! "<< rc << std::endl;
            }
        }
        tNum = taskNum.fetch_add(1);
    }

    clock_gettime(CLOCK_REALTIME, &endTmp);
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    memcached_free(memc);

}


int main(int argc, char **argv) {
    if(argc >= 2){
        threadNum = atoi(argv[1]);
    }


    int value_len = 1000;
    for (int i = 0; i < value_len; ++i) {
        value += "1";
    }

    double setT = 0, readT = 0;
    keys = (uint64_t*)malloc(sizeof(uint64_t) * total);

    for (uint64_t i = 0; i < total; ++i) {
        keys[i] = i + 1;
    }
    pthread_t threadId[MaxThreadNum];
    IndexAttributes attributes[MaxThreadNum];

    taskNum.store(0);
    for (int i = 0; i < threadNum; ++i) {
        attributes[i]={
                .addValue = 0,
                .threadId = i
        };
        pthread_create(threadId + i, NULL,
                       reinterpret_cast<void *(*)(void *)>(setValue), (void *) (attributes + i));
    }
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        setT += attributes[i].usedTime;
    }

    taskNum.store(0);
    for (int i = 0; i < threadNum; ++i) {
        attributes[i]={
                .addValue = 0,
                .threadId = i
        };
        pthread_create(threadId + i, NULL,
                       reinterpret_cast<void *(*)(void *)>(readValue), (void *) (attributes + i));
    }
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        readT += attributes[i].usedTime;
    }
    printf("%ld, %d, %ld, %.2fops, %.2fops\n",
           total, threadNum, value.length(),
           total / perTask * perTask * threadNum / setT, total / perTask * perTask * threadNum/readT);
    fflush(stdout);

    return 0;
}
