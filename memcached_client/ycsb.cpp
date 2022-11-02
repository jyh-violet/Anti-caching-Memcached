//
// Created by workshop on 6/15/2022.
//
#include <iostream>
 #include <libmemcached/memcached.h>
#include <atomic>
#include <fstream>
#include <libconfig.h>

#define MaxThreadNum 64

using namespace std;

static uint64_t LOAD_SIZE = 32000000;
static uint64_t RUN_SIZE = 32000000;
int threadNum = 1;
const char* init_file, *txn_file;


enum Operation{
    INSERT,
    READ,
    UPDATE,
    SCAN
};

uint64_t* init_keys;
uint64_t* txn_keys;
Operation* operations;
uint64_t perTask = 10000;
std::atomic_int64_t taskNum;


#ifdef test_clht
#define index clht
#define put_fn(key, value) clht_put(clht, key, value)
#define read_fn(key) (value_t*)clht_get(clht, key)
#else
#ifdef test_lfht
#define index (&chainHashMap)
#define put_fn(key, value) lf_chain_put(&chainHashMap, key, value)
#define read_fn(key) (value_t*)lf_chain_find(&chainHashMap,  key)
#endif
#endif

typedef struct IndexAttributes{
    uint64_t addValue;
    int threadId;
    int failed;
    double usedTime;
}IndexAttributes;
std::string value = "";
std::string prefix = "key";
int value_len = 1000;

#define traceLen 100000000
void loadData(IndexAttributes* attributes){
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
    while ((tNum + 1) * perTask <= LOAD_SIZE){
        printf("thread:%d, load tNum:%ld\n", attributes->threadId, tNum);
        for (uint64_t i = 0; i < perTask; ++i) {
            std::string key = prefix + std::to_string(init_keys[i + tNum*perTask]);
            rc=memcached_set(memc,key.c_str(),key.length(),value.c_str(),value.length(),0,0);
            if(rc !=MEMCACHED_SUCCESS){
                attributes->failed ++;
//                std::cout<<"Save ("<<key << "," << value<< ") fail!" << rc <<std::endl;
            }
        }
        tNum = taskNum.fetch_add(1);
    }


    clock_gettime(CLOCK_REALTIME, &endTmp);
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    memcached_free(memc);
    printf("thread:%d, failed:%d\n", attributes->threadId, attributes->failed);
}
void transaction(IndexAttributes* attributes){
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
    int setFailed = 0, readFailed = 0;
    while ((tNum + 1) * perTask <= RUN_SIZE){
        printf("thread:%d, txn tNum:%ld\n", attributes->threadId, tNum);
        for (uint64_t i = 0; i < perTask; ++i) {
            //        value_t value = keys[i] + attributes->addValue;
            switch (operations[i + tNum*perTask]) {
                case INSERT:
                    case UPDATE:
                    {
                        std::string key = prefix + std::to_string(txn_keys[i + tNum*perTask]);
                        rc=memcached_set(memc,key.c_str(),key.length(),value.c_str(),value.length(),0,0);
                        if(rc !=MEMCACHED_SUCCESS){
                            setFailed ++;
//                            std::cout<<"Save ("<<key << "," << value<< ") fail!" << rc <<std::endl;
                        }
                        break;
                    }
                    case READ:
                    {
                        std::string key = prefix + std::to_string(txn_keys[i + tNum*perTask]);
                        char* result = memcached_get(memc,key.c_str(),key.length(),&value_length,&flags,&rc);
                        if(rc != MEMCACHED_SUCCESS){
                            readFailed ++;
//                            std::cout<<"Get " << key << "  fail! "<< rc << std::endl;
                        }
                        break;
                    }
                    default:
                        fprintf(stderr, "not support operation:%d", operations[i + tNum*perTask]);
            }

            //            if(i % traceLen == 0){
            //                vmlog(LOG, "setValue (%ld)",  + tNum*perTask);
            //            }
        }

        tNum = taskNum.fetch_add(1);
    }

    clock_gettime(CLOCK_REALTIME, &endTmp);
    attributes->usedTime = (endTmp.tv_sec - startTmp.tv_sec) + (endTmp.tv_nsec - startTmp.tv_nsec) * 1e-9;
    printf("thread:%d, setFailed:%d, readFailed:%d\n", attributes->threadId, setFailed, readFailed);
    attributes->failed = setFailed + readFailed;
}
void test(){
    double initT = 0, txnT = 0;
    int initFailed = 0, txnFailed = 0;
    pthread_t threadId[MaxThreadNum];
    IndexAttributes attributes[MaxThreadNum];
    taskNum.store(0);
    for (int i = 0; i < threadNum; ++i) {
        attributes[i]={
                .threadId = i,
                .failed = 0
        };
        pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(loadData), (void *) (attributes + i));
    }
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        initT+= attributes[i].usedTime;
        initFailed+= attributes[i].failed;
    }
    taskNum.store(0);
    for (int i = 0; i < threadNum; ++i) {
        attributes[i]={
                .threadId = i,
                .failed = 0
        };
        pthread_create(threadId + i, NULL, reinterpret_cast<void *(*)(void *)>(transaction), (void *) (attributes + i));
    }
    for (int i = 0; i < threadNum; ++i) {
        pthread_join(threadId[i], NULL);
        txnT += attributes[i].usedTime;
        txnFailed+= attributes[i].failed;
    }


    printf("%d, %ld,  %ld, %d, %d, %d, %d, %.2fops, %.2fops, %s\n",
           0, LOAD_SIZE, RUN_SIZE, threadNum, value_len,
           initFailed, txnFailed,
           LOAD_SIZE / perTask * perTask * threadNum / initT ,
           RUN_SIZE / perTask * perTask * threadNum / txnT,
           init_file);
    fflush(stdout);

}


void ycsb_load_run_randint(const char* init_file, const char* txn_file)
{
    std::ifstream infile_load(init_file);

    std::string op;
    uint64_t key;
    //    int range;

    std::string insert("INSERT");
    std::string read("READ");
    std::string scan("SCAN");
    std::string update("UPDATE");

    uint64_t count = 0;
    while ((count < LOAD_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return ;
        }
        init_keys[count] = key;
        count++;
    }

    fprintf(stderr, "Loaded %ld keys\n", count);

    std::ifstream infile_txn(txn_file);

    count = 0;
    while ((count < RUN_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;
        txn_keys[count] = key;
        if (op.compare(insert) == 0) {
            operations[count] = INSERT;
        } else if (op.compare(read) == 0) {
            operations[count] = READ;
        } else if (op.compare(scan) == 0) {
            operations[count] = SCAN;
        } else if (op.compare(update) == 0){
            operations[count] = UPDATE;
        } else{
            std::cout << "UNRECOGNIZED CMD!\n";
            return;
        }
        count++;
    }
    fprintf(stderr, "Loaded %ld txns\n", count);

    std::atomic<int> range_complete, range_incomplete;
    range_complete.store(0);
    range_incomplete.store(0);

}

int main(int argc, char **argv) {
    const char ConfigFile[]= "config.cfg";

    config_t cfg;

    config_init(&cfg);
    cfg.root->type = CONFIG_TYPE_INT64;

    /* Read the file. If there is an error, report it and exit. */
    if(! config_read_file(&cfg, ConfigFile))
    {
        fprintf(stderr, "%s:%d - %s\n", config_error_file(&cfg),
                config_error_line(&cfg), config_error_text(&cfg));
        config_destroy(&cfg);
        return(EXIT_FAILURE);
    }
    config_lookup_int(&cfg, "threadNum", &threadNum);
    config_lookup_int64(&cfg, "LOAD_SIZE", reinterpret_cast<long long int *>(&LOAD_SIZE));
    config_lookup_int64(&cfg, "TXN_SIZE", reinterpret_cast<long long int *>(&RUN_SIZE));
    config_lookup_string(&cfg, "LOAD_FILE", &init_file);
    config_lookup_string(&cfg, "TXN_FILE", &txn_file);

    init_keys = (uint64_t*)malloc(sizeof(uint64_t) * LOAD_SIZE);
    txn_keys = (uint64_t*)malloc(sizeof(uint64_t) * RUN_SIZE);
    operations = (Operation*) malloc(sizeof(Operation) * RUN_SIZE);

    ycsb_load_run_randint(init_file, txn_file);
    printf("LOAD_SIZE:%ld, TXN_SIZE:%ld\n", LOAD_SIZE, RUN_SIZE);

    int value_len = 1000;
    for (int i = 0; i < value_len; ++i) {
        value += "1";
    }

    test();
    return 0;
}
