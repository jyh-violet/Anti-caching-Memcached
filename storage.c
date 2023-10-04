/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "memcached.h"
#ifdef EXTSTORE

#include "storage.h"
#include "extstore.h"
#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <limits.h>
#include <ctype.h>
#include <libpmem.h>
#include <emmintrin.h>  // _mm_clflush _mm_stream_si32 _mm_stream_si64 _mm_stream_si128
#include <immintrin.h>  // _mm_clflushopt _mm512_stream_si512 _mm512_load_epi32
#include <stdatomic.h>
#include <numa.h>

#define PAGE_BUCKET_DEFAULT 0
#define PAGE_BUCKET_COMPACT 1
#define PAGE_BUCKET_CHUNKED 2
#define PAGE_BUCKET_LOWTTL  3
#define PAGE_BUCKET_BASE 4
//#define use_nvm_alloc_pull_flags

/*
 * API functions
 */
bool flushItToExt(void * storage, struct lru_pull_tail_return *it_info, item *hdr_it, obj_io* pre_io);
int storage_write(void *storage, const int clsid,
                  int local_keyLen, int local_slabNum,
                  int local_lru_start, obj_io* pre_io);
// re-cast an io_pending_t into this more descriptive structure.
// the first few items _must_ match the original struct.
typedef struct _io_pending_storage_t {
    int io_queue_type;
    LIBEVENT_THREAD *thread;
    conn *c;
    mc_resp *resp;            /* original struct ends here */
    item *hdr_it;             /* original header item. */
    obj_io io_ctx;            /* embedded extstore IO header */
    unsigned int iovec_data;  /* specific index of data iovec */
    bool noreply;             /* whether the response had noreply set */
    bool miss;                /* signal a miss to unlink hdr_it */
    bool badcrc;              /* signal a crc failure */
    bool active;              /* tells if IO was dispatched or not */
} io_pending_storage_t;
// Only call this if item has ITEM_HDR
bool storage_validate_item(void *e, item *it) {
    item_hdr *hdr = (item_hdr *)ITEM_data(it);
    if (extstore_check(e, hdr->page_id, hdr->page_version) != 0) {
        return false;
    } else {
        return true;
    }
}

void storage_delete(void *e, item *it) {
#ifndef ENABLE_NVM
    if (it->it_flags & ITEM_HDR) {
        item_hdr *hdr = (item_hdr *)ITEM_data(it);
        extstore_delete(e, hdr->page_id, hdr->page_version,
                1, ITEM_ntotal(it));
    }
#endif
#ifdef ENABLE_NVM
    if(it->it_flags & ITEM_NVM){
#ifndef FIX_KEY
        item_unlink_q(it);
        item * nvm_data = (item*)((item_hdr *) ITEM_data(it))->nvmptr;
        slabs_free(nvm_data, ITEM_ntotal(nvm_data), ITEM_clsid(nvm_data) | NVM_SLAB);
#endif
    }
#endif
}

// Function for the extra stats called from a protocol.
// NOTE: This either needs a name change or a wrapper, perhaps?
// it's defined here to reduce exposure of extstore.h to the rest of memcached
// but feels a little off being defined here.
// At very least maybe "process_storage_stats" in line with making this more
// of a generic wrapper module.
void process_extstore_stats(ADD_STAT add_stats, conn *c) {
    int i;
    char key_str[STAT_KEY_LEN];
    char val_str[STAT_VAL_LEN];
    int klen = 0, vlen = 0;
    struct extstore_stats st;

    assert(add_stats);

    void *storage = c->thread->storage;
    if (storage == NULL) {
        return;
    }
    extstore_get_stats(storage, &st);
    st.page_data = calloc(st.page_count, sizeof(struct extstore_page_data));
    extstore_get_page_data(storage, &st);

    for (i = 0; i < st.page_count; i++) {
        APPEND_NUM_STAT(i, "version", "%llu",
                (unsigned long long) st.page_data[i].version);
        APPEND_NUM_STAT(i, "bytes", "%llu",
                (unsigned long long) st.page_data[i].bytes_used);
        APPEND_NUM_STAT(i, "bucket", "%u",
                st.page_data[i].bucket);
        APPEND_NUM_STAT(i, "free_bucket", "%u",
                st.page_data[i].free_bucket);
    }
}

// Additional storage stats for the main stats output.
void storage_stats(ADD_STAT add_stats, conn *c) {
    struct extstore_stats st;
    if (c->thread->storage) {
        STATS_LOCK();
        APPEND_STAT("extstore_compact_lost", "%llu", (unsigned long long)stats.extstore_compact_lost);
        APPEND_STAT("extstore_compact_rescues", "%llu", (unsigned long long)stats.extstore_compact_rescues);
        APPEND_STAT("extstore_compact_skipped", "%llu", (unsigned long long)stats.extstore_compact_skipped);
        STATS_UNLOCK();
        extstore_get_stats(c->thread->storage, &st);
        APPEND_STAT("extstore_page_allocs", "%llu", (unsigned long long)st.page_allocs);
        APPEND_STAT("extstore_page_evictions", "%llu", (unsigned long long)st.page_evictions);
        APPEND_STAT("extstore_page_reclaims", "%llu", (unsigned long long)st.page_reclaims);
        APPEND_STAT("extstore_pages_free", "%llu", (unsigned long long)st.pages_free);
        APPEND_STAT("extstore_pages_used", "%llu", (unsigned long long)st.pages_used);
        APPEND_STAT("extstore_objects_evicted", "%llu", (unsigned long long)st.objects_evicted);
        APPEND_STAT("extstore_objects_read", "%llu", (unsigned long long)st.objects_read);
        APPEND_STAT("extstore_objects_written", "%llu", (unsigned long long)st.objects_written);
        APPEND_STAT("extstore_objects_used", "%llu", (unsigned long long)st.objects_used);
        APPEND_STAT("extstore_bytes_evicted", "%llu", (unsigned long long)st.bytes_evicted);
        APPEND_STAT("extstore_bytes_written", "%llu", (unsigned long long)st.bytes_written);
        APPEND_STAT("extstore_bytes_read", "%llu", (unsigned long long)st.bytes_read);
        APPEND_STAT("extstore_bytes_used", "%llu", (unsigned long long)st.bytes_used);
        APPEND_STAT("extstore_bytes_fragmented", "%llu", (unsigned long long)st.bytes_fragmented);
        APPEND_STAT("extstore_limit_maxbytes", "%llu", (unsigned long long)(st.page_count * st.page_size));
        APPEND_STAT("extstore_io_queue", "%llu", (unsigned long long)(st.io_queue));
    }

}


#ifdef ENABLE_NVM
extern  int toDRAMswap;
extern  int toNVMswap;
extern  int flushToNVM;
extern  int flushToSSD;
extern  int NVMtoDRAMswap;
__thread int counter = 0;
#define LOG_INTERLVAL 100000
extern __thread int lru_counter;
extern __thread int lru_index;
extern __thread int nvm_lru_counter;

extern int slabNum;
extern __thread int slab_index;
extern __thread int thread_type;
extern __thread int slab_counter_index;

extern bool need_log;
__thread int64_t asso_delet = 0;
extern __thread int per_thread_counter;
extern __thread int slab_span;
extern __thread int slab_min;
extern int alloc_pull_flags[MAX_THREAD];
extern int nvm_alloc_pull_flags[MAX_THREAD];

volatile int64_t IsWriting = false;
bool _swap_item(char* buf, int pageVersion, int pageId, int offset,
                int local_nvm_slab_index, int local_nvm_adder, int local_nvm_per_thread_counter){
    need_log = true;
    bool swaped = true;
    item* it = (item*)(buf);
    int classId;
#ifdef tiny
    uint32_t hv = it->time_persist;
#else
    uint32_t hv = it->time;
#endif
    int local_slabNum = slabNum;
    char *key= ITEM_key(it);
    item *old_it =it->h_next;
    item_hdr *hdr = (item_hdr*) ITEM_data(old_it);
    size_t orig_ntotal = ITEM_ntotal(it);
    if( (strncmp(ITEM_key(it), ITEM_key(old_it), it->nkey) == 0)
    && (old_it->it_flags & ITEM_HDR)
    && (old_it->it_flags & ITEM_LINKED)
    && (hdr->page_id == pageId)
    &&(hdr->page_version == pageVersion)
    && (hdr->offset == (offset))
    && (IsWarmForNVM(old_it)  || (old_it->it_flags & ITEM_NEED_SWAP))){
        item_lock(hv);
        if((old_it->it_flags & ITEM_HDR)
        && (old_it->it_flags & ITEM_LINKED)
        && (strncmp(ITEM_key(it), ITEM_key(old_it), it->nkey) == 0)
        && (old_it->it_flags & ITEM_LINKED)
        && (hdr->page_id == pageId)
        &&(hdr->page_version == pageVersion)
        && (hdr->offset == (offset)) ){
#if defined(TUPLE_SWAP) || defined(NVM_AS_DRAM) || defined(BLOCK_SWAP) || defined(DRAM_CACHE)
            bool inDRAM = true;
#else
#ifdef  MEM_MOD
            bool inDRAM = false;
#else
            bool inDRAM = IsHotForDRAM(old_it);
#endif
#endif
            refcount_incr(old_it);
            if(inDRAM){
                item* new_it = do_item_alloc(key, it->nkey, 0, old_it->exptime, it->nbytes);
                if(new_it != NULL){
                    memcpy((char *)new_it+READ_OFFSET,
                           (char *)it+READ_OFFSET, orig_ntotal-READ_OFFSET);
#ifdef DRAM_CACHE
                    hdr->cache_item = new_it;
#else
                    new_it->it_flags = old_it->it_flags;
                    new_it->it_flags &=(~ITEM_HDR) & (~ITEM_NVM);
                    item_replace(old_it, new_it, hv);
                    ITEM_set_cas(new_it, ITEM_get_cas(old_it));
                    do_item_remove(new_it);
#endif
#ifdef M_STAT
                    __sync_fetch_and_add(&toDRAMswap, 1);
#endif
                }  else{
#if defined(NVM_AS_DRAM) || defined(DRAM_CACHE)
                    swaped = false;
#else
                    goto toNVM;
#endif
                }

            }else{
#if (!defined(NVM_AS_DRAM)) && (!defined(DRAM_CACHE))
                toNVM:
#endif
                classId = slabs_clsid(orig_ntotal);
#ifdef FIX_KEY
                item *hdr_it = NULL;
                for (int i = 0; i < local_nvm_per_thread_counter && hdr_it == NULL; ++i) {
                    hdr_it = slabs_alloc_c(orig_ntotal, classId | NVM_SLAB, 0,
                                           (local_nvm_slab_index + (local_nvm_adder + i) % local_nvm_per_thread_counter ) % local_slabNum, local_slabNum);
                }
                /* Run the storage write understanding the start of the item is dirty.
                 * We will fill it (time/exptime/etc) from the header item on read.
                 */
                if(hdr_it != NULL){
                    hdr_it->nkey = it->nkey;
                    // for NVM and ext items, key is not in the same place with data, and need copy respectively !!!!
                    memcpy(ITEM_key(hdr_it),ITEM_key(it), it->nkey);
                    item_hdr *new_hdr = (item_hdr *) ITEM_data(hdr_it);
                    hdr_it->nbytes = it->nbytes;
                    char* nvm_data = new_hdr->nvmptr;
#ifdef MEM_MOD
                    item* nvm_item = (item*)new_hdr->nvmptr;

                    item* cache_it = NULL;
                    if(IsHotForDRAM(old_it)){
                        __sync_fetch_and_add(&toDRAMswap, 1);
                        cache_it = item_alloc(key, old_it->nkey, 0, 0, old_it->nbytes);
                    }

                    if(cache_it != NULL){
                        new_hdr->cache_item = cache_it;
                        cache_it->nkey = old_it->nkey;
                        memcpy(ITEM_key(cache_it), ITEM_key(it), it->nkey);
                        memcpy(ITEM_data(cache_it), (char *)it+READ_OFFSET, orig_ntotal-READ_OFFSET);
                        item_link_q(cache_it);
                    }else{
                        new_hdr->cache_item = NULL;
                        memcpy(ITEM_data(nvm_item), (char *)it+READ_OFFSET, orig_ntotal-READ_OFFSET);
                    }
#else

                    memcpy((char *)nvm_data,
                           (char *)it+READ_OFFSET, orig_ntotal-READ_OFFSET);
#endif
                    hdr_it->it_flags |= ITEM_NVM;
#ifdef LRU_COUNTER
                    int local_nvm_lru_counter  = (local_nvm_slab_index + local_nvm_adder) % local_slabNum;
#ifdef use_nvm_alloc_pull_flags
                    if(nvm_alloc_pull_flags[local_nvm_lru_counter] == 0){
                        local_nvm_lru_counter += local_slabNum;
                    }
#endif
                    hdr_it->slabs_clsid |= (local_nvm_lru_counter * POWER_LARGEST);
#endif
                    // overload nbytes for the header it
                    hdr_it->nbytes = it->nbytes;
                    /* success! Now we need to fill relevant data into the new
                         * header and replace. Most of this requires the item lock
                         */

                    if(false){
                        fprintf(stderr, "(%ld), _swap_item slabs_alloc old_it:%p it:%p,ref:%d, flag:%d, slab_id:%d, nvmptr:%p hv:%d, new hv:%d, old flag:%d error\n",
                                pthread_self(), (void *)old_it,  (void *)hdr_it,hdr_it->refcount, hdr_it->it_flags,
                                hdr_it->slabs_clsid,  (void *)nvm_data,  hv, hash(ITEM_key(hdr_it), it->nkey), it->it_flags);
                        exit(0);
                    }
                    item_replace(old_it, hdr_it, hv);
                    if(false){
                        fprintf(stderr, "(%ld), _swap_item it:%p,ref:%d, flag:%d, hv:%d error\n",
                                pthread_self(), (void *)hdr_it,  hdr_it->refcount, hdr_it->it_flags, hv);
                        exit(0);
                    }
                    do_item_remove(hdr_it);
#ifdef M_STAT
                    __sync_fetch_and_add(&toNVMswap, 1);
#endif
                } else{
                    fprintf(stderr, "(%ld), _swap_item slabs_alloc failed ref:%d, nvm_slab:%d, old flag:%d, local_nvm_slab_index:%d, local_nvm_adder:%d, local_slabNum:%d\n",
                            pthread_self(),old_it->refcount,  classId, it->it_flags, local_nvm_slab_index, local_nvm_adder, local_slabNum);
                    swaped = false;
                }
#else
                char* nvm_data =  NULL;
#ifdef SLAB_PART
                for (int i = 0; i < slabNum; i++) {
                    slab_counter = (slab_index + i) % slabNum;
#else
                    for (int i = 0; i < 100; i++) {
#endif
                        nvm_data =  slabs_alloc(orig_ntotal, classId | NVM_SLAB, 0);
#ifndef  NO_FLUSH_IN_ALLOC
                        if(nvm_data == NULL){
                            struct lru_pull_tail_return new_it_info = {NULL, 0};
                            //TODO:  id may need change
                            for (int j = 0; j < MAX_THREAD; ++j) {
#ifdef LRU_COUNTER
                                lru_counter  = (slab_index + j) % MAX_THREAD;
#endif
                                lru_pull_tail(classId, COLD_LRU, 0, LRU_PULL_RETURN_ITEM | LRU_NVM, 0, &new_it_info);
                                if(new_it_info.it != NULL){
                                    if(!flushItToExt(ext_storage, &new_it_info)){
                                        //                            fprintf(stderr, "_swap_item flushItToNvm i :%d\n", i);
                                    }
                                }
                            }
                        } else{
                            break;
                        }
#else
if(nvm_data != NULL){
                            break;
                        }
#endif
if(i % 18 == 17){
                            counter ++;
                            if(counter % LOG_INTERLVAL == 0){
                                fprintf(stderr, "_swap_item: nvm_slabs_alloc:%d, slab_index:%d,slab_counter:%d\n",
                                        i, slab_index, slab_counter);
                                print_slab(classId | NVM_SLAB);
                            }

                        }
                    }

                    if ( nvm_data != NULL) {
                        //            fprintf(stderr, "storage_write key:%.10s\n", ITEM_key(it));

                        old_it->it_flags |= ITEM_NVM;
                        old_it->it_flags &= ~(ITEM_HDR);
                        //            item *buf_it = (item *) ((char*)nvm->data[nvm->block_no] + nvm->offset);
                        //            buf_it->time = it_info.hv;
                        memcpy((char *)nvm_data+READ_OFFSET,
                               (char *)it+READ_OFFSET, orig_ntotal-READ_OFFSET);
                        item_hdr *hdr = (item_hdr *) ITEM_data(old_it);
                        hdr->nvmptr = nvm_data;
                        hdr->nvm_slab_id = classId;
#ifdef LRU_COUNTER
                        nvm_lru_counter  = (nvm_lru_counter + 1) % MAX_THREAD;
                        hdr->nvm_slab_id |= (nvm_lru_counter % slabNum * POWER_LARGEST);
#endif
                        // overload nbytes for the header it
                        old_it->nbytes = it->nbytes;
                        old_it->it_flags |= ITEM_LINKED;
                        item_link_q(old_it);
                        /* success! Now we need to fill relevant data into the new
                             * header and replace. Most of this requires the item lock
                             */
                        //                fprintf(stderr, "replace\n");
#ifdef M_STAT
__sync_fetch_and_add(&toNVMswap, 1);
#endif
                    } else{
                        swaped = false;
                        //                fprintf(stderr, "key:%.30s alloc nvm error\n", ITEM_key(it));
                    }
#endif

            }
            do_item_remove_with_unlock(old_it, hv);
        } else{
            item_unlock(hv);
//            fprintf(stderr, "swap_item no match!\n");
        }
    }
    return swaped;
}

bool _storage_Swap_wbuf(char* buf, size_t size, int pageVersion, int pageId, int offset, int local_nvm_slab_index, int local_nvm_per_thread_counter){
    bool swapped = true;
    int local_adder = 0;
    for (size_t i = 0; i <size;) {
        item* it = (item*)(buf + i);
        size_t orig_ntotal = ITEM_ntotal(it);
//        fprintf(stderr, "pageId:%d, off:%d, orig_ntotal:%ld\n", pageId, offset,  orig_ntotal);

        if(i + orig_ntotal <= size){
            uint32_t crc2 = crc32c(0,(char *)it+READ_OFFSET, orig_ntotal-READ_OFFSET);
//            fprintf(stderr, "_swap_item, it:%p, key:%.30s page:%d, off:%d, crc:%d, len:%ld, old_crc:%d\n",
//                    (void *)it, ITEM_key(it), pageId, offset, crc2, orig_ntotal, it->exptime);
            if(it->crc != crc2){
                break;
            }
        } else{
            break;
        }

        local_adder ++;
        if( _swap_item((char *)it, pageVersion, pageId, offset + i, local_nvm_slab_index,
                       local_adder % local_nvm_per_thread_counter, local_nvm_per_thread_counter) == false){
            swapped = false;
        }
        i +=orig_ntotal;
    }
    return swapped;
}

extern int keyLen;

void* _swap_thread(void *arg) {
    swap_thread *me = (swap_thread *)arg;
    slab_span = slabNum;
    slab_min = 0;
    per_thread_counter = slab_span/ settings.swap_threadcount;
    int left = slab_span % settings.swap_threadcount;
    slab_index = me->threadId * per_thread_counter + slab_min;
    if(me->threadId < left){
        per_thread_counter += 1;
        slab_index += me->threadId;
    } else{
        slab_index += left;
    }
    fprintf(stderr, "_swap_thread(%ld), slab_index:%d, per_thread_counter:%d, keyLen:%d\n",
            pthread_self(), slab_index, per_thread_counter, keyLen);

    int local_nvm_slab_index = me->threadId * (slabNum/ settings.swap_threadcount);
    int local_nvm_per_thread_counter = slabNum/ settings.swap_threadcount;
    if(me->threadId == (settings.swap_threadcount - 1)){
        local_nvm_per_thread_counter = slabNum - local_nvm_slab_index;
    }
    thread_type = SWAP_THREAD;
    me->stop = 0;
    if(per_thread_counter == 0){
        me->stop = 1;
        return NULL;
    }
    unsigned int adder = 0;
    while (1) {
        swap_buf *swap_stack = NULL;
        pthread_mutex_lock(&me->mutex);
        if (me->queue == NULL) {
            pthread_cond_wait(&me->cond, &me->mutex);
        }

        // Pull and disconnect a batch from the queue
        // Chew small batches from the queue so the IO thread picker can keep
        // the IO queue depth even, instead of piling on threads one at a time
        // as they gobble a queue.
        if (me->queue != NULL) {
            int i;
            swap_buf* end = NULL;
            swap_stack = me->queue;
            end = swap_stack;
            for (i = 0; i < 10; ++i) {
                if (end->next) {
                    end = end->next;
                } else {
                    me->queue_tail = end->next;
                    break;
                }
            }
            me->depth -= i;
            me->queue = end->next;
            end->next = NULL;
        }
        pthread_mutex_unlock(&me->mutex);
#if defined(TUPLE_SWAP) || defined(NVM_AS_DRAM) || defined(BLOCK_SWAP) || defined(DRAM_CACHE) || defined(MEM_MOD)
#else
        while (IsWriting > 0) {
            usleep(10000);
        }
#endif
        item* it;
        swap_buf *swapBuf = swap_stack;
        size_t orig_ntotal;
        uint32_t crc2;
        while (swapBuf) {
            // We need to note next before the callback in case the obj_io
            // gets reused.
            swap_buf *next = swapBuf->next;
            switch (swapBuf->mode) {
                case SWAP_ITME:
                    it = (item*)(swapBuf->buf);
                    orig_ntotal = ITEM_ntotal(it);
                    crc2 = crc32c(0, (char *)it+READ_OFFSET, orig_ntotal-READ_OFFSET);
                    if(it->crc == crc2){
                        adder ++;
                        _swap_item(swapBuf->buf, swapBuf->page_version, swapBuf->page_id, swapBuf->offset,
                                   local_nvm_slab_index,adder % local_nvm_per_thread_counter, local_nvm_per_thread_counter);
                    } else{
                        fprintf(stderr, "_swap_item buf:%p crc error,orig_ntotal:%ld, it->exptime:%d, crc2:%d io->offset:%d, page:%d, len:%d\n",
                                (void *)swapBuf->buf, orig_ntotal, it->exptime, crc2, swapBuf->offset, swapBuf->page_id, swapBuf->len);
                    }
                    break;
                case SWAP_WBUF:
                    if(_storage_Swap_wbuf(swapBuf->buf, swapBuf->len,
                                          swapBuf->page_version, swapBuf->page_id, swapBuf->offset,local_nvm_slab_index, local_nvm_per_thread_counter) == false){
                        resetPageWbufWaitSwap(me->engine, swapBuf->page_id,swapBuf->offset);
                    }
//                    fprintf(stderr, "swap buf, page:%d, wbuf_index:%d\n", swapBuf->page_id, swapBuf->offset / swapBuf->len);
                    resetWbuf(me->engine, swapBuf->page_id, swapBuf->offset);
                    break;
                case SWAP_WBUF_NEEDREAD:
                    if(swapBuf->buf == NULL){
                        swapBuf->buf = get_io_wubf_static(me->engine);
                    }
                    readWbuf(me->engine, swapBuf->page_id, swapBuf, swapBuf->len, swapBuf->offset);
                    if(_storage_Swap_wbuf(swapBuf->buf, swapBuf->len,
                                          swapBuf->page_version, swapBuf->page_id, swapBuf->offset, local_nvm_slab_index, local_nvm_per_thread_counter) == false){
                        resetPageWbufWaitSwap(me->engine, swapBuf->page_id,swapBuf->offset);
                    }
                    resetWbuf(me->engine, swapBuf->page_id, swapBuf->offset);
                    break;
                case SWAP_ITEM_NEEDREAD:
                    if(swapBuf->buf == NULL){
                        swapBuf->buf = get_io_wubf_static(me->engine);
                    }
                    readWbuf(me->engine, swapBuf->page_id, swapBuf, swapBuf->len, swapBuf->offset);
                    it = (item*)(swapBuf->buf);
                    orig_ntotal = ITEM_ntotal(it);
                    crc2 = crc32c(0, (char *)it+READ_OFFSET, orig_ntotal-READ_OFFSET);
                    if(it->crc == crc2){
                        adder ++;
                        _swap_item(swapBuf->buf, swapBuf->page_version, swapBuf->page_id, swapBuf->offset,
                                   local_nvm_slab_index,adder % local_nvm_per_thread_counter, local_nvm_per_thread_counter);
                    } else{
                        fprintf(stderr, "_swap_item buf:%p crc error,orig_ntotal:%ld, it->exptime:%d, crc2:%d io->offset:%d, page:%d, len:%d\n",
                                (void *)swapBuf->buf, orig_ntotal, it->exptime, crc2, swapBuf->offset, swapBuf->page_id, swapBuf->len);
                    }
                    break;

            }
            pthread_mutex_lock(&me->mutex);
            swapBuf->next = me->swapBufStack;
            me->swapBufStack = swapBuf;
//            fprintf(stderr, "swapthread:%p, return swap buf:%p, next:%p\n",
//                    (void *)me, (void *)swapBuf->buf, (void *)(swapBuf->next?swapBuf->next->buf:NULL));
            pthread_mutex_unlock(&me->mutex);
            swapBuf = next;
        }
    }
    me->stop = 1;
    return NULL;
}

#endif

// FIXME: This runs in the IO thread. to get better IO performance this should
// simply mark the io wrapper with the return value and decrement wrapleft, if
// zero redispatching. Still a bit of work being done in the side thread but
// minimized at least.
// TODO: wrap -> p?
#ifndef ENABLE_NVM
static void _storage_get_item_cb(void *e, obj_io *io, int ret) {
#ifdef YCSB
//    io_pending_storage_t *p = (io_pending_storage_t*) io->data;
//    p->active = false;
//    fprintf(stderr, "_storage_get_item_cb, return io:%p\n", (void *)io);
#else

    fprintf(stderr, "_storage_get_item_cb, io:%p\n", (void *) io);
    // FIXME: assumes success
    io_pending_storage_t *p = (io_pending_storage_t *)io->data;
    mc_resp *resp = p->resp;
    conn *c = p->c;
    assert(p->active == true);
    item *read_it = (item *)io->buf;

    if(read_it ==NULL){
        fprintf(stderr, "_storage_get_item_cb, io:%p, null\n", (void *) io);
        resp->skip = true;
        return;
    }


    bool miss = false;

    // TODO: How to do counters for hit/misses?
    if (ret < 1) {
        miss = true;
    } else {
        uint32_t crc2;
        uint32_t crc = (uint32_t) read_it->exptime;
        int x;
        // item is chunked, crc the iov's
        if (io->iov != NULL) {
            // first iov is the header, which we don't use beyond crc
            crc2 = crc32c(0, (char *)io->iov[0].iov_base+STORE_OFFSET, io->iov[0].iov_len-STORE_OFFSET);
            // make sure it's not sent. hack :(
            io->iov[0].iov_len = 0;
            for (x = 1; x < io->iovcnt; x++) {
                crc2 = crc32c(crc2, (char *)io->iov[x].iov_base, io->iov[x].iov_len);
            }
        } else {
            crc2 = crc32c(0, (char *)read_it+STORE_OFFSET, io->len-STORE_OFFSET);
        }

        if (crc != crc2) {
            miss = true;
            p->badcrc = true;
        }
    }

    if (miss) {
        if (p->noreply) {
            // In all GET cases, noreply means we send nothing back.
            resp->skip = true;
        } else {
            // TODO: This should be movable to the worker thread.
            // Convert the binprot response into a miss response.
            // The header requires knowing a bunch of stateful crap, so rather
            // than simply writing out a "new" miss response we mangle what's
            // already there.
            if (c->protocol == binary_prot) {
                protocol_binary_response_header *header =
                    (protocol_binary_response_header *)resp->wbuf;

                // cut the extra nbytes off of the body_len
                uint32_t body_len = ntohl(header->response.bodylen);
                uint8_t hdr_len = header->response.extlen;
                body_len -= resp->iov[p->iovec_data].iov_len + hdr_len;
                resp->tosend -= resp->iov[p->iovec_data].iov_len + hdr_len;
                header->response.extlen = 0;
                header->response.status = (uint16_t)htons(PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
                header->response.bodylen = htonl(body_len);

                // truncate the data response.
                resp->iov[p->iovec_data].iov_len = 0;
                // wipe the extlen iov... wish it was just a flat buffer.
                resp->iov[p->iovec_data-1].iov_len = 0;
                resp->chunked_data_iov = 0;
            } else {
                int i;
                // Meta commands have EN status lines for miss, rather than
                // END as a trailer as per normal ascii.
                if (resp->iov[0].iov_len >= 3
                        && memcmp(resp->iov[0].iov_base, "VA ", 3) == 0) {
                    // TODO: These miss translators should use specific callback
                    // functions attached to the io wrap. This is weird :(
                    resp->iovcnt = 1;
                    resp->iov[0].iov_len = 4;
                    resp->iov[0].iov_base = "EN\r\n";
                    resp->tosend = 4;
                } else {
                    // Wipe the iovecs up through our data injection.
                    // Allows trailers to be returned (END)
                    for (i = 0; i <= p->iovec_data; i++) {
                        resp->tosend -= resp->iov[i].iov_len;
                        resp->iov[i].iov_len = 0;
                        resp->iov[i].iov_base = NULL;
                    }
                }
                resp->chunked_total = 0;
                resp->chunked_data_iov = 0;
            }
        }
        p->miss = true;
    } else {
        assert(read_it->slabs_clsid != 0);
        // TODO: should always use it instead of ITEM_data to kill more
        // chunked special casing.
        if ((read_it->it_flags & ITEM_CHUNKED) == 0) {
            resp->iov[p->iovec_data].iov_base = ITEM_data(read_it);
        }
        p->miss = false;
    }

    p->active = false;
    //assert(c->io_wrapleft >= 0);


    // All IO's have returned, lets re-attach this connection to our original
    // thread.
    io_queue_t *q = conn_io_queue_get(p->c, p->io_queue_type);
    q->count--;
    if (q->count == 0) {
        redispatch_conn(c);
    }
#endif
}
#endif

#ifdef YCSB
int storage_get_item(conn *c, item *it, mc_resp *resp) {
#ifdef NEED_ALIGN
    item_hdr hdr;
    memcpy(&hdr, ITEM_data(it), sizeof(hdr));
#else
    item_hdr *hdr = (item_hdr *)ITEM_data(it);
#endif
    size_t ntotal = ITEM_ntotal(it);

    obj_io *eio = (obj_io*)resp->io_ctx;


    int iovtotal = (c->protocol == binary_prot) ? it->nbytes - 2 : it->nbytes;
    resp_add_iov(resp, "", iovtotal);


    // We need to stack the sub-struct IO's together for submission.
    eio->next = NULL;

    // reference ourselves for the callback.
    // Now, fill in io->io based on what was in our header.
#ifdef NEED_ALIGN
    eio->page_version = hdr.page_version;
    eio->page_id = hdr.page_id;
    eio->offset = hdr.offset;
#else
    eio->page_version = hdr->page_version;
    eio->page_id = hdr->page_id;
    eio->offset = hdr->offset;
#endif
    eio->len = ntotal;
    eio->mode = OBJ_IO_READ;

    bool no_swap=true;
//    int oldref = it->refcount;
    if(IsWarm(it)){
        no_swap = false;
    } else if(false){
        fprintf(stderr, "storage_get_item, it:%p, time:%d, current_time:%d readcount:%d\n",
                (void *)it, it->time, current_time, it->read_count);
    }
#ifdef TUPLE_SWAP
    if(it->it_flags & ITEM_NEED_SWAP){
        item_remove(it);
        no_swap = true;
        extstore_submit_opt(c->thread->storage, eio,
                            no_swap);
    }else{
        no_swap=false;

        if(extstore_submit_opt(c->thread->storage, eio,
                               no_swap) == 2){
            item_remove_setNeedSwap(it);
        } else{
            item_remove(it);
        }
    }
#else
    if(no_swap){
        item_remove(it);
    } else{
        item_remove_setNeedSwap(it);
    }
    extstore_submit_opt(c->thread->storage, eio,
                        no_swap);
#endif

    return 0;
}

int nvm_aio_get_item(conn *c, char* src, int len) {
    extstore_submit_nvm(c->thread->storage, src,len);
    return 1;
}

#else

    int storage_get_item(conn *c, item *it, mc_resp *resp) {
#ifdef NEED_ALIGN
        item_hdr hdr;
        memcpy(&hdr, ITEM_data(it), sizeof(hdr));
#else
        item_hdr *hdr = (item_hdr *)ITEM_data(it);
#endif
        io_queue_t *q = conn_io_queue_get(c, IO_QUEUE_EXTSTORE);
        size_t ntotal = ITEM_ntotal(it);
        unsigned int clsid = slabs_clsid(ntotal);
        item *new_it;
        bool chunked = false;
        if (ntotal > settings.slab_chunk_size_max) {
            // Pull a chunked item header.
            uint32_t flags;
            FLAGS_CONV(it, flags);
            new_it = item_alloc(ITEM_key(it), it->nkey, flags, it->exptime, it->nbytes);
            assert(new_it == NULL || (new_it->it_flags & ITEM_CHUNKED));
            chunked = true;
        } else {
            new_it = do_item_alloc_pull(ntotal, clsid);
        }
        if (new_it == NULL)
            return -1;
        // so we can free the chunk on a miss
        new_it->slabs_clsid = clsid;

        io_pending_storage_t *p = do_cache_alloc(c->thread->io_cache);
        // this is a re-cast structure, so assert that we never outsize it.
        assert(sizeof(io_pending_t) >= sizeof(io_pending_storage_t));
        memset(p, 0, sizeof(io_pending_storage_t));
        p->active = true;
        p->miss = false;
        p->badcrc = false;
        p->noreply = c->noreply;
        // io_pending owns the reference for this object now.
        p->hdr_it = it;
        p->resp = resp;
        p->io_queue_type = IO_QUEUE_EXTSTORE;
        obj_io *eio = &p->io_ctx;

        // FIXME: error handling.
        if (chunked) {
            unsigned int ciovcnt = 0;
            size_t remain = new_it->nbytes;
            item_chunk *chunk = (item_chunk *) ITEM_schunk(new_it);
            // TODO: This might make sense as a _global_ cache vs a per-thread.
            // but we still can't load objects requiring > IOV_MAX iovs.
            // In the meantime, these objects are rare/slow enough that
            // malloc/freeing a statically sized object won't cause us much pain.
            eio->iov = malloc(sizeof(struct iovec) * IOV_MAX);
            if (eio->iov == NULL) {
                item_remove(new_it);
                do_cache_free(c->thread->io_cache, p);
                return -1;
            }

            // fill the header so we can get the full data + crc back.
            eio->iov[0].iov_base = new_it;
            eio->iov[0].iov_len = ITEM_ntotal(new_it) - new_it->nbytes;
            ciovcnt++;

            while (remain > 0) {
                chunk = do_item_alloc_chunk(chunk, remain);
                // FIXME: _pure evil_, silently erroring if item is too large.
                if (chunk == NULL || ciovcnt > IOV_MAX-1) {
                    item_remove(new_it);
                    free(eio->iov);
                    // TODO: wrapper function for freeing up an io wrap?
                    eio->iov = NULL;
                    do_cache_free(c->thread->io_cache, p);
                    return -1;
                }
                eio->iov[ciovcnt].iov_base = chunk->data;
                eio->iov[ciovcnt].iov_len = (remain < chunk->size) ? remain : chunk->size;
                chunk->used = (remain < chunk->size) ? remain : chunk->size;
                remain -= chunk->size;
                ciovcnt++;
            }

            eio->iovcnt = ciovcnt;
        }

        // Chunked or non chunked we reserve a response iov here.
        p->iovec_data = resp->iovcnt;
        int iovtotal = (c->protocol == binary_prot) ? it->nbytes - 2 : it->nbytes;
        if (chunked) {
            resp_add_chunked_iov(resp, new_it, iovtotal);
        } else {
            resp_add_iov(resp, "", iovtotal);
        }

        // We can't bail out anymore, so mc_resp owns the IO from here.
        resp->io_pending = (io_pending_t *)p;

        eio->buf = (void *)new_it;
        p->c = c;

        // We need to stack the sub-struct IO's together for submission.
        eio->next = q->stack_ctx;
        q->stack_ctx = eio;

        // No need to stack the io_pending's together as they live on mc_resp's.
        assert(q->count >= 0);
        q->count++;
        // reference ourselves for the callback.
        eio->data = (void *)p;

        // Now, fill in io->io based on what was in our header.
#ifdef NEED_ALIGN
eio->page_version = hdr.page_version;
eio->page_id = hdr.page_id;
eio->offset = hdr.offset;
#else
eio->page_version = hdr->page_version;
eio->page_id = hdr->page_id;
eio->offset = hdr->offset;
#endif
eio->len = ntotal;
eio->mode = OBJ_IO_READ;
eio->cb = _storage_get_item_cb;

// FIXME: This stat needs to move to reflect # of flash hits vs misses
// for now it's a good gauge on how often we request out to flash at
// least.
pthread_mutex_lock(&c->thread->stats.mutex);
c->thread->stats.get_extstore++;
pthread_mutex_unlock(&c->thread->stats.mutex);

return 0;
    }
#endif

void storage_submit_cb(io_queue_t *q) {
    // Don't need to do anything special for extstore.
    extstore_submit(q->ctx, q->stack_ctx);
//    fprintf(stderr, "extstore_submit\n");
}

static void recache_or_free(io_pending_t *pending) {
//    fprintf(stderr, "recache_or_free\n");
    // re-cast to our specific struct.
    io_pending_storage_t *p = (io_pending_storage_t *)pending;

    conn *c = p->c;
    obj_io *io = &p->io_ctx;
    assert(io != NULL);
    item *it = (item *)io->buf;
    assert(c != NULL);
    bool do_free = true;
    if (p->active) {
        // If request never dispatched, free the read buffer but leave the
        // item header alone.
        do_free = false;
#ifndef YCSB
        size_t ntotal = ITEM_ntotal(p->hdr_it);
        slabs_free(it, ntotal, slabs_clsid(ntotal));
#endif
        io_queue_t *q = conn_io_queue_get(c, p->io_queue_type);
        q->count--;
        assert(q->count >= 0);
#ifndef ENABLE_NVM
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.get_aborted_extstore++;
        pthread_mutex_unlock(&c->thread->stats.mutex);
#endif
    } else if (p->miss) {
        // If request was ultimately a miss, unlink the header.
        do_free = false;
        size_t ntotal = ITEM_ntotal(p->hdr_it);
        item_unlink(p->hdr_it);
        slabs_free(it, ntotal, slabs_clsid(ntotal));
#ifndef ENABLE_NVM
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.miss_from_extstore++;
        if (p->badcrc)
            c->thread->stats.badcrc_from_extstore++;
        pthread_mutex_unlock(&c->thread->stats.mutex);
#endif
    } else if (settings.ext_recache_rate) {
        fprintf(stderr, "io_pending_storage_t:%p, active=false\n", (void *)p);
        // hashvalue is cuddled during store
#ifndef YCSB
        uint32_t hv = (uint32_t)it->time;
        // opt to throw away rather than wait on a lock.
        void *hold_lock = item_trylock(hv);
        if (hold_lock != NULL) {
            item *h_it = p->hdr_it;
            uint8_t flags = ITEM_LINKED|ITEM_FETCHED|ITEM_ACTIVE;
            // Item must be recently hit at least twice to recache.
            if (((h_it->it_flags & flags) == flags) &&
                    h_it->time > current_time - ITEM_UPDATE_INTERVAL &&
                    c->recache_counter++ % settings.ext_recache_rate == 0) {
                do_free = false;
                // In case it's been updated.
                it->exptime = h_it->exptime;
                it->it_flags &= ~ITEM_LINKED;
                it->refcount = 0;
                it->h_next = NULL; // might not be necessary.
                STORAGE_delete(c->thread->storage, h_it);
                item_replace(h_it, it, hv);
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.recache_from_extstore++;
                pthread_mutex_unlock(&c->thread->stats.mutex);
            }
        }
        if (hold_lock)
            item_trylock_unlock(hold_lock);
#endif
    }
    if (do_free){
#ifndef YCSB
        slabs_free(it, ITEM_ntotal(it), ITEM_clsid(it));
        fprintf(stderr, "recache_or_free free item:%p\n", (void *) it);
#endif
    }


    p->io_ctx.buf = NULL;
    p->io_ctx.next = NULL;
#ifndef YCSB
    p->active = false;
    // TODO: reuse lock and/or hv.
    item_remove(p->hdr_it);
#endif

}

// Called after the IO is processed but before the response is transmitted.
// TODO: stubbed with a reminder: should be able to move most of the extstore
// callback code into this code instead, executing on worker thread instead of
// IO thread.
void storage_complete_cb(io_queue_t *q) {
    // need to reset the stack for next use.
//    fprintf(stderr, "storage_complete_cb q:%p\n", (void *)q);
    q->stack_ctx = NULL;
    return;
}

// Called after responses have been transmitted. Need to free up related data.
void storage_finalize_cb(io_pending_t *pending) {
    recache_or_free(pending);
    io_pending_storage_t *p = (io_pending_storage_t *)pending;
    obj_io *io = &p->io_ctx;
    // malloc'ed iovec list used for chunked extstore fetches.
    if (io->iov) {
        free(io->iov);
        io->iov = NULL;
    }
    // don't need to free the main context, since it's embedded.
}

/*
 * WRITE FLUSH THREAD
 */
#ifndef ENABLE_NVM

static int storage_write(void *storage, const int clsid, const int item_age) {
    int did_moves = 0;
    struct lru_pull_tail_return it_info;

    it_info.it = NULL;
    lru_pull_tail(clsid, COLD_LRU, 0, LRU_PULL_RETURN_ITEM, 0, &it_info);
    /* Item is locked, and we have a reference to it. */
    if (it_info.it == NULL) {
        return did_moves;
    }

    obj_io io;
    item *it = it_info.it;
    /* First, storage for the header object */
    size_t orig_ntotal = ITEM_ntotal(it);
    uint32_t flags;
    if ((it->it_flags & ITEM_HDR) == 0 &&
            (item_age == 0 || current_time - it->time > item_age)) {
        FLAGS_CONV(it, flags);
        item *hdr_it = do_item_alloc(ITEM_key(it), it->nkey, flags, it->exptime, sizeof(item_hdr));
        /* Run the storage write understanding the start of the item is dirty.
         * We will fill it (time/exptime/etc) from the header item on read.
         */
        if (hdr_it != NULL) {
            fprintf(stderr, "storage_write key:%.10s\n", ITEM_key(it));
            int bucket = (it->it_flags & ITEM_CHUNKED) ?
                PAGE_BUCKET_CHUNKED : PAGE_BUCKET_DEFAULT;
            // Compress soon to expire items into similar pages.
            if (it->exptime - current_time < settings.ext_low_ttl) {
                bucket = PAGE_BUCKET_LOWTTL;
            }
            hdr_it->it_flags |= ITEM_HDR;
            io.len = orig_ntotal;
            io.mode = OBJ_IO_WRITE;
            // NOTE: when the item is read back in, the slab mover
            // may see it. Important to have refcount>=2 or ~ITEM_LINKED
            assert(it->refcount >= 2);
            // NOTE: write bucket vs free page bucket will disambiguate once
            // lowttl feature is better understood.
            if (extstore_write_request(storage, bucket, bucket, &io) == 0) {
                fprintf(stderr, "after extstore_write_request\n");
                // cuddle the hash value into the time field so we don't have
                // to recalculate it.
                item *buf_it = (item *) io.buf;
                buf_it->time = it_info.hv;
                // copy from past the headers + time headers.
                // TODO: should be in items.c
                if (it->it_flags & ITEM_CHUNKED) {
                    // Need to loop through the item and copy
                    item_chunk *sch = (item_chunk *) ITEM_schunk(it);
                    int remain = orig_ntotal;
                    int copied = 0;
                    // copy original header
                    int hdrtotal = ITEM_ntotal(it) - it->nbytes;
                    memcpy((char *)io.buf+STORE_OFFSET, (char *)it+STORE_OFFSET, hdrtotal - STORE_OFFSET);
                    copied = hdrtotal;
                    // copy data in like it were one large object.
                    while (sch && remain) {
                        assert(remain >= sch->used);
                        memcpy((char *)io.buf+copied, sch->data, sch->used);
                        // FIXME: use one variable?
                        remain -= sch->used;
                        copied += sch->used;
                        sch = sch->next;
                    }
                } else {
                    memcpy((char *)io.buf+STORE_OFFSET, (char *)it+STORE_OFFSET, io.len-STORE_OFFSET);
                }
                // crc what we copied so we can do it sequentially.
                buf_it->it_flags &= ~ITEM_LINKED;
                buf_it->exptime = crc32c(0, (char*)io.buf+STORE_OFFSET, orig_ntotal-STORE_OFFSET);
                extstore_write(storage, &io);
                item_hdr *hdr = (item_hdr *) ITEM_data(hdr_it);
                hdr->page_version = io.page_version;
                hdr->page_id = io.page_id;
                hdr->offset  = io.offset;
                // overload nbytes for the header it
                hdr_it->nbytes = it->nbytes;
                /* success! Now we need to fill relevant data into the new
                 * header and replace. Most of this requires the item lock
                 */
                /* CAS gets set while linking. Copy post-replace */
                item_replace(it, hdr_it, it_info.hv);
                ITEM_set_cas(hdr_it, ITEM_get_cas(it));
                do_item_remove(hdr_it);
                did_moves = 1;
                LOGGER_LOG(NULL, LOG_EVICTIONS, LOGGER_EXTSTORE_WRITE, it, bucket);
            } else {
                /* Failed to write for some reason, can't continue. */
                slabs_free(hdr_it, ITEM_ntotal(hdr_it), ITEM_clsid(hdr_it));
            }
        }
    }
    do_item_remove(it);
    item_unlock(it_info.hv);
    return did_moves;
}
#endif

static pthread_mutex_t storage_write_plock;
#define WRITE_SLEEP_MIN 500
int storage_write_id = 0;
__thread int storage_write_threadId =0;
#ifndef NO_EXT
static pthread_t storage_write_tid;

int storage_write(void *storage, const int clsid,
              int local_keyLen, int local_slabNum,
              int local_lru_start, obj_io* pre_io) {
    item* hdr_it = NULL;
#ifdef FIX_KEY
    hdr_it = do_item_alloc(NULL, local_keyLen, 0, 0, sizeof(item_hdr));
    if(false){
        fprintf(stderr, "nvm_write(%ld),LRU_NVM slab_index:%d hdr_it alloc fail\n",
                pthread_self(), slab_index);
    }
    if(hdr_it == NULL){
        fprintf(stderr, "nvm_write(%ld),LRU_NVM slab_index:%d hdr_it alloc fail\n",
                pthread_self(), slab_index);
        return 0;
    }

#endif //FIX_KEY

    struct lru_pull_tail_return it_info = {NULL, 0};

    for (int i = 0; i < local_slabNum && it_info.it == NULL; ++i) {
#ifdef LRU_COUNTER
        nvm_lru_counter  = (local_lru_start + i) % local_slabNum;
#ifdef use_nvm_alloc_pull_flags
        if(nvm_alloc_pull_flags[nvm_lru_counter] == 1){
            nvm_lru_counter += local_slabNum;
        }
#endif
#endif //LRU_COUNTER
        it_info.it = NULL;
        lru_pull_tail(clsid, COLD_LRU, 0, LRU_PULL_RETURN_ITEM | LRU_NVM, 0, &it_info);
        /* Item is locked, and we have a reference to it. */
        if(it_info.it != NULL){
            break;
        }
    }

    if (it_info.it == NULL) {
//        print_slab(clsid | NVM_SLAB);
//        fprintf(stderr, "storage_write lru_pull_tail fail\n");
        item_free(hdr_it);
        return 0;
    }
#ifdef FIX_KEY
    if(flushItToExt(storage, &it_info, hdr_it, pre_io)){
#else
        if(flushItToExt(storage, &it_info, hdr_it, NULL)){
#endif //FIX_KEY
#ifdef M_STAT
        __sync_fetch_and_add(&flushToSSD, 1);
#endif //M_STAT
            return -1;
    }
    return 0;

}

static void *storage_write_thread(void *arg) {
    void *storage = arg;
    // NOTE: ignoring overflow since that would take years of uptime in a
    // specific load pattern of never going to sleep.
#ifndef ENABLE_NVM
    unsigned int backoff[MAX_NUMBER_OF_SLAB_CLASSES] = {0};
#endif
    unsigned int counter = 0;
//    useconds_t to_sleep = WRITE_SLEEP_MIN;
    logger *l = logger_create();
    if (l == NULL) {
        fprintf(stderr, "Failed to allocate logger for storage compaction thread\n");
        abort();
    }
    storage_write_threadId = __sync_fetch_and_add(&storage_write_id, 1);
    slab_index = storage_write_threadId *  (slabNum / settings.ext_io_threadcount);
    per_thread_counter = slabNum / settings.ext_io_threadcount;
    slab_span = slabNum;
    slab_min = 0;
    if(storage_write_threadId == (settings.ext_io_threadcount - 1)){
        per_thread_counter = slabNum - slab_index;
    }
    thread_type = STORAGE_WRITE_THREAD;
    fprintf(stderr, "storage_write_thread(%ld), slab_index:%d\n", pthread_self(), slab_index);

#ifdef ENABLE_NVM
    int totalFree = 0;
    unsigned int last_chunks_free[MAX_NUMBER_OF_SLAB_CLASSES] = {0};
    unsigned int last_target[MAX_NUMBER_OF_SLAB_CLASSES] = {0};
    memset(last_chunks_free, 0, sizeof (last_chunks_free));
    memset(last_target, 0,  sizeof (last_target));
//    pthread_mutex_lock(&storage_write_plock);
    // NOTE: ignoring overflow since that would take years of uptime in a
    // specific load pattern of never going to sleep.
    int local_slabNum = slabNum;
    int local_per_thread_counter = per_thread_counter;
    int local_keyLen = keyLen;
    int local_lru_index = 0;
    int local_slab_index = slab_index;
    double factor = 1.0;
    while (1) {
        // cache per-loop to avoid calls to the slabs_clsid() search loop
#if defined(NO_SSD)
        bool nvm_limit_reached = false;
#else
        bool nvm_limit_reached = true;
#endif
        if (nvm_limit_reached){
            int totalToSSD = 0;
            int chunks_free =  0;
            bool nvm_once_reached = false;

            for (int x = 1; x <MAX_NUMBER_OF_SLAB_CLASSES; x++) {
                if (get_slab_size(x) < MIN_VALUE_SIZE){
                    continue;
                }
                nvm_limit_reached = false;
                chunks_free = slabs_available_chunks(x + MAX_NUMBER_OF_SLAB_CLASSES, &nvm_limit_reached,
                                                     NULL, local_slabNum, local_slab_index, local_slab_index + local_per_thread_counter);
#ifdef SLAB_PART
                int target = settings.ext_free_memchunks[x + MAX_NUMBER_OF_SLAB_CLASSES] / local_slabNum * local_per_thread_counter;
#else
                int target = settings.ext_free_memchunks[x + MAX_NUMBER_OF_SLAB_CLASSES];
#endif
                unsigned int toNVM = 0;
                unsigned int toSSD = 0;

                if( nvm_limit_reached && (((target > (last_target[x])) || (chunks_free < target * settings.slab_automove_freeratio )
                || (chunks_free < (last_chunks_free[x] * settings.slab_automove_freeratio))))){
                    __sync_fetch_and_add(&IsWriting, 1);

                    // storage_write() will fail and cut loop after filling write buffer.
                    int failed = 0;

                    while ((chunks_free + toSSD < target * factor)) {
                        int ret = 0;
                        local_lru_index = (local_lru_index + 1) % local_per_thread_counter;


                        if ((ret=storage_write(storage, x, local_keyLen, local_slabNum,
                                               local_slab_index + local_lru_index, NULL))) {
                            toSSD ++;
                            failed = 0;
                        } else{
                            failed ++;
                            if(failed > 10){
                                break;
                            }
                        }
                    }
                    __sync_fetch_and_sub(&IsWriting, 1);

                }else {
                    nvm_limit_reached = false;
//                    fprintf(stderr, "storage_write_thread(%ld): chunks_free:%d,last_chunks_free:%d, target:%d, last_target:%d\n",
//                                    pthread_self(),chunks_free, last_chunks_free[x], target, last_target[x]);
                }

                if(toSSD){
                    counter ++;
                    if((counter % 10 == 0)){
                        fprintf(stderr, "storage_write_thread(%ld): slab:%d,chunks_free:%d,last_chunks_free:%d, last_target:%d, "
                                        "toNVM:%d, toSSD:%d, totalFree:%d, target:%d\n",
                                        pthread_self(), x, chunks_free, last_chunks_free[x], last_target[x], toNVM, toSSD, totalFree, target);
                    }
                    totalToSSD += toSSD;
                }
                totalFree += toSSD;
                last_target[x] = target;
                last_chunks_free[x] = chunks_free;
                nvm_once_reached |= nvm_limit_reached;
            }

            if(!nvm_once_reached){
                factor *=  0.5;
                if(factor < 0.1){
                    factor = 0.1;
                }
//                fprintf(stderr, "storage_write_thread(%ld): nvm_once_reached:%d sleep chunks_free:%d,last_chunks_free:%d, last_target:%d, "
//                                "toSSD:%d, totalFree:%d\n",
//                                pthread_self(), nvm_once_reached, chunks_free, last_chunks_free[12], last_target[12], totalToSSD, totalFree);
                usleep(1000);
            } else {
                factor = 1.0;
            }
#ifdef use_nvm_alloc_pull_flags
            for (int i = local_slab_index; i < local_slab_index + local_per_thread_counter; ++i) {
                nvm_alloc_pull_flags[i] = 1 - nvm_alloc_pull_flags[i];
            }
#endif
        } else{
            usleep(10000);
        }
    }

#else
    pthread_mutex_lock(&storage_write_plock);

    while (1) {
        // cache per-loop to avoid calls to the slabs_clsid() search loop
        int min_class = slabs_clsid(settings.ext_item_size);
        bool do_sleep = true;
        counter++;
        if (to_sleep > settings.ext_max_sleep)
            to_sleep = settings.ext_max_sleep;

        for (int x = 0; x < MAX_NUMBER_OF_SLAB_CLASSES; x++) {
            bool did_move = false;
            bool mem_limit_reached = false;
            unsigned int chunks_free;
            int item_age;
            int target = settings.ext_free_memchunks[x];
            if (min_class > x || (backoff[x] && (counter % backoff[x] != 0))) {
                // Long sleeps means we should retry classes sooner.
                if (to_sleep > WRITE_SLEEP_MIN * 10)
                    backoff[x] /= 2;
                continue;
            }

            // Avoid extra slab lock calls during heavy writing.
            chunks_free = slabs_available_chunks(x, &mem_limit_reached,
                    NULL);

            // storage_write() will fail and cut loop after filling write buffer.
            while (1) {
                // if we are low on chunks and no spare, push out early.
                if (chunks_free < target && mem_limit_reached) {
                    item_age = 0;
                } else {
                    item_age = settings.ext_item_age;
                }
                if (storage_write(storage, x, item_age)) {
                    chunks_free++; // Allow stopping if we've done enough this loop
                    did_move = true;
                    do_sleep = false;
                    if (to_sleep > WRITE_SLEEP_MIN)
                        to_sleep /= 2;
                } else {
                    break;
                }
            }

            if (!did_move) {
                backoff[x]++;
            } else if (backoff[x]) {
                backoff[x] /= 2;
            }
            fprintf(stderr, "slab:%d,chunks_free:%d, item_age:%d\n", x, chunks_free, item_age);
        }


        // flip lock so we can be paused or stopped
        pthread_mutex_unlock(&storage_write_plock);
        if (do_sleep) {
            usleep(to_sleep);
            to_sleep *= 2;
        }
        pthread_mutex_lock(&storage_write_plock);
    }
#endif
    return NULL;
}
#endif


// TODO
// logger needs logger_destroy() to exist/work before this is safe.
/*int stop_storage_write_thread(void) {
    int ret;
    pthread_mutex_lock(&lru_maintainer_lock);
    do_run_lru_maintainer_thread = 0;
    pthread_mutex_unlock(&lru_maintainer_lock);
    // WAKEUP SIGNAL
    if ((ret = pthread_join(lru_maintainer_tid, NULL)) != 0) {
        fprintf(stderr, "Failed to stop LRU maintainer thread: %s\n", strerror(ret));
        return -1;
    }
    settings.lru_maintainer_thread = false;
    return 0;
}*/

void storage_write_pause(void) {
    pthread_mutex_lock(&storage_write_plock);
}

void storage_write_resume(void) {
    pthread_mutex_unlock(&storage_write_plock);
}

int start_storage_write_thread(void *arg) {
    pthread_mutex_init(&storage_write_plock, NULL);
    #ifndef NO_EXT
    int ret;
    if ((ret = pthread_create(&storage_write_tid, NULL,
        storage_write_thread, arg)) != 0) {
        fprintf(stderr, "Can't create storage_write thread: %s\n",
            strerror(ret));
        return -1;
    }
    #endif

    return 0;
}


/*** COMPACTOR ***/

/* Fetch stats from the external storage system and decide to compact.
 * If we're more than half full, start skewing how aggressively to run
 * compaction, up to a desired target when all pages are full.
 */
static int storage_compact_check(void *storage, logger *l,
        uint32_t *page_id, uint64_t *page_version,
        uint64_t *page_size, bool *drop_unread) {
    struct extstore_stats st;
    int x;
    double rate;
    uint64_t frag_limit;
    uint64_t low_version = ULLONG_MAX;
    uint64_t lowest_version = ULLONG_MAX;
    unsigned int low_page = 0;
    unsigned int lowest_page = 0;
    extstore_get_stats(storage, &st);
    if (st.pages_used == 0)
        return 0;

    // lets pick a target "wasted" value and slew.
    if (st.pages_free > settings.ext_compact_under)
        return 0;
    *drop_unread = false;

    // the number of free pages reduces the configured frag limit
    // this allows us to defrag early if pages are very empty.
    rate = 1.0 - ((double)st.pages_free / st.page_count);
    rate *= settings.ext_max_frag;
    frag_limit = st.page_size * rate;
    LOGGER_LOG(l, LOG_SYSEVENTS, LOGGER_COMPACT_FRAGINFO,
            NULL, rate, frag_limit);
    st.page_data = calloc(st.page_count, sizeof(struct extstore_page_data));
    extstore_get_page_data(storage, &st);

    // find oldest page by version that violates the constraint
    for (x = 0; x < st.page_count; x++) {
        if (st.page_data[x].version == 0 ||
            st.page_data[x].bucket == PAGE_BUCKET_LOWTTL)
            continue;
        if (st.page_data[x].version < lowest_version) {
            lowest_page = x;
            lowest_version = st.page_data[x].version;
        }
        if (st.page_data[x].bytes_used < frag_limit) {
            if (st.page_data[x].version < low_version) {
                low_page = x;
                low_version = st.page_data[x].version;
            }
        }
    }
    *page_size = st.page_size;
    free(st.page_data);

    // we have a page + version to attempt to reclaim.
    if (low_version != ULLONG_MAX) {
        *page_id = low_page;
        *page_version = low_version;
        return 1;
    } else if (lowest_version != ULLONG_MAX && settings.ext_drop_unread
            && st.pages_free <= settings.ext_drop_under) {
        // nothing matched the frag rate barrier, so pick the absolute oldest
        // version if we're configured to drop items.
        *page_id = lowest_page;
        *page_version = lowest_version;
        *drop_unread = true;
        return 1;
    }

    return 0;
}

static pthread_t storage_compact_tid;
static pthread_mutex_t storage_compact_plock;
#define MIN_STORAGE_COMPACT_SLEEP 10000

struct storage_compact_wrap {
    obj_io io;
    pthread_mutex_t lock; // gates the bools.
    bool done;
    bool submitted;
    bool miss; // version flipped out from under us
};

static void storage_compact_readback(void *storage, logger *l,
        bool drop_unread, char *readback_buf,
        uint32_t page_id, uint64_t page_version, uint64_t read_size) {
    uint64_t offset = 0;
    unsigned int rescues = 0;
    unsigned int lost = 0;
    unsigned int skipped = 0;

    while (offset < read_size) {
        item *hdr_it = NULL;
        item_hdr *hdr = NULL;
        item *it = (item *)(readback_buf+offset);
        unsigned int ntotal;
        // probably zeroed out junk at the end of the wbuf
        if (it->nkey == 0) {
            break;
        }

        ntotal = ITEM_ntotal(it);
#ifdef tiny
        uint32_t hv = (uint32_t)it->time_persist;
#else
        uint32_t hv = (uint32_t)it->time;
#endif

        item_lock(hv);
        // We don't have a conn and don't need to do most of do_item_get
        hdr_it = assoc_find(ITEM_key(it), it->nkey, hv);
        if (hdr_it != NULL) {
            bool do_write = false;
            refcount_incr(hdr_it);

            // Check validity but don't bother removing it.
            if ((hdr_it->it_flags & ITEM_HDR) && !item_is_flushed(hdr_it) &&
                   (hdr_it->exptime == 0 || hdr_it->exptime > current_time)) {
                hdr = (item_hdr *)ITEM_data(hdr_it);
                if (hdr->page_id == page_id && hdr->page_version == page_version) {
                    // Item header is still completely valid.
                    extstore_delete(storage, page_id, page_version, 1, ntotal);
                    // drop inactive items.
                    if (drop_unread && GET_LRU(hdr_it->slabs_clsid) == COLD_LRU) {
                        do_write = false;
                        skipped++;
                    } else {
                        do_write = true;
                    }
                }
            }

            if (do_write) {
                bool do_update = false;
                int tries;
                obj_io io;
                io.len = ntotal;
                io.mode = OBJ_IO_WRITE;
                for (tries = 10; tries > 0; tries--) {
                    if (extstore_write_request(storage, PAGE_BUCKET_COMPACT, PAGE_BUCKET_COMPACT, &io) == 0) {
                        memcpy(io.buf, it, io.len);
                        extstore_write(storage, &io);
                        do_update = true;
                        break;
                    } else {
                        usleep(1000);
                    }
                }

                if (do_update) {
                    if (it->refcount == 2) {
                        hdr->page_version = io.page_version;
                        hdr->page_id = io.page_id;
                        hdr->offset = io.offset;
                        rescues++;
                    } else {
                        lost++;
                        // TODO: re-alloc and replace header.
                    }
                } else {
                    lost++;
                }
            }

            do_item_remove(hdr_it);
        }

        item_unlock(hv);
        offset += ntotal;
        if (read_size - offset < sizeof(struct _stritem))
            break;
    }

    STATS_LOCK();
    stats.extstore_compact_lost += lost;
    stats.extstore_compact_rescues += rescues;
    stats.extstore_compact_skipped += skipped;
    STATS_UNLOCK();
    LOGGER_LOG(l, LOG_SYSEVENTS, LOGGER_COMPACT_READ_END,
            NULL, page_id, offset, rescues, lost, skipped);
}

static void _storage_compact_cb(void *e, obj_io *io, int ret) {
    struct storage_compact_wrap *wrap = (struct storage_compact_wrap *)io->data;
    assert(wrap->submitted == true);

    pthread_mutex_lock(&wrap->lock);

    if (ret < 1) {
        wrap->miss = true;
    }
    wrap->done = true;

    pthread_mutex_unlock(&wrap->lock);
}

// TODO: hoist the storage bits from lru_maintainer_thread in here.
// would be nice if they could avoid hammering the same locks though?
// I guess it's only COLD. that's probably fine.
static void *storage_compact_thread(void *arg) {
    void *storage = arg;
    useconds_t to_sleep = settings.ext_max_sleep;
    bool compacting = false;
    uint64_t page_version = 0;
    uint64_t page_size = 0;
    uint64_t page_offset = 0;
    uint32_t page_id = 0;
    bool drop_unread = false;
    char *readback_buf = NULL;
    struct storage_compact_wrap wrap;

    logger *l = logger_create();
    if (l == NULL) {
        fprintf(stderr, "Failed to allocate logger for storage compaction thread\n");
        abort();
    }

    readback_buf = malloc(settings.ext_wbuf_size);
    if (readback_buf == NULL) {
        fprintf(stderr, "Failed to allocate readback buffer for storage compaction thread\n");
        abort();
    }

    pthread_mutex_init(&wrap.lock, NULL);
    wrap.done = false;
    wrap.submitted = false;
    wrap.io.data = &wrap;
    wrap.io.iov = NULL;
    wrap.io.buf = (void *)readback_buf;

    wrap.io.len = settings.ext_wbuf_size;
    wrap.io.mode = OBJ_IO_READ;
    wrap.io.cb = _storage_compact_cb;
    pthread_mutex_lock(&storage_compact_plock);

    while (1) {
        pthread_mutex_unlock(&storage_compact_plock);
        if (to_sleep) {
            extstore_run_maint(storage);
            usleep(to_sleep);
        }
        pthread_mutex_lock(&storage_compact_plock);

        if (!compacting && storage_compact_check(storage, l,
                    &page_id, &page_version, &page_size, &drop_unread)) {
            page_offset = 0;
            compacting = true;
            LOGGER_LOG(l, LOG_SYSEVENTS, LOGGER_COMPACT_START,
                    NULL, page_id, page_version);
        }

        if (compacting) {
            pthread_mutex_lock(&wrap.lock);
            if (page_offset < page_size && !wrap.done && !wrap.submitted) {
                wrap.io.page_version = page_version;
                wrap.io.page_id = page_id;
                wrap.io.offset = page_offset;
                // FIXME: should be smarter about io->next (unlink at use?)
                wrap.io.next = NULL;
                wrap.submitted = true;
                wrap.miss = false;

                extstore_submit(storage, &wrap.io);
            } else if (wrap.miss) {
                LOGGER_LOG(l, LOG_SYSEVENTS, LOGGER_COMPACT_ABORT,
                        NULL, page_id);
                wrap.done = false;
                wrap.submitted = false;
                compacting = false;
            } else if (wrap.submitted && wrap.done) {
                LOGGER_LOG(l, LOG_SYSEVENTS, LOGGER_COMPACT_READ_START,
                        NULL, page_id, page_offset);
                storage_compact_readback(storage, l, drop_unread,
                        readback_buf, page_id, page_version, settings.ext_wbuf_size);
                page_offset += settings.ext_wbuf_size;
                wrap.done = false;
                wrap.submitted = false;
            } else if (page_offset >= page_size) {
                compacting = false;
                wrap.done = false;
                wrap.submitted = false;
                extstore_close_page(storage, page_id, page_version);
                LOGGER_LOG(l, LOG_SYSEVENTS, LOGGER_COMPACT_END,
                        NULL, page_id);
            }
            pthread_mutex_unlock(&wrap.lock);

            // finish actual compaction quickly.
            to_sleep = MIN_STORAGE_COMPACT_SLEEP;
        } else {
            if (to_sleep < settings.ext_max_sleep)
                to_sleep += settings.ext_max_sleep;
        }
    }
    free(readback_buf);

    return NULL;
}

// TODO
// logger needs logger_destroy() to exist/work before this is safe.
/*int stop_storage_compact_thread(void) {
    int ret;
    pthread_mutex_lock(&lru_maintainer_lock);
    do_run_lru_maintainer_thread = 0;
    pthread_mutex_unlock(&lru_maintainer_lock);
    if ((ret = pthread_join(lru_maintainer_tid, NULL)) != 0) {
        fprintf(stderr, "Failed to stop LRU maintainer thread: %s\n", strerror(ret));
        return -1;
    }
    settings.lru_maintainer_thread = false;
    return 0;
}*/

void storage_compact_pause(void) {
    pthread_mutex_lock(&storage_compact_plock);
}

void storage_compact_resume(void) {
    pthread_mutex_unlock(&storage_compact_plock);
}

int start_storage_compact_thread(void *arg) {
    int ret;

    pthread_mutex_init(&storage_compact_plock, NULL);
    if ((ret = pthread_create(&storage_compact_tid, NULL,
        storage_compact_thread, arg)) != 0) {
        fprintf(stderr, "Can't create storage_compact thread: %s\n",
            strerror(ret));
        return -1;
    }

    return 0;
}

/*** UTILITY ***/
// /path/to/file:100G:bucket1
// FIXME: Modifies argument. copy instead?
struct extstore_conf_file *storage_conf_parse(char *arg, unsigned int page_size) {
    struct extstore_conf_file *cf = NULL;
    char *b = NULL;
    char *p = strtok_r(arg, ":", &b);
    char unit = 0;
    uint64_t multiplier = 0;
    int base_size = 0;
    if (p == NULL)
        goto error;
    // First arg is the filepath.
    cf = calloc(1, sizeof(struct extstore_conf_file));
    cf->file = strdup(p);

    p = strtok_r(NULL, ":", &b);
    if (p == NULL) {
        fprintf(stderr, "must supply size to ext_path, ie: ext_path=/f/e:64m (M|G|T|P supported)\n");
        goto error;
    }
    unit = tolower(p[strlen(p)-1]);
    p[strlen(p)-1] = '\0';
    // sigh.
    switch (unit) {
        case 'm':
            multiplier = 1024 * 1024;
            break;
        case 'g':
            multiplier = 1024 * 1024 * 1024;
            break;
        case 't':
            multiplier = 1024 * 1024;
            multiplier *= 1024 * 1024;
            break;
        case 'p':
            multiplier = 1024 * 1024;
            multiplier *= 1024 * 1024 * 1024;
            break;
    }
    base_size = atoi(p);
    multiplier *= base_size;
    // page_count is nearest-but-not-larger-than pages * psize
    cf->page_count = multiplier / page_size;
    assert(page_size * cf->page_count <= multiplier);

    // final token would be a default free bucket
    p = strtok_r(NULL, ",", &b);
    // TODO: We reuse the original DEFINES for now,
    // but if lowttl gets split up this needs to be its own set.
    if (p != NULL) {
        if (strcmp(p, "compact") == 0) {
            cf->free_bucket = PAGE_BUCKET_COMPACT;
        } else if (strcmp(p, "lowttl") == 0) {
            cf->free_bucket = PAGE_BUCKET_LOWTTL;
        } else if (strcmp(p, "chunked") == 0) {
            cf->free_bucket = PAGE_BUCKET_CHUNKED;
        } else if (strcmp(p, "default") == 0) {
            cf->free_bucket = PAGE_BUCKET_DEFAULT;
        } else {
            fprintf(stderr, "Unknown extstore bucket: %s\n", p);
            goto error;
        }
    } else {
        // TODO: is this necessary?
        cf->free_bucket = PAGE_BUCKET_DEFAULT;
    }

    // TODO: disabling until compact algorithm is improved.
    if (cf->free_bucket != PAGE_BUCKET_DEFAULT) {
        fprintf(stderr, "ext_path only presently supports the default bucket\n");
        goto error;
    }

    return cf;
error:
    if (cf) {
        if (cf->file)
            free(cf->file);
        free(cf);
    }
    return NULL;
}

int nvm_conf_parse(char *arg, struct settings* settings) {
    char *b = NULL;
    char *p = strtok_r(arg, ":", &b);
    char unit = 0;
    uint64_t multiplier = 0;
    int base_size = 0;
    if (p == NULL)
        return 1;
    // First arg is the filepath.
    settings->nvm_file = strdup(p);

    p = strtok_r(NULL, ":", &b);
    if (p == NULL) {
        fprintf(stderr, "must supply size to ext_path, ie: ext_path=/f/e:64m (M|G|T|P supported)\n");
        return 1;
    }
    unit = tolower(p[strlen(p)-1]);
    p[strlen(p)-1] = '\0';
    // sigh.
    switch (unit) {
        case 'm':
            multiplier = 1024 * 1024;
            break;
        case 'g':
            multiplier = 1024 * 1024 * 1024;
            break;
        case 't':
            multiplier = 1024 * 1024;
            multiplier *= 1024 * 1024;
            break;
        case 'p':
            multiplier = 1024 * 1024;
            multiplier *= 1024 * 1024 * 1024;
            break;
        default:
            return 1;
    }
    base_size = atoi(p);
    multiplier *= base_size;
    settings->nvm_limit = multiplier;

    return 0;

}


struct storage_settings {
    struct extstore_conf_file *storage_file;
    struct extstore_conf ext_cf;
};

struct nvm_settings {
    char* file;
    uint64_t limit;
    unsigned int block_size; // ideally 64-256M in size
    unsigned int io_threadcount;
};

void *storage_init_config(struct settings *s) {
    struct storage_settings *cf = calloc(1, sizeof(struct storage_settings));

    s->ext_item_size = 512;
    s->ext_item_age = UINT_MAX;
    s->ext_low_ttl = 0;
    s->ext_recache_rate = 2000;
    s->ext_max_frag = 0.8;
    s->ext_drop_unread = false;
#ifdef ENABLE_NVM
    s->ext_wbuf_size = 1024 * 256;
#else
    s->ext_wbuf_size = 1024 * 1024 * 4;
#endif
    s->ext_compact_under = 0;
    s->ext_drop_under = 0;
    s->ext_max_sleep = 1000000;
    s->slab_automove_freeratio = 0.01;
    s->ext_page_size = 1024 * 1024 * 32;
    s->ext_io_threadcount = 1;
    cf->ext_cf.page_size = settings.ext_page_size;
    cf->ext_cf.wbuf_size = settings.ext_wbuf_size;
    cf->ext_cf.io_threadcount = settings.ext_io_threadcount;
    cf->ext_cf.swap_threadcount = cf->ext_cf.io_threadcount;
    settings.swap_threadcount = cf->ext_cf.swap_threadcount;
    cf->ext_cf.io_depth = 1;
#ifdef ENABLE_NVM
    cf->ext_cf.page_buckets = 4 + MAX_THREAD;
    cf->ext_cf.wbuf_count = 4 * ( cf->ext_cf.page_size / cf->ext_cf.wbuf_size);
#else
    cf->ext_cf.page_buckets = 4;
    cf->ext_cf.wbuf_count = cf->ext_cf.page_buckets;
#endif
    cf->ext_cf.bufSwapFn = _storage_Swap_wbuf;
    cf->ext_cf.itemSwap = _swap_item;

    return cf;
}


// TODO: pass settings struct?
#ifdef ENABLE_NVM
int storage_read_config(void *conf, void *nvm_conf, char **subopt){
#else
int storage_read_config(void *conf, char **subopt) {
#endif
    fprintf(stderr, "storage_read_config\n");
    struct storage_settings *cf = conf;
    struct nvm_settings *nvm_cf = nvm_conf;
    struct extstore_conf *ext_cf = &cf->ext_cf;
    char *subopts_value;

    enum {
        EXT_PAGE_SIZE,
        EXT_WBUF_SIZE,
        EXT_THREADS,
        EXT_IO_DEPTH,
        EXT_PATH,
        EXT_ITEM_SIZE,
        EXT_ITEM_AGE,
        EXT_LOW_TTL,
        EXT_RECACHE_RATE,
        EXT_COMPACT_UNDER,
        EXT_DROP_UNDER,
        EXT_MAX_SLEEP,
        EXT_MAX_FRAG,
        EXT_DROP_UNREAD,
        SLAB_AUTOMOVE_FREERATIO, // FIXME: move this back?
#ifdef ENABLE_NVM
        NVM_PATH,
#endif
    };

    char *const subopts_tokens[] = {
        [EXT_PAGE_SIZE] = "ext_page_size",
        [EXT_WBUF_SIZE] = "ext_wbuf_size",
        [EXT_THREADS] = "ext_threads",
        [EXT_IO_DEPTH] = "ext_io_depth",
        [EXT_PATH] = "ext_path",
        [EXT_ITEM_SIZE] = "ext_item_size",
        [EXT_ITEM_AGE] = "ext_item_age",
        [EXT_LOW_TTL] = "ext_low_ttl",
        [EXT_RECACHE_RATE] = "ext_recache_rate",
        [EXT_COMPACT_UNDER] = "ext_compact_under",
        [EXT_DROP_UNDER] = "ext_drop_under",
        [EXT_MAX_SLEEP] = "ext_max_sleep",
        [EXT_MAX_FRAG] = "ext_max_frag",
        [EXT_DROP_UNREAD] = "ext_drop_unread",
        [SLAB_AUTOMOVE_FREERATIO] = "slab_automove_freeratio",
#ifdef ENABLE_NVM
        [NVM_PATH] = "nvm_path",
#endif
        NULL
    };

    switch (getsubopt(subopt, subopts_tokens, &subopts_value)) {
        case EXT_PAGE_SIZE:
            if (cf->storage_file) {
                fprintf(stderr, "Must specify ext_page_size before any ext_path arguments\n");
                return 1;
            }
            if (subopts_value == NULL) {
                fprintf(stderr, "Missing ext_page_size argument\n");
                return 1;
            }
            if (!safe_strtoul(subopts_value, &ext_cf->page_size)) {
                fprintf(stderr, "could not parse argument to ext_page_size\n");
                return 1;
            }
            ext_cf->page_size *= 1024 * 1024; /* megabytes */
            break;
        case EXT_WBUF_SIZE:
            if (subopts_value == NULL) {
                fprintf(stderr, "Missing ext_wbuf_size argument\n");
                return 1;
            }
            if (!safe_strtoul(subopts_value, &ext_cf->wbuf_size)) {
                fprintf(stderr, "could not parse argument to ext_wbuf_size\n");
                return 1;
            }
            ext_cf->wbuf_size *= 1024; /* megabytes */
            settings.ext_wbuf_size = ext_cf->wbuf_size;
            break;
        case EXT_THREADS:
            if (subopts_value == NULL) {
                fprintf(stderr, "Missing ext_threads argument\n");
                return 1;
            }
            if (!safe_strtoul(subopts_value, &ext_cf->io_threadcount)) {
                fprintf(stderr, "could not parse argument to ext_threads\n");
                return 1;
            }
            settings.ext_io_threadcount = ext_cf->io_threadcount;
            settings.swap_threadcount = ext_cf->swap_threadcount;
            break;
        case EXT_IO_DEPTH:
            if (subopts_value == NULL) {
                fprintf(stderr, "Missing ext_io_depth argument\n");
                return 1;
            }
            if (!safe_strtoul(subopts_value, &ext_cf->io_depth)) {
                fprintf(stderr, "could not parse argument to ext_io_depth\n");
                return 1;
            }
            break;
        case EXT_ITEM_SIZE:
            if (subopts_value == NULL) {
                fprintf(stderr, "Missing ext_item_size argument\n");
                return 1;
            }
            if (!safe_strtoul(subopts_value, &settings.ext_item_size)) {
                fprintf(stderr, "could not parse argument to ext_item_size\n");
                return 1;
            }
            break;
        case EXT_ITEM_AGE:
            if (subopts_value == NULL) {
                fprintf(stderr, "Missing ext_item_age argument\n");
                return 1;
            }
            if (!safe_strtoul(subopts_value, &settings.ext_item_age)) {
                fprintf(stderr, "could not parse argument to ext_item_age\n");
                return 1;
            }
            fprintf(stderr, "ext_item_age:%d\n", settings.ext_item_age);
            break;
        case EXT_LOW_TTL:
            if (subopts_value == NULL) {
                fprintf(stderr, "Missing ext_low_ttl argument\n");
                return 1;
            }
            if (!safe_strtoul(subopts_value, &settings.ext_low_ttl)) {
                fprintf(stderr, "could not parse argument to ext_low_ttl\n");
                return 1;
            }
            break;
        case EXT_RECACHE_RATE:
            if (subopts_value == NULL) {
                fprintf(stderr, "Missing ext_recache_rate argument\n");
                return 1;
            }
            if (!safe_strtoul(subopts_value, &settings.ext_recache_rate)) {
                fprintf(stderr, "could not parse argument to ext_recache_rate\n");
                return 1;
            }
            break;
        case EXT_COMPACT_UNDER:
            if (subopts_value == NULL) {
                fprintf(stderr, "Missing ext_compact_under argument\n");
                return 1;
            }
            if (!safe_strtoul(subopts_value, &settings.ext_compact_under)) {
                fprintf(stderr, "could not parse argument to ext_compact_under\n");
                return 1;
            }
            break;
        case EXT_DROP_UNDER:
            if (subopts_value == NULL) {
                fprintf(stderr, "Missing ext_drop_under argument\n");
                return 1;
            }
            if (!safe_strtoul(subopts_value, &settings.ext_drop_under)) {
                fprintf(stderr, "could not parse argument to ext_drop_under\n");
                return 1;
            }
            break;
        case EXT_MAX_SLEEP:
            if (subopts_value == NULL) {
                fprintf(stderr, "Missing ext_max_sleep argument\n");
                return 1;
            }
            if (!safe_strtoul(subopts_value, &settings.ext_max_sleep)) {
                fprintf(stderr, "could not parse argument to ext_max_sleep\n");
                return 1;
            }
            break;
        case EXT_MAX_FRAG:
            if (subopts_value == NULL) {
                fprintf(stderr, "Missing ext_max_frag argument\n");
                return 1;
            }
            if (!safe_strtod(subopts_value, &settings.ext_max_frag)) {
                fprintf(stderr, "could not parse argument to ext_max_frag\n");
                return 1;
            }
            break;
        case SLAB_AUTOMOVE_FREERATIO:
            if (subopts_value == NULL) {
                fprintf(stderr, "Missing slab_automove_freeratio argument\n");
                return 1;
            }
            if (!safe_strtod(subopts_value, &settings.slab_automove_freeratio)) {
                fprintf(stderr, "could not parse argument to slab_automove_freeratio\n");
                return 1;
            }
            break;
        case EXT_DROP_UNREAD:
            settings.ext_drop_unread = true;
            break;
        case EXT_PATH:
            if (subopts_value) {
                struct extstore_conf_file *tmp = storage_conf_parse(subopts_value, ext_cf->page_size);
                if (tmp == NULL) {
                    fprintf(stderr, "failed to parse ext_path argument\n");
                    return 1;
                }
                if (cf->storage_file != NULL) {
                    tmp->next = cf->storage_file;
                }
                cf->storage_file = tmp;
            } else {
                fprintf(stderr, "missing argument to ext_path, ie: ext_path=/d/file:5G\n");
                return 1;
            }
            break;
        case NVM_PATH:
            if(nvm_conf_parse(subopts_value, &settings)){
                fprintf(stderr, "failed to parse nvm_path argument\n");
                return 1;
            }
            nvm_cf->file = settings.nvm_file;
            nvm_cf->limit = settings.nvm_limit;
            break;
        default:
            fprintf(stderr, "Illegal suboption \"%s\"\n", subopts_value);
            return 1;
    }

    return 0;
}

int storage_check_config(void *conf) {
    struct storage_settings *cf = conf;
    struct extstore_conf *ext_cf = &cf->ext_cf;

    if (cf->storage_file) {
        if (settings.item_size_max > ext_cf->wbuf_size) {
            fprintf(stderr, "-I (item_size_max: %d) cannot be larger than ext_wbuf_size: %d\n",
                settings.item_size_max, ext_cf->wbuf_size);
            return 1;
        }

        if (settings.udpport) {
            fprintf(stderr, "Cannot use UDP with extstore enabled (-U 0 to disable)\n");
            return 1;
        }

        return 0;
    }

    return 2;
}

void *storage_init(void *conf) {
    struct storage_settings *cf = conf;
    struct extstore_conf *ext_cf = &cf->ext_cf;

    enum extstore_res eres;
    void *storage = NULL;
    if (settings.ext_compact_under == 0) {
        // If changing the default fraction (4), change the help text as well.
        settings.ext_compact_under = cf->storage_file->page_count / 4;
        /* Only rescues non-COLD items if below this threshold */
        settings.ext_drop_under = cf->storage_file->page_count / 4;
    }
    crc32c_init();
    /* Init free chunks to zero. */
    for (int x = 0; x < MAX_NUMBER_OF_SLAB_CLASSES; x++) {
        settings.ext_free_memchunks[x] = 0;
    }
    storage = extstore_init(cf->storage_file, ext_cf, &eres);
    if (storage == NULL) {
        fprintf(stderr, "Failed to initialize external storage: %s\n",
                extstore_err(eres));
        if (eres == EXTSTORE_INIT_OPEN_FAIL) {
            perror("extstore open");
        }
        return NULL;
    }

    return storage;
}

#endif

// nvm

typedef struct storage_nvm {
    char *data;
    uint64_t offset;
    uint64_t block_no;
    unsigned int block_size; // ideally 64-256M in size
    unsigned int io_threadcount;
}storage_nvm;


#ifdef ENABLE_NVM



/*
 * WRITE FLUSH THREAD
 */

bool flushItFromDRAMToExt(void * storage, struct lru_pull_tail_return *it_info, item *hdr_it){
    bool did_moves = false;
    item* it = it_info->it;

    /* First, storage for the header object */
    size_t orig_ntotal = ITEM_ntotal(it);
#ifdef FIX_KEY

    obj_io io;
    //            fprintf(stderr, "storage_write orig_ntotal:%ld\n", orig_ntotal);
    io.len = orig_ntotal;
    io.mode = OBJ_IO_WRITE;
    // NOTE: when the item is read back in, the slab mover
    // may see it. Important to have refcount>=2 or ~ITEM_LINKED
    assert(it->refcount >= 2);
    // NOTE: write bucket vs free page bucket will disambiguate once
    // lowttl feature is better understood.
    if (extstore_write_request(storage, PAGE_BUCKET_BASE, PAGE_BUCKET_DEFAULT, &io) == 0) {
        /* success! Now we need to fill relevant data into the new
         * header and replace. Most of this requires the item lock
         */


        // cuddle the hash value into the time field so we don't have
        // to recalculate it.
        item *buf_it = (item *) io.buf;
#ifdef tiny
        buf_it->time_persist = it_info->hv;
#else
        buf_it->time = it_info->hv;
#endif

        // copy from past the headers + time headers.
        // TODO: should be in items.c
        memcpy((char *)io.buf+READ_OFFSET, (char *)it+READ_OFFSET, io.len-READ_OFFSET);
        buf_it->nbytes = it->nbytes;
        buf_it->nkey = it->nkey;
        buf_it->slabs_clsid = it->slabs_clsid;
        // crc what we copied so we can do it sequentially.
        buf_it->it_flags &= ~ITEM_LINKED;
        buf_it->crc = crc32c(0, (char*)io.buf+READ_OFFSET, orig_ntotal-READ_OFFSET);
        buf_it->h_next = hdr_it;
        extstore_write(storage, &io);

        if(false){
            fprintf(stderr, "(%ld), flushItToExt: before replace it:%p, it->ref:%d, it->flag:%d, hv:%d\n" , pthread_self(),
                    (void *)it, it->refcount, it->it_flags, it_info->hv);
        }

        memcpy(ITEM_key(hdr_it), ITEM_key(it), it->nkey);
        hdr_it->nkey = it->nkey;
        hdr_it->nbytes = it->nbytes;
        item_hdr *hdr = (item_hdr *) ITEM_data(hdr_it);
        hdr->page_version = io.page_version;
        hdr->page_id = io.page_id;
        hdr->offset  = io.offset;
        /* no need to replace */
        hdr_it->it_flags |= ITEM_HDR;
        if(false){
            fprintf(stderr, "flushItToExt io->offset:%d, page:%d, crc:%d, orig_ntotal:%ld, len:%d\n",
                    io.offset, io.page_id, buf_it->exptime, orig_ntotal, io.len);
        }
        item_replace(it, hdr_it, it_info->hv);
        ITEM_set_cas(hdr_it, ITEM_get_cas(it));
        if(false){
            fprintf(stderr, "(%ld), flushItToExt: it:%p, it->ref:%d, it->flag:%d, hv:%d\n" , pthread_self(),
                    (void *)it, it->refcount, it->it_flags, hash(ITEM_key(it), it->nkey));
        }
        did_moves = true;
        do_item_remove(hdr_it);

        do_item_remove_with_unlock(it_info->it, it_info->hv);
    } else{
        do_item_remove_with_unlock(it_info->it, it_info->hv);
        item_free(hdr_it);
    }

#else
    fprintf(stderr, "flushItFromDRAMToExt error\n");
#endif // FIX_KEY
    return did_moves;
}
bool flushItToExt(void * storage, struct lru_pull_tail_return *it_info, item *hdr_it, obj_io* pre_io){
    bool did_moves = false;
    item* it = it_info->it;
    /* First, storage for the header object */
    size_t orig_ntotal = ITEM_ntotal(it);
#ifdef FIX_KEY

    obj_io io;
    //            fprintf(stderr, "storage_write orig_ntotal:%ld\n", orig_ntotal);
    io.len = orig_ntotal;
    io.mode = OBJ_IO_WRITE;
    // NOTE: when the item is read back in, the slab mover
    // may see it. Important to have refcount>=2 or ~ITEM_LINKED
    assert(it->refcount >= 2);
    // NOTE: write bucket vs free page bucket will disambiguate once
    // lowttl feature is better understood.
    if (extstore_write_request(storage, PAGE_BUCKET_BASE, PAGE_BUCKET_DEFAULT, &io) == 0) {
        /* success! Now we need to fill relevant data into the new
         * header and replace. Most of this requires the item lock
         */


        // cuddle the hash value into the time field so we don't have
        // to recalculate it.
        item *buf_it = (item *) io.buf;
#ifdef tiny
        buf_it->time_persist = it_info->hv;
#else
        buf_it->time = it_info->hv;
#endif
        // copy from past the headers + time headers.
        // TODO: should be in items.c
        memcpy((char *)io.buf+READ_OFFSET, (char *)((item_hdr *) ITEM_data(it))->nvmptr, io.len-READ_OFFSET);
        buf_it->nbytes = it->nbytes;
        buf_it->nkey = it->nkey;
        buf_it->slabs_clsid = it->slabs_clsid;
        // crc what we copied so we can do it sequentially.
        buf_it->it_flags &= ~ITEM_LINKED;
        buf_it->crc = crc32c(0, (char*)io.buf+READ_OFFSET, orig_ntotal-READ_OFFSET);
        buf_it->h_next = hdr_it;
        extstore_write(storage, &io);

        if(false){
            fprintf(stderr, "(%ld), flushItToExt: before replace it:%p, it->ref:%d, it->flag:%d, hv:%d\n" , pthread_self(),
                    (void *)it, it->refcount, it->it_flags, it_info->hv);
        }

        memcpy(ITEM_key(hdr_it), ITEM_key(it), it->nkey);
        hdr_it->nkey = it->nkey;
        hdr_it->nbytes = it->nbytes;
        item_hdr *hdr = (item_hdr *) ITEM_data(hdr_it);
        hdr->page_version = io.page_version;
        hdr->page_id = io.page_id;
        hdr->offset  = io.offset;
        hdr_it->it_flags |= ITEM_HDR;
        hdr_it->it_flags &= (~ITEM_ACTIVE) & (~ITEM_FETCHED) & (~ITEM_NEED_SWAP);
        hdr_it->read_count = 0;
#ifdef tiny
        hdr_it->time_persist = it->time_persist;
#else
        hdr_it->time = it->time;
#endif

        if(false){
            fprintf(stderr, "flushItToExt io->offset:%d, page:%d, crc:%d, orig_ntotal:%ld, len:%d\n",
                    io.offset, io.page_id, buf_it->exptime, orig_ntotal, io.len);
        }
        item_replace(it, hdr_it, it_info->hv);
        ITEM_set_cas(hdr_it, ITEM_get_cas(it));
        did_moves = true;
#if defined(DRMA_CACHE) || defined(MEM_MOD)
        item_hdr *old_hdr = (item_hdr*) ITEM_data(it_info->it);
        if(old_hdr->cache_item != NULL) {
            if(old_hdr->cache_item->it_flags & ITEM_LINKED){
                item_unlink_q(old_hdr->cache_item);
                old_hdr->cache_item->it_flags &= ~ITEM_LINKED;
                do_item_remove(old_hdr->cache_item);
            }

            old_hdr->cache_item = NULL;
        }
#endif
        do_item_remove(hdr_it);
        do_item_remove(it_info->it);
        item_unlock(it_info->hv);
    } else{
        do_item_remove(it_info->it);
        item_unlock(it_info->hv);
        item_free(hdr_it);
    }

#else
        obj_io io;
        //
        int bucket = PAGE_BUCKET_DEFAULT;
        io.len = orig_ntotal;
        io.mode = OBJ_IO_WRITE;
        // NOTE: when the item is read back in, the slab mover
        // may see it. Important to have refcount>=2 or ~ITEM_LINKED
        assert(it->refcount >= 2);
        // NOTE: write bucket vs free page bucket will disambiguate once
        // lowttl feature is better understood.
        if (extstore_write_request(storage, bucket, bucket, &io) == 0) {
            /* success! Now we need to fill relevant data into the new
             * header and replace. Most of this requires the item lock
             */

            //                fprintf(stderr, "after extstore_write_request\n");
            // cuddle the hash value into the time field so we don't have
            // to recalculate it.
            item *buf_it = (item *) io.buf;
            buf_it->time = it_info->hv;
            // copy from past the headers + time headers.
            // TODO: should be in items.c
            if (it_info->it_flags & ITEM_CHUNKED) {
                fprintf(stderr, "flushItToExt(%ld), error! unsupported ITEM_CHUNKED\n", pthread_self());
                exit(0);
            } else {
                memcpy((char *)io.buf+STORE_OFFSET, (char *)it+STORE_OFFSET, io.len-STORE_OFFSET);
            }
            // crc what we copied so we can do it sequentially.
            buf_it->it_flags &= ~ITEM_LINKED;
            buf_it->exptime = crc32c(0, (char*)io.buf+STORE_OFFSET, orig_ntotal-STORE_OFFSET);
            extstore_write(storage, &io);

            item_lock(it_info->hv);
            if( (it->it_flags & ITEM_LINKED) && hash(ITEM_key(it), it->nkey) == it_info->hv
            && strncmp(ITEM_key(it), it_info->key, it_info->nkey) == 0){

                item_hdr *hdr = (item_hdr *) ITEM_data(it);
                item* nvm_data = (item*) hdr->nvmptr;
                hdr->page_version = io.page_version;
                hdr->page_id = io.page_id;
                hdr->offset  = io.offset;
                /* no need to replace */
                item_unlink_q(it);
                it->it_flags |= ITEM_HDR;
                it->it_flags &= (~ITEM_NVM);
                slabs_free(nvm_data, ITEM_ntotal(nvm_data), ITEM_clsid(nvm_data) | NVM_SLAB);
                //            fprintf(stderr, "(%ld), flushItToExt: it:%p, it->flag:%d, nvm_data:%p, flag:%d\n" , pthread_self(),
                //                    (void *)it, it->it_flags, (void *)nvm_data, nvm_data->it_flags);
                did_moves = true;
            }
            item_unlock(it_info->hv);


            //            fprintf(stderr, "flushItToSSD: %.30s, page:%d, version:%d, off:%d, crc:%d, len:%d\n",
            //                    ITEM_key(it), hdr->page_id, hdr->page_version, hdr->offset, buf_it->exptime, io.len);
        } else{
            counter ++;
            if(counter % LOG_INTERLVAL == 0){
                fprintf(stderr, "%s,%d, flushItToExt failed!\n", __FILE__, __LINE__);

            }
        }
#endif //FIX_KEY
    return did_moves;
}

item* writeItToExt(void * storage, char* key, char* value, int key_n, int value_n, uint32_t hv){
    item *hdr_it = do_item_alloc(NULL, key_n, 0, 0, sizeof(item_hdr));
    if(hdr_it == NULL){
        return NULL;
    }
    size_t orig_ntotal = sizeof(item) + key_n + 1 +value_n;

    obj_io io;
    //            fprintf(stderr, "storage_write orig_ntotal:%ld\n", orig_ntotal);
    io.len = orig_ntotal;
    io.mode = OBJ_IO_WRITE;
    // NOTE: when the item is read back in, the slab mover
    // may see it. Important to have refcount>=2 or ~ITEM_LINKED
    // NOTE: write bucket vs free page bucket will disambiguate once
    // lowttl feature is better understood.
    if (extstore_write_request(storage, PAGE_BUCKET_BASE, PAGE_BUCKET_DEFAULT, &io) == 0) {
        /* success! Now we need to fill relevant data into the new
         * header and replace. Most of this requires the item lock
         */


        // cuddle the hash value into the time field so we don't have
        // to recalculate it.
        item *buf_it = (item *) io.buf;
        // copy from past the headers + time headers.
        // TODO: should be in items.c
        memcpy(ITEM_data(buf_it), value, value_n);
        memcpy(ITEM_key(buf_it), key, key_n);
#ifdef tiny
        buf_it->time_persist = hv;
#else
        buf_it->time = hv;
#endif
        buf_it->nbytes =value_n;
        buf_it->nkey = key_n;
        buf_it->slabs_clsid = slabs_clsid(orig_ntotal);
        buf_it->it_flags &= ~ITEM_LINKED;
        buf_it->crc = crc32c(0, (char*)io.buf+READ_OFFSET, orig_ntotal-READ_OFFSET);
        buf_it->h_next = hdr_it;
        extstore_write(storage, &io);
        memcpy(ITEM_key(hdr_it), key, key_n);
        hdr_it->nkey = key_n;
        hdr_it->nbytes = value_n;
        item_hdr *hdr = (item_hdr *) ITEM_data(hdr_it);
        hdr->page_version = io.page_version;
        hdr->page_id = io.page_id;
        hdr->offset  = io.offset;
        hdr_it->it_flags |= ITEM_HDR;
    } else{
        item_free(hdr_it);
        hdr_it = NULL;
    }
    return hdr_it;
}

bool flushItToNvm( struct lru_pull_tail_return *it_info, item *hdr_it, int local_nvm_lru_counter){
    bool did_moves = false;
    item* it = it_info->it;
    /* First, storage for the header object */
    size_t orig_ntotal = ITEM_ntotal(it);
#ifdef FIX_KEY
    /* Run the storage write understanding the start of the item is dirty.
     * We will fill it (time/exptime/etc) from the header item on read.
     */
    memcpy(ITEM_key(hdr_it), ITEM_key(it), it->nkey);
    hdr_it->nkey = it->nkey;
    item_hdr *hdr = (item_hdr *) ITEM_data(hdr_it);
    hdr_it->nbytes = it->nbytes;
    char* nvm_data = hdr->nvmptr;
    memcpy((char *)nvm_data,
           (char *)it+READ_OFFSET, orig_ntotal-READ_OFFSET);
    hdr_it->it_flags |= ITEM_NVM;
    hdr_it->it_flags &= (~ITEM_ACTIVE);
#ifdef LRU_COUNTER
    hdr_it->slabs_clsid |= (local_nvm_lru_counter * POWER_LARGEST);
#endif //LRU_COUNTER
    // overload nbytes for the header it
    hdr_it->nbytes = it->nbytes;
    if(false){
        fprintf(stderr, "(%ld), flushItToNvm alloc:%p,ref:%d, flag:%d hv:%d\n",
                pthread_self(), (void *)hdr_it,  it->refcount, it->it_flags, it_info->hv);
    }
    /* success! Now we need to fill relevant data into the new
         * header and replace. Most of this requires the item lock
         */
    /* CAS gets set while linking. Copy post-replace */
    if(false){
        //                uint64_t  key_n;
        //                memcpy(&key_n, ITEM_key(it), sizeof (uint64_t));
        //                fprintf(stderr, "(%ld), replace: hdr_it:%p, old it:%p slab_id:%d\n", pthread_self(), (void *)hdr_it, (void *)it, hdr_it->slabs_clsid);
        //                fprintf(stderr, "flushItToNVM: %ld\n", key_n);
    }
    item_replace(it, hdr_it, it_info->hv);
    ITEM_set_cas(hdr_it, ITEM_get_cas(it));
    do_item_remove(hdr_it);
    did_moves = true;
    do_item_remove_with_unlock(it_info->it, it_info->hv);

#else
    uint32_t flags;
    FLAGS_CONV(it, flags);
    item *hdr_it = do_item_alloc(ITEM_key(it), it->nkey, flags, it->exptime, sizeof(item_hdr));
    /* Run the storage write understanding the start of the item is dirty.
     * We will fill it (time/exptime/etc) from the header item on read.
     */
    if(hdr_it != NULL){
        char* nvm_data =  NULL;
#ifdef SLAB_PART
        for (int i = 0; i < slabNum; i++) {
            slab_counter = (slab_index + i) % settings.ext_io_threadcount;
#else
            for (int i = 0; i < 100; i++) {
#endif
                nvm_data =  slabs_alloc(orig_ntotal, it_info->slabs_clsid | NVM_SLAB, 0);
#ifndef NO_FLUSH_IN_ALLOC
                if(nvm_data == NULL){
                    struct lru_pull_tail_return new_it_info = {NULL, 0};
                    //TODO:  id may need change
                    for (int j = 0; j < MAX_THREAD; ++j) {
#ifdef LRU_COUNTER
                        lru_counter  = (slab_index + j) % MAX_THREAD;
#endif
                        lru_pull_tail(it_info->slabs_clsid , COLD_LRU, 0, LRU_PULL_RETURN_ITEM | LRU_NVM, 0, &new_it_info);
                        if(new_it_info.it != NULL){
                            if(!flushItToExt(ext_storage, &new_it_info)){
                                //                            fprintf(stderr, "%s %d flushItToExt failed\n", __FILE__, __LINE__);
                            }
                        }
                    }
                } else{
                    break;
                }
#else
if(nvm_data != NULL){
                    break;
                }
#endif
if(i % 18 == 17){
                    counter ++;
                    if(counter % LOG_INTERLVAL == 0){
                        fprintf(stderr, "flushItToNvm: nvm_slabs_alloc:%d, slab_index:%d, slab_counter:%d\n",
                                i, slab_index, slab_counter);
                        print_slab(it_info->slabs_clsid  | NVM_SLAB);
                        //                        exit(-1);
                    }
                }
            }

            if ( nvm_data != NULL) {
                //            fprintf(stderr, "storage_write key:%.10s\n", ITEM_key(it));

                memcpy((char *)nvm_data+READ_OFFSET,
                       (char *)it+READ_OFFSET, orig_ntotal-READ_OFFSET);
                item_lock(it_info->hv);
                if( (it->it_flags & ITEM_LINKED) && hash(ITEM_key(it), it->nkey) == it_info->hv
                && strncmp(ITEM_key(it), it_info->key, it_info->nkey) == 0){
                    refcount_incr(it);

                    hdr_it->it_flags |= ITEM_NVM;
                    //            item *buf_it = (item *) ((char*)nvm->data[nvm->block_no] + nvm->offset);
                    //            buf_it->time = it_info.hv;

                    item_hdr *hdr = (item_hdr *) ITEM_data(hdr_it);
                    hdr->nvmptr = nvm_data;
                    hdr->nvm_slab_id = ITEM_clsid(it);
#ifdef LRU_COUNTER
                    nvm_lru_counter  = (nvm_lru_counter + 1) % MAX_THREAD;
                    hdr->nvm_slab_id |= (nvm_lru_counter % slabNum * POWER_LARGEST);
#endif
                    // overload nbytes for the header it
                    hdr_it->nbytes = it->nbytes;
                    /* success! Now we need to fill relevant data into the new
                         * header and replace. Most of this requires the item lock
                         */
                    /* CAS gets set while linking. Copy post-replace */
                    //                fprintf(stderr, "(%ld), replace: old it:%p slab_id:%d\n", pthread_self(), (void *)it, it->slabs_clsid);
                    //                fprintf(stderr, "flushItToNVM: %.30s\n", ITEM_key(it));
                    item_replace(it, hdr_it, it_info->hv);
                    ITEM_set_cas(hdr_it, ITEM_get_cas(it));
                    do_item_remove(hdr_it);
                    did_moves = true;
                    item_unlock(it_info->hv);
                    do_item_remove(it);
                } else{
                    item_unlock(it_info->hv);
                    slabs_free(nvm_data, ITEM_ntotal((item*)nvm_data), ITEM_clsid((item*)nvm_data) | NVM_SLAB);
                    slabs_free(hdr_it, hdr_it->nbytes, ITEM_clsid(hdr_it));
                }

                //                fprintf(stderr, "replace\n");

            } else{
                //                fprintf(stderr, "flushItToNvm before slabs_free: %p, flag:%d, has been linked\n", (void *)hdr_it, hdr_it->it_flags);
                slabs_free(hdr_it, hdr_it->nbytes, ITEM_clsid(hdr_it));
            }
        } else{
            //            fprintf(stderr, "flushToNvm alloc hdr_it error:\n");
        }
#endif //FIX_KEY
    return did_moves;
}

    int swap_nvm_to_dram(struct lru_pull_tail_return *it_info, item *new_it){
        int did_moves = 0;
        item *it = it_info->it;
        /* First, storage for the header object */
#ifdef FIX_KEY
        item* nvm_data = (item*) ((item_hdr*)ITEM_data(it))->nvmptr;
        memcpy((char *)new_it+READ_OFFSET,
               (char *)nvm_data, ITEM_ntotal(it)-READ_OFFSET);
        memcpy(ITEM_key(new_it), ITEM_key(it), it->nkey);

        new_it->nbytes = it->nbytes;
        new_it->nkey = it->nkey;
        item_replace(it, new_it, it_info->hv);
        ITEM_set_cas(new_it, ITEM_get_cas(it));
        do_item_remove(new_it);
        if(false){
            fprintf(stderr, "(%ld), swap it:%p,ref:%d, flag:%d hv:%d\n",
                    pthread_self(), (void *)it,  it->refcount, it->it_flags, it_info->hv);
        }
        do_item_remove_with_unlock(it_info->it, it_info->hv);
#ifdef M_STAT
        __sync_fetch_and_add(&NVMtoDRAMswap, 1);
#endif //M_STAT
        did_moves = 1;

#else
        new_it = do_item_alloc(it_info->key, it_info->nkey, flags, it->exptime, it->nbytes);
        //            item *buf_it = (item *) ((char*)nvm->data[nvm->block_no] + nvm->offset);
        //            buf_it->time = it_info.hv;
        if(new_it != NULL){
            item* nvm_data = (item*) ((item_hdr*)ITEM_data(it))->nvmptr;
            memcpy((char *)new_it+READ_OFFSET,
                   (char *)nvm_data+READ_OFFSET, ITEM_ntotal(it)-READ_OFFSET);
            if( (it->it_flags & ITEM_LINKED) && hash(ITEM_key(it), it->nkey) == it_info->hv
            && strncmp(ITEM_key(it), it_info->key, it_info->nkey) == 0){
                if(false){
                    fprintf(stderr, "(%ld), before swap it:%p,ref:%d, flag:%d hv:%d\n",
                            pthread_self(), (void *)it,  it->refcount, it->it_flags, it_info->hv);
                }
                new_it->it_flags = (it->it_flags & (~ITEM_NVM) & (~ITEM_HDR));
                item_replace(it, new_it, it_info->hv);
                ITEM_set_cas(new_it, ITEM_get_cas(it));
                do_item_remove(new_it);
                if(false){
                    fprintf(stderr, "(%ld), swap it:%p,ref:%d, flag:%d hv:%d\n",
                            pthread_self(), (void *)it,  it->refcount, it->it_flags, it_info->hv);
                }
                do_item_remove(it_info->it);
                item_unlock(it_info->hv);
#ifndef FIX_KEY
                slabs_free(nvm_data, ITEM_ntotal(nvm_data), ITEM_clsid(nvm_data) | NVM_SLAB);
#endif
#ifdef M_STAT
                __sync_fetch_and_add(&NVMtoDRAMswap, 1);
#endif
                did_moves = 1;
            } else{
                do_item_remove(it_info->it);
                item_unlock(it_info->hv);
                slabs_free(new_it, ITEM_ntotal(new_it), ITEM_clsid(new_it));
            }


        } else{
            do_item_remove(it_info->it);
            item_unlock(it_info->hv);
            //        fprintf(stderr, "(%ld), swap item alloc error\n", pthread_self());
        }
#endif //FIX_KEY
        return did_moves;
}
    extern bool inTxn;
#if defined(DRAM_CACHE) || defined(MEM_MOD)
    int dram_evict(const int clsid, int local_slabNum, int local_per_thread_counter,
                   int local_lru_start, int local_nvm_lru_index, int local_nvm_slab_index){
        struct lru_pull_tail_return it_info = {NULL, 0};

        for (int i = 0; i < local_slabNum && it_info.it == NULL; ++i) {
#ifdef LRU_COUNTER
            {
                lru_counter = (local_lru_start + i) % local_slabNum;
#ifdef use_alloc_pull_flags
                if(alloc_pull_flags[lru_counter] == 0){
                    lru_counter += local_slabNum;
                }
#endif
            }
#endif //LRU_COUNTER
it_info.it = NULL;
            lru_pull_tail(clsid, COLD_LRU, 0, LRU_PULL_RETURN_ITEM | LRU_PULL_MEM_ONLY, 0, &it_info);
            /* Item is locked, and we have a reference to it. */
            if(it_info.it != NULL){
                break;
            }
            if ((i < local_per_thread_counter)) {
                lru_pull_tail_hot(clsid, HOT_LRU, 0, LRU_PULL_RETURN_ITEM, 0, &it_info);
            }
        }

        if (it_info.it == NULL) {
            return 0;
        }
        item *it = it_info.it;
        item* hdr_it = do_item_get(ITEM_key(it), it->nkey, it_info.hv,  NULL, DONT_UPDATE);
        if(hdr_it == NULL){
            if(it->it_flags & ITEM_LINKED){
                it->it_flags &= ~ITEM_LINKED;
                item_unlink_q(it);
            }
            do_item_remove(it);
            item_unlock(it_info.hv);
//            fprintf(stderr, "dram_evict error, it:%p, it->flag:%d, ref:%d\n",
//                    (void *)it, it->it_flags, it->refcount);
//            exit(0);
            return 1;
        }
//        item* hdr_it = item_get(ITEM_key(it), it->nkey, NULL, DONT_UPDATE);
        item_hdr * hdr =  (item_hdr *)ITEM_data(hdr_it);
        if(((hdr_it->it_flags & ITEM_NVM)) && hdr->cache_item == it){
#ifdef MEM_MOD
            item* nvm_item = (item*)hdr->nvmptr;
            memcpy(ITEM_key(nvm_item), ITEM_key(it), it->nkey);
            memcpy(ITEM_data(nvm_item), ITEM_data(it), it->nbytes + 2);
            if(hdr->cache_item->it_flags & ITEM_LINKED){
                hdr->cache_item->it_flags &= ~ITEM_LINKED;
                item_unlink_q(hdr->cache_item);
                do_item_remove(hdr->cache_item);
            }
            hdr->cache_item = NULL;
#ifdef M_STAT
            __sync_fetch_and_add(&flushToNVM, 1);
#endif

#else
            do_item_remove(it);
            hdr->cache_item = NULL;
#endif
        } else{

            if(it->it_flags & ITEM_LINKED){
                it->it_flags &= ~ITEM_LINKED;
                item_unlink_q(it);
            }
//            fprintf(stderr, "dram_evict error, it:%p, it->flag:%d, ref:%d\n",
//                    (void *)it, it->it_flags, it->refcount);
//            exit(0);
        }
        do_item_remove(hdr_it);
        do_item_remove(it);
        item_unlock(it_info.hv);
        return 1;
    }
#endif
    int nvm_write(void *storage, const int clsid, int local_slabNum, int local_per_thread_counter,
                  int local_lru_start, int local_nvm_lru_index, int local_nvm_slab_index) {
    int did_moves = 0;
    item* hdr_it = NULL;
    size_t orig_ntotal = 0;
#ifdef FIX_KEY
#ifdef NVM_AS_DRAM
    hdr_it = do_item_alloc(NULL, keyLen, 0, 0, sizeof(item_hdr));
#else
    orig_ntotal = get_slab_size(clsid);

    for (int i = 0; i < local_slabNum && hdr_it == NULL; ++i) {
        int slab_counter = (local_nvm_slab_index + i) % local_slabNum;
        int classId = slabs_clsid(orig_ntotal - READ_OFFSET);
        hdr_it = slabs_alloc_c(orig_ntotal - READ_OFFSET, classId | NVM_SLAB, 0, slab_counter, local_slabNum);
    }
    if(false){
        fprintf(stderr, "nvm_write(%ld), slab_index:%d hdr_it alloc nvm fail, per_thread_counter:%d\n",
                pthread_self(), local_nvm_slab_index, per_thread_counter);
        print_slab(clsid | NVM_SLAB);
    }
#endif // NVM_AS_DRAM
    if(hdr_it == NULL){
//        fprintf(stderr, "nvm_write alloc hdr_it fail, clsid:%d\n", clsid);
        return did_moves;
    }
#endif //FIX_KEY

    struct lru_pull_tail_return it_info = {NULL, 0};

    for (int i = 0; i < local_slabNum && it_info.it == NULL; ++i) {
#ifdef LRU_COUNTER
        {
            lru_counter = (local_lru_start + i) % local_slabNum;
#ifdef use_alloc_pull_flags
            if(alloc_pull_flags[lru_counter] == 0){
                lru_counter += local_slabNum;
            }
#endif
        }
#endif //LRU_COUNTER
        it_info.it = NULL;
        lru_pull_tail(clsid, COLD_LRU, 0, LRU_PULL_RETURN_ITEM | LRU_PULL_MEM_ONLY, 0, &it_info);
        /* Item is locked, and we have a reference to it. */
        if(it_info.it != NULL){
            break;
        }
        if ((i < local_per_thread_counter)) {
            lru_pull_tail_hot(clsid, HOT_LRU, 0, LRU_PULL_RETURN_ITEM, 0, &it_info);
        }
    }

    if (it_info.it == NULL) {
//        fprintf(stderr, "nvm_write lru_pull_tail fail\n");
        slabs_free(hdr_it, orig_ntotal, clsid | NVM_SLAB);
        return did_moves;
    }
    if ((it_info.it->it_flags & ITEM_HDR) == 0 && (it_info.it->it_flags & ITEM_NVM) == 0) {
#ifdef NVM_AS_DRAM
            if(flushItFromDRAMToExt(storage, &it_info, hdr_it)){
                did_moves = -1;
#ifdef M_STAT
                __sync_fetch_and_add(&flushToSSD, 1);
#endif
            }
#else
#if defined(DRAM_TO_SSD)  && !defined(NO_SSD)
        if(!inTxn || it_info.it->it_flags & ITEM_ACTIVE){
                if(flushItToNvm(&it_info, hdr_it)){
#ifdef M_STAT
                    __sync_fetch_and_add(&flushToNVM, 1);
#endif //M_STAT
                    did_moves = 1;
                }
        } else{
                orig_ntotal = get_slab_size(clsid);
                slabs_free(hdr_it, orig_ntotal, clsid | NVM_SLAB);
                char key[keyLen];
                hdr_it = do_item_alloc(key, keyLen, 0, 0, sizeof(item_hdr));
                if(hdr_it != NULL && flushItFromDRAMToExt(storage, &it_info, hdr_it)){
#ifdef M_STAT
                    __sync_fetch_and_add(&flushToSSD, 1);
#endif //M_STAT
                    did_moves = -1;
                }
        }

#else
            int local_nvm_lru_counter = (local_nvm_lru_index + local_nvm_slab_index)%local_slabNum;
#ifdef use_nvm_alloc_pull_flags
            if(nvm_alloc_pull_flags[local_nvm_lru_counter] == 0){
                local_nvm_lru_counter += local_slabNum;
            }
#endif
        if(flushItToNvm(&it_info, hdr_it, local_nvm_lru_counter)){
#ifdef M_STAT
            __sync_fetch_and_add(&flushToNVM, 1);
#endif
            did_moves = 1;
        }
#endif //defined(DRAM_TO_SSD)  && !defined(NO_SSD)
#endif //NVM_AS_DRAM
    }
    return did_moves;
}
#ifndef NO_EXT

int nvm_swap(void *storage, const int clsid, const int item_age,  int8_t flag) {
    int did_moves = 0;
    item* new_it = NULL;
#ifdef FIX_KEY
    new_it = do_item_alloc(NULL, keyLen, 0, 0, get_slab_size(clsid) - sizeof (item) - keyLen - 8);
    if(new_it == NULL){
        return did_moves;
    }
//    fprintf(stderr, "(%ld), nvm_swap: new_it:%p, keyLen:%d, clsid:%d, flag:%d, new->slab:%d, nbytes:%ld\n",
//            pthread_self(), (void *)new_it, keyLen, clsid, new_it->it_flags, new_it->slabs_clsid, get_slab_size(clsid) - sizeof (item) - keyLen - 8);
#endif


    struct lru_pull_tail_return it_info = {NULL, 0};
    for (int i = 0; i < slabNum; ++i) {
#ifdef LRU_COUNTER
        nvm_lru_counter  = (slab_index + i) % slabNum;
#ifdef use_nvm_alloc_pull_flags
        if(nvm_alloc_pull_flags[nvm_lru_counter] == 1){
            nvm_lru_counter += slabNum;
        }
#endif
#endif
        it_info.it = NULL;
        lru_pull_head(clsid, COLD_LRU, 0, LRU_PULL_RETURN_ITEM | LRU_NVM, 0, &it_info);
        /* Item is locked, and we have a reference to it. */
        if(it_info.it != NULL){
            break;
        }
    }
    /* Item is locked, and we have a reference to it. */

//    fprintf(stderr, "(%ld), nvm_swap: get return:%p\n", pthread_self(), (void *)it_info.it);
    if (it_info.it == NULL) {
        item_free(new_it);
        return did_moves;
    }

    did_moves = swap_nvm_to_dram(&it_info, new_it);
    return did_moves;
}
#if defined(DRAM_CACHE) || defined(MEM_MOD)

int nvm_add_to_cache(const int clsid, const int item_age,  int8_t flag) {
        int did_moves = 0;
        item* new_it = NULL;
#ifdef FIX_KEY
        new_it = do_item_alloc(NULL, keyLen, 0, 0, get_slab_size(clsid) - sizeof (item) - keyLen - 8);
        if(new_it == NULL){
            return did_moves;
        }
        //    fprintf(stderr, "(%ld), nvm_swap: new_it:%p, keyLen:%d, clsid:%d, flag:%d, new->slab:%d, nbytes:%ld\n",
        //            pthread_self(), (void *)new_it, keyLen, clsid, new_it->it_flags, new_it->slabs_clsid, get_slab_size(clsid) - sizeof (item) - keyLen - 8);
#endif


    struct lru_pull_tail_return it_info = {NULL, 0};
    for (int i = 0; i < slabNum; ++i) {
#ifdef LRU_COUNTER
            nvm_lru_counter  = (slab_index + i) % slabNum;
#ifdef use_nvm_alloc_pull_flags
            if(nvm_alloc_pull_flags[nvm_lru_counter] == 1){
                nvm_lru_counter += slabNum;
            }
#endif
#endif
        it_info.it = NULL;
        lru_pull_head(clsid, COLD_LRU, 0, LRU_PULL_RETURN_ITEM | LRU_NVM, 0, &it_info);
        /* Item is locked, and we have a reference to it. */
        if(it_info.it != NULL){
            break;
        }
    }
    /* Item is locked, and we have a reference to it. */

    //    fprintf(stderr, "(%ld), nvm_swap: get return:%p\n", pthread_self(), (void *)it_info.it);
    if (it_info.it == NULL) {
        item_free(new_it);
        return did_moves;
    }
    item *it = it_info.it;
    /* First, storage for the header object */
    item* nvm_data = (item*) ((item_hdr*)ITEM_data(it))->nvmptr;
    memcpy((char *)new_it+READ_OFFSET,
           (char *)nvm_data+READ_OFFSET, ITEM_ntotal(it)-READ_OFFSET);
    memcpy(ITEM_key(new_it), ITEM_key(it), it->nkey);

    new_it->nbytes = it->nbytes;
    new_it->nkey = it->nkey;
    item_hdr *hdr = (item_hdr *)ITEM_data(it);
    item_link_q(new_it);
    hdr->cache_item = new_it;
    do_item_remove(it);
    item_unlock(it_info.hv);
#ifdef M_STAT
    __sync_fetch_and_add(&NVMtoDRAMswap, 1);
#endif //M_STAT
    did_moves = 1;
   if(false){
       uint64_t  key_n;
       memcpy(&key_n, ITEM_key(it), sizeof (uint64_t));
       fprintf(stderr, "nvm_add_to_cache :%ld\n", key_n);
   }
    return did_moves;
}
#endif

static pthread_t nvm_write_tid;
static pthread_t nvm_swap_tid;
int nvm_write_id = 0;
extern uint16_t need_free_slabs_count;
extern int value_len;
static void *nvm_write_thread(void *arg){
#if !defined(DRAM_CACHE) && !defined(MEM_MOD)
    void *storage = arg;
#endif
    int index = __sync_fetch_and_add(&nvm_write_id, 1);
#ifdef NVM_AS_DRAM
    storage_write_threadId = __sync_fetch_and_add(&storage_write_id, 1);
#endif //NVM_AS_DRAM
//#ifdef NVM_AIO
//    slab_index = index *  (slabNum / NVM_TNUM);
//    int local_nvm_slab_index = index * (slabNum/ NVM_TNUM);
//    per_thread_counter = slabNum /NVM_TNUM;
//    int local_nvm_per_thread_counter = slabNum / NVM_TNUM;
//    if(index == (NVM_TNUM - 1)){
//        per_thread_counter = slabNum - slab_index;
//        local_nvm_per_thread_counter = slabNum - local_nvm_slab_index;
//    }
//#else
    slab_index = index *  (slabNum / settings.ext_io_threadcount);
    int local_nvm_slab_index = index * (slabNum/ settings.ext_io_threadcount);
    per_thread_counter = slabNum / settings.ext_io_threadcount;
    int local_nvm_per_thread_counter = slabNum / settings.ext_io_threadcount;
    if(index == (settings.ext_io_threadcount - 1)){
        per_thread_counter = slabNum - slab_index;
        local_nvm_per_thread_counter = slabNum - local_nvm_slab_index;
    }
//#endif
    slab_span = slabNum;
    slab_min =0;
    thread_type = NVM_WRITE_THREAD;
    int totalFree = 0;
    int counter = 0;
    int empty = 0;
    unsigned int last_chunks_free[MAX_NUMBER_OF_SLAB_CLASSES] = {0};
    unsigned int last_target[MAX_NUMBER_OF_SLAB_CLASSES] = {0};
    memset(last_chunks_free, 0, sizeof (last_chunks_free));
    memset(last_target, 0,  sizeof (last_target));
//    int need_sleep[MAX_NUMBER_OF_SLAB_CLASSES] = {0};

#ifdef SLAB_PART
    size_t ntotal = sizeof(item) + keyLen + value_len;;
    unsigned int id = slabs_clsid(ntotal);
    prealloc_nvm_slab(id, local_nvm_slab_index);
#endif
    int local_slabNum = slabNum;
    int local_per_thread_counter = per_thread_counter;
    int local_keyLen = keyLen;
    int local_lru_index = 0;
    int local_nvm_lru_index = 0;
    int local_slab_index = slab_index;
    double factor = 1.0;
    fprintf(stderr, "nvm_write_thread(%ld), slab_index:%d, local_keyLen:%d, keyLen:%d\n",
            pthread_self(), slab_index, local_keyLen, keyLen);

    // NOTE: ignoring overflow since that would take years of uptime in a
    // specific load pattern of never going to sleep.
    while (1) {
        // cache per-loop to avoid calls to the slabs_clsid() search loop
        bool mem_limit_reached = false;
        int totalToNVM = 0;
        bool mem_once_reached = true;
#ifdef ENABLE_NVM
        for (int x = 1; x < MAX_NUMBER_OF_SLAB_CLASSES; x++) {
            if (get_slab_size(x) < MIN_VALUE_SIZE){
                continue;
            }
#else
        for (int x = POWER_SMALLEST; x < MAX_NUMBER_OF_SLAB_CLASSES; x++) {
#endif //ENABLE_NVM
            unsigned int chunks_free;
            int item_age;
            mem_limit_reached=false;
            // Avoid extra slab lock calls during heavy writing.
            chunks_free = slabs_available_chunks(x, &mem_limit_reached,
                                                 NULL, local_slabNum, local_slab_index, local_slab_index + local_per_thread_counter);
            mem_once_reached |= mem_limit_reached;
#ifdef SLAB_PART
            int target = settings.ext_free_memchunks[x] / local_slabNum * local_per_thread_counter;
#else
            int target = settings.ext_free_memchunks[x];
#endif
            if(target == 0){
                continue;
            }

            unsigned int toNVM = 0;
            unsigned int toSSD = 0;
            int failed = 0;
            if(mem_limit_reached && ((target > last_target[x]) || (chunks_free < target * settings.slab_automove_freeratio) || (chunks_free < last_chunks_free[x] * settings.slab_automove_freeratio))){
                // storage_write() will fail and cut loop after filling write buffer.
                __sync_fetch_and_add(&IsWriting, 1);
                while (((chunks_free + toNVM < target * factor))) {
                    int ret = 0;
                    local_lru_index = (local_lru_index + 1) % local_per_thread_counter;
                    local_nvm_lru_index = (local_nvm_lru_index + 1) % local_nvm_per_thread_counter;
#if defined(DRAM_CACHE) || defined(MEM_MOD)
                    if ((ret=dram_evict(x, local_slabNum, local_per_thread_counter,
                                       local_lru_index + local_slab_index, local_nvm_lru_index, local_nvm_slab_index))) {
#else
                    if ((ret=nvm_write(storage, x, local_slabNum, local_per_thread_counter,
                                        local_lru_index + local_slab_index, local_nvm_lru_index, local_nvm_slab_index))) {
#endif
                        toNVM ++;
                        failed = 0;
                    } else{
                        failed ++;
#ifdef use_alloc_pull_flags
                        if(failed % 5 == 0){
                            for (int i = 0; i < local_per_thread_counter; ++i) {
                                alloc_pull_flags[(slab_index + i) ] =
                                        1 - alloc_pull_flags[(slab_index + i) ];
                            }
                        }
#endif
                        if(failed > 3){
                            break;
                        }
                    }
                }
                __sync_fetch_and_sub(&IsWriting, 1);
#ifdef use_alloc_pull_flags
                for (int i = 0; i < local_per_thread_counter; ++i) {
                    alloc_pull_flags[(slab_index + i)] =
                            1 - alloc_pull_flags[(slab_index + i) ];
                }
#endif
            } else {
                mem_limit_reached = false;
//                fprintf(stderr, "nvm_write_thread(%ld): slab:%d, slab_index:%d, chunks_free:%d,last_chunks_free:%d, last_target:%d, target:%d\n",
//                                pthread_self(), x, slab_index, chunks_free, last_chunks_free[x], last_target[x], target);
            }


            if( toNVM != 0 || toSSD != 0){
                counter ++;
                if(counter % 10000 == 0  && slab_index == 0){
                    fprintf(stderr, "nvm_write_thread(%ld): slab:%d,chunks_free:%d,last_chunks_free:%d, last_target:%d, "
                                    "toNVM:%d, toSSD:%d, totalFree:%d, item_age:%d, target:%d, asso_delet:%ld\n",
                            pthread_self(), x, chunks_free, last_chunks_free[x], last_target[x], toNVM, toSSD, totalFree, item_age, target,asso_delet);
                }
                totalToNVM += toNVM;
                totalFree += toNVM;
            }
            last_chunks_free[x] = chunks_free;
            last_target[x] = target;
        }
        if(totalToNVM == 0){
            empty ++;
            if(empty == 5){
                factor *= 0.5;
                if(factor < 0.1){
                    factor = 0.1;
                }
                IsWriting = false;
                if(false){
                    fprintf(stderr, "nvm_write_thread(%ld): mem_limit_reached:%d, sleep slab_index:%d, last_chunks_free:%d, last_target:%d\n",
                            pthread_self(), mem_limit_reached, slab_index,last_chunks_free[9], last_target[9]);
                }
                usleep(1000);
                empty = 0;
#ifdef use_alloc_pull_flags
                for (int i = 0; i < local_per_thread_counter; ++i) {
                    alloc_pull_flags[(slab_index + i) ] =
                            1 - alloc_pull_flags[(slab_index + i)];
                }
#endif
            }
        } else{
            factor = 1.0;
            empty = 0;
        }
    }
    return NULL;
}

static void *nvm_swap_thread(void *arg){
#if  !defined(DRAM_CACHE) && !defined(MEM_MOD)
    void *storage = arg;
#endif
    thread_type = NVM_SWAP_THREAD;
    slab_index = 0;
    per_thread_counter = slabNum;
    slab_span = slabNum;
    slab_min = 0;
    logger *l = logger_create();
    if (l == NULL) {
        fprintf(stderr, "Failed to allocate logger for storage compaction thread\n");
        abort();
    }
    // NOTE: ignoring overflow since that would take years of uptime in a
    // specific load pattern of never going to sleep.
    while (1) {
        // cache per-loop to avoid calls to the slabs_clsid() search loop
        unsigned int totalToDRAM = 0;
        for (int x = 1; x <= MAX_NUMBER_OF_SLAB_CLASSES; x++) {
            if (get_slab_size(x) < MIN_VALUE_SIZE){
                continue;
            }
            unsigned int toDRAM = 0;
            if(settings.ext_free_memchunks[x] == 0){
                // don't do anything before the work start
                continue;
            }
            // storage_write() will fail and cut loop after filling write buffer.
            for (int i = 0; i < 500; ++i) {
                int ret = 0;
#if defined(DRAM_CACHE) || defined(MEM_MOD)
                if ((ret=nvm_add_to_cache(x, 0, LRU_NVM))) {
#else
                if ((ret=nvm_swap(storage, x, 0, LRU_NVM))) {
#endif
                    toDRAM ++;
                } else {
                    break;
                }
            }
            totalToDRAM += toDRAM;

        }
        usleep(10000);

    }
    return NULL;

}

#endif //NO_EXT

int start_nvm_write_thread(void *arg) {
#ifndef NO_EXT
    int ret;

    if ((ret = pthread_create(&nvm_write_tid, NULL,
                              nvm_write_thread, arg)) != 0) {
        fprintf(stderr, "Can't create nvm_write thread: %s\n",
                strerror(ret));
        return -1;
    }
#endif //NO_EXT
    return 0;
}

int start_nvm_swap_thread(void *arg) {
#ifndef NO_EXT
    int ret;

    if ((ret = pthread_create(&nvm_swap_tid, NULL,
                              nvm_swap_thread, arg)) != 0) {
        fprintf(stderr, "Can't create nvm_swap thread: %s\n",
                strerror(ret));
        return -1;
    }
#endif
    return 0;
}
#endif




inline char* mallocNVM(const char* filename, size_t size){
    char* str;
#ifdef ENABLE_NVM
#ifdef NUMA
    str = numa_alloc_onnode(size, 1);
    if(str == NULL){
        fprintf(stderr,"could not allocate memory on node %d!\n", 1);
    }
#else
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
    } else{
        fprintf(stderr, "mallocNVM pmem_map_file:%s size:%ld, success\n",
                filename, size);
    }
#endif
#else
    posix_memalign((void**)(&str), 64, size);
#endif
    return str;
}

#ifdef ENABLE_NVM
void *nvm_init_config(struct settings *s){
    struct nvm_settings *cf = calloc(1, sizeof(struct nvm_settings));
    s->nvm_threadcount = 4;
    s->nvm_limit = 1 << 30;
    s->nvm_file = NULL;
    s->nvm_block_size = 64 * 1024 * 1024;
    cf->io_threadcount = s->nvm_threadcount;
    cf->block_size = s->nvm_block_size;
    return cf;
}


char* NVM_base=NULL;
size_t NVM_len = 0;
size_t NVM_size = 0;

void nvm_init(void *conf){
    struct nvm_settings* cf = conf;
    if(cf->file ==NULL){
        fprintf(stderr, "nvm_init error!\n");
        return;
    }
#ifdef DRAM_TEST
    char *data = malloc(cf->limit);
    memset(data, 0, cf->limit);
#else
    char* data = mallocNVM(cf->file, cf->limit);
    memset(data, 0, cf->limit);
    NVM_size =cf->limit;
#endif
#ifdef NVM_AS_DRAM
    NVM_base = data;
    NVM_len = cf->limit;
    slabs_prefill_global_with_nvm(data, cf->limit);
#else
    nvm_slabs_init(data, cf->limit, false, false);
#endif
}


int nvm_get_item(conn *c, item *it, mc_resp *resp) {
#ifdef NEED_ALIGN
    item_hdr hdr;
    memcpy(&hdr, ITEM_data(it), sizeof(hdr));
#else
    item_hdr *hdr = (item_hdr *)ITEM_data(it);
#endif
    char data[it->nbytes];
    memcpy(data, hdr->nvmptr, it->nbytes);
return 0;
}


void write_nt_64(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa  (%[data]), %%zmm0 \n"
            WRITE_NT_64_ASM
            :
    : [addr] "r" (dst), [data] "r" (src)
    : "%zmm0"
    );
}

void write_nt_128(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa  (%[data]), %%zmm0 \n"
            WRITE_NT_128_ASM
            :
    : [addr] "r" (dst), [data] "r" (src)
    : "%zmm0"
    );
}

void write_nt_256(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa  (%[data]), %%zmm0 \n"
            WRITE_NT_256_ASM
            :
    : [addr] "r" (dst), [data] "r" (src)
    : "%zmm0"
    );
}

void write_nt_512(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa  (%[data]), %%zmm0 \n"
            WRITE_NT_512_ASM
            :
    : [addr] "r" (dst), [data] "r" (src)
    : "%zmm0"
    );
}

void write_nt_1024(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa  (%[data]), %%zmm0 \n"
            WRITE_NT_1024_ASM
            :
    : [addr] "r" (dst), [data] "r" (src)
    : "%zmm0"
    );
}

void write_nt_2048(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa  (%[data]), %%zmm0 \n"
            WRITE_NT_2048_ASM
            :
    : [addr] "r" (dst), [data] "r" (src)
    : "%zmm0"
    );
}

void write_nt_4096(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa  (%[data]), %%zmm0 \n"
            WRITE_NT_4096_ASM
            :
    : [addr] "r" (dst), [data] "r" (src)
    : "%zmm0"
    );
}

void write_nt_8192(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa  (%[data]), %%zmm0 \n"
            WRITE_NT_8192_ASM
            :
    : [addr] "r" (dst), [data] "r" (src)
    : "%zmm0"
    );
}

void write_nt_16384(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa  (%[data]), %%zmm0 \n"
            WRITE_NT_8192_ASM
            "add $8192, %[addr]\n"
            WRITE_NT_8192_ASM
            : [addr] "+r" (dst)
            : [data] "r" (src)
            : "%zmm0"
            );
}

void write_nt_32768(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa  (%[data]), %%zmm0 \n"
            EXEC_4_TIMES(
                    WRITE_NT_8192_ASM
                    "add $8192, %[addr]\n"
                    )
                    : [addr] "+r" (dst)
                    : [data] "r" (src)
                    : "%zmm0"
                    );
}

void write_nt_65536(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa  (%[data]), %%zmm0 \n"
            EXEC_8_TIMES(
                    WRITE_NT_8192_ASM
                    "add $8192, %[addr]\n"
                    )
                    : [addr] "+r" (dst)
                    : [data] "r" (src)
                    : "%zmm0"
                    );
}

void memcpy_nt_128(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa (%[data]), %%zmm0 \n"
            "vmovntdqa 1*64(%[data]), %%zmm1 \n"
            "vmovntdq %%zmm0, 0(%[addr]) \n"\
            "vmovntdq %%zmm1, 1*64(%[addr]) \n"
            :
            : [addr] "r" (dst), [data] "r" (src)
            : "%zmm0", "%zmm1"
            );
}

void memcpy_nt_256(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa (%[data]), %%zmm0 \n"
            "vmovntdqa 1*64(%[data]), %%zmm1 \n"
            "vmovntdqa 2*64(%[data]), %%zmm2 \n"
            "vmovntdqa 3*64(%[data]), %%zmm3 \n"
            "vmovntdq %%zmm0, 0(%[addr]) \n"
            "vmovntdq %%zmm1, 1*64(%[addr]) \n"
            "vmovntdq %%zmm2, 2*64(%[addr]) \n"
            "vmovntdq %%zmm3, 3*64(%[addr]) \n"
            :
            : [addr] "r" (dst), [data] "r" (src)
            : "%zmm0", "%zmm1", "%zmm2", "%zmm3"
            );
}

void memcpy_nt_512(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa (%[data]), %%zmm0 \n"
            "vmovntdqa 1*64(%[data]), %%zmm1 \n"
            "vmovntdqa 2*64(%[data]), %%zmm2 \n"
            "vmovntdqa 3*64(%[data]), %%zmm3 \n"
            "vmovntdqa 4*64(%[data]), %%zmm4 \n"
            "vmovntdqa 5*64(%[data]), %%zmm5 \n"
            "vmovntdqa 6*64(%[data]), %%zmm6 \n"
            "vmovntdqa 7*64(%[data]), %%zmm7 \n"
            "vmovntdq %%zmm0, 0*64(%[addr]) \n"
            "vmovntdq %%zmm1, 1*64(%[addr]) \n"
            "vmovntdq %%zmm2, 2*64(%[addr]) \n"
            "vmovntdq %%zmm3, 3*64(%[addr]) \n"
            "vmovntdq %%zmm4, 4*64(%[addr]) \n"
            "vmovntdq %%zmm5, 5*64(%[addr]) \n"
            "vmovntdq %%zmm6, 6*64(%[addr]) \n"
            "vmovntdq %%zmm7, 7*64(%[addr]) \n"
            :
            : [addr] "r" (dst), [data] "r" (src)
            : "%zmm0", "%zmm1", "%zmm2", "%zmm3",
            "%zmm4", "%zmm5", "%zmm6", "%zmm7"
            );
}

void memcpy_nt_1024(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa (%[data]), %%zmm0 \n"
            "vmovntdqa 1*64(%[data]), %%zmm1 \n"
            "vmovntdqa 2*64(%[data]), %%zmm2 \n"
            "vmovntdqa 3*64(%[data]), %%zmm3 \n"
            "vmovntdqa 4*64(%[data]), %%zmm4 \n"
            "vmovntdqa 5*64(%[data]), %%zmm5 \n"
            "vmovntdqa 6*64(%[data]), %%zmm6 \n"
            "vmovntdqa 7*64(%[data]), %%zmm7 \n"
            "vmovntdqa 8*64(%[data]), %%zmm8 \n"
            "vmovntdqa 9*64(%[data]), %%zmm9 \n"
            "vmovntdqa 10*64(%[data]), %%zmm10 \n"
            "vmovntdqa 11*64(%[data]), %%zmm11 \n"
            "vmovntdqa 12*64(%[data]), %%zmm12 \n"
            "vmovntdqa 13*64(%[data]), %%zmm13 \n"
            "vmovntdqa 14*64(%[data]), %%zmm14 \n"
            "vmovntdqa 15*64(%[data]), %%zmm15 \n"
            "vmovntdq %%zmm0, 0*64(%[addr]) \n"
            "vmovntdq %%zmm1, 1*64(%[addr]) \n"
            "vmovntdq %%zmm2, 2*64(%[addr]) \n"
            "vmovntdq %%zmm3, 3*64(%[addr]) \n"
            "vmovntdq %%zmm4, 4*64(%[addr]) \n"
            "vmovntdq %%zmm5, 5*64(%[addr]) \n"
            "vmovntdq %%zmm6, 6*64(%[addr]) \n"
            "vmovntdq %%zmm7, 7*64(%[addr]) \n"
            "vmovntdq %%zmm8, 8*64(%[addr]) \n"
            "vmovntdq %%zmm9, 9*64(%[addr]) \n"
            "vmovntdq %%zmm10, 10*64(%[addr]) \n"
            "vmovntdq %%zmm11, 11*64(%[addr]) \n"
            "vmovntdq %%zmm12, 12*64(%[addr]) \n"
            "vmovntdq %%zmm13, 13*64(%[addr]) \n"
            "vmovntdq %%zmm14, 14*64(%[addr]) \n"
            "vmovntdq %%zmm15, 15*64(%[addr]) \n"
            :
            : [addr] "r" (dst), [data] "r" (src)
            : "%zmm0", "%zmm1", "%zmm2", "%zmm3",
            "%zmm4", "%zmm5", "%zmm6", "%zmm7",
            "%zmm8", "%zmm9", "%zmm10", "%zmm11",
            "%zmm12", "%zmm13", "%zmm14", "%zmm15"
            );
}

void memcpy_nt_2048(void *dst, void *src) {
    __asm__ volatile(
            "vmovntdqa (%[data]), %%zmm0 \n"
            "vmovntdqa 1*64(%[data]), %%zmm1 \n"
            "vmovntdqa 2*64(%[data]), %%zmm2 \n"
            "vmovntdqa 3*64(%[data]), %%zmm3 \n"
            "vmovntdqa 4*64(%[data]), %%zmm4 \n"
            "vmovntdqa 5*64(%[data]), %%zmm5 \n"
            "vmovntdqa 6*64(%[data]), %%zmm6 \n"
            "vmovntdqa 7*64(%[data]), %%zmm7 \n"
            "vmovntdqa 8*64(%[data]), %%zmm8 \n"
            "vmovntdqa 9*64(%[data]), %%zmm9 \n"
            "vmovntdqa 10*64(%[data]), %%zmm10 \n"
            "vmovntdqa 11*64(%[data]), %%zmm11 \n"
            "vmovntdqa 12*64(%[data]), %%zmm12 \n"
            "vmovntdqa 13*64(%[data]), %%zmm13 \n"
            "vmovntdqa 14*64(%[data]), %%zmm14 \n"
            "vmovntdqa 15*64(%[data]), %%zmm15 \n"
            "vmovntdqa 16*64(%[data]), %%zmm16 \n"
            "vmovntdqa 17*64(%[data]), %%zmm17 \n"
            "vmovntdqa 18*64(%[data]), %%zmm18 \n"
            "vmovntdqa 19*64(%[data]), %%zmm19 \n"
            "vmovntdqa 20*64(%[data]), %%zmm20 \n"
            "vmovntdqa 21*64(%[data]), %%zmm21 \n"
            "vmovntdqa 22*64(%[data]), %%zmm22 \n"
            "vmovntdqa 23*64(%[data]), %%zmm23 \n"
            "vmovntdqa 24*64(%[data]), %%zmm24 \n"
            "vmovntdqa 25*64(%[data]), %%zmm25 \n"
            "vmovntdqa 26*64(%[data]), %%zmm26 \n"
            "vmovntdqa 27*64(%[data]), %%zmm27 \n"
            "vmovntdqa 28*64(%[data]), %%zmm28 \n"
            "vmovntdqa 29*64(%[data]), %%zmm29 \n"
            "vmovntdqa 30*64(%[data]), %%zmm30 \n"
            "vmovntdqa 31*64(%[data]), %%zmm31 \n"
            "vmovntdq %%zmm0, 0*64(%[addr]) \n"
            "vmovntdq %%zmm1, 1*64(%[addr]) \n"
            "vmovntdq %%zmm2, 2*64(%[addr]) \n"
            "vmovntdq %%zmm3, 3*64(%[addr]) \n"
            "vmovntdq %%zmm4, 4*64(%[addr]) \n"
            "vmovntdq %%zmm5, 5*64(%[addr]) \n"
            "vmovntdq %%zmm6, 6*64(%[addr]) \n"
            "vmovntdq %%zmm7, 7*64(%[addr]) \n"
            "vmovntdq %%zmm8, 8*64(%[addr]) \n"
            "vmovntdq %%zmm9, 9*64(%[addr]) \n"
            "vmovntdq %%zmm10, 10*64(%[addr]) \n"
            "vmovntdq %%zmm11, 11*64(%[addr]) \n"
            "vmovntdq %%zmm12, 12*64(%[addr]) \n"
            "vmovntdq %%zmm13, 13*64(%[addr]) \n"
            "vmovntdq %%zmm14, 14*64(%[addr]) \n"
            "vmovntdq %%zmm15, 15*64(%[addr]) \n"
            "vmovntdq %%zmm16, 16*64(%[addr]) \n"
            "vmovntdq %%zmm17, 17*64(%[addr]) \n"
            "vmovntdq %%zmm18, 18*64(%[addr]) \n"
            "vmovntdq %%zmm19, 19*64(%[addr]) \n"
            "vmovntdq %%zmm20, 20*64(%[addr]) \n"
            "vmovntdq %%zmm21, 21*64(%[addr]) \n"
            "vmovntdq %%zmm22, 22*64(%[addr]) \n"
            "vmovntdq %%zmm23, 23*64(%[addr]) \n"
            "vmovntdq %%zmm24, 24*64(%[addr]) \n"
            "vmovntdq %%zmm25, 25*64(%[addr]) \n"
            "vmovntdq %%zmm26, 26*64(%[addr]) \n"
            "vmovntdq %%zmm27, 27*64(%[addr]) \n"
            "vmovntdq %%zmm28, 28*64(%[addr]) \n"
            "vmovntdq %%zmm29, 29*64(%[addr]) \n"
            "vmovntdq %%zmm30, 30*64(%[addr]) \n"
            "vmovntdq %%zmm31, 31*64(%[addr]) \n"
            :
            : [addr] "r" (dst), [data] "r" (src)
            : "%zmm0", "%zmm1", "%zmm2", "%zmm3",
            "%zmm4", "%zmm5", "%zmm6", "%zmm7",
            "%zmm8", "%zmm9", "%zmm10", "%zmm11",
            "%zmm12", "%zmm13", "%zmm14", "%zmm15",
            "%zmm16", "%zmm17", "%zmm18", "%zmm19",
            "%zmm20", "%zmm21", "%zmm22", "%zmm23",
            "%zmm24", "%zmm25", "%zmm26", "%zmm27",
            "%zmm28", "%zmm29", "%zmm30", "%zmm31"
            );
}

#endif //ENABLE_NVM