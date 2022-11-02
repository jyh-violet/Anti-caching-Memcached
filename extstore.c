/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

// FIXME: config.h?
#include <stdint.h>
#include <stdbool.h>
// end FIXME
#include <stdlib.h>
#include <limits.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "extstore.h"
#include "config.h"

// TODO: better if an init option turns this on/off.
#ifdef EXTSTORE_DEBUG
#define E_DEBUG(...) \
    do { \
        fprintf(stderr, __VA_ARGS__); \
    } while (0)
#else
#define E_DEBUG(...)
#endif

//#ifndef DRAM_TEST
//#define DRAM_TEST
//#endif

#define STAT_L(e) pthread_mutex_lock(&e->stats_mutex);
#define STAT_UL(e) pthread_mutex_unlock(&e->stats_mutex);
#define STAT_INCR(e, stat, amount) { \
    pthread_mutex_lock(&e->stats_mutex); \
    e->stats.stat += amount; \
    pthread_mutex_unlock(&e->stats_mutex); \
}

#define STAT_DECR(e, stat, amount) { \
    pthread_mutex_lock(&e->stats_mutex); \
    e->stats.stat -= amount; \
    pthread_mutex_unlock(&e->stats_mutex); \
}

typedef struct __store_wbuf {
    struct __store_wbuf *next;
    char *buf;
    char *buf_pos;
    unsigned int free;
    unsigned int size;
    unsigned int offset; /* offset into page this write starts at */
    bool full; /* done writing to this page */
    bool flushed; /* whether wbuf has been flushed to disk */
} _store_wbuf;

typedef struct _store_page {
    pthread_mutex_t mutex; /* Need to be held for most operations */
    uint64_t obj_count; /* _delete can decrease post-closing */
    uint64_t bytes_used; /* _delete can decrease post-closing */
    uint64_t offset; /* starting address of page within fd */
    unsigned int version;
    unsigned int refcount;
    unsigned int allocated;
    unsigned int written; /* item offsets can be past written if wbuf not flushed */
    unsigned int bucket; /* which bucket the page is linked into */
    unsigned int free_bucket; /* which bucket this page returns to when freed */
    int fd;
    unsigned short id;
    bool active; /* actively being written to */
    bool closed; /* closed and draining before free */
    bool free; /* on freelist */
    _store_wbuf *wbuf; /* currently active wbuf from the stack */
    struct _store_page *next;
#ifdef ENABLE_NVM
    char* wbufSwaped;
    char* wbufWaitSwap;
    swap_buf **wbufs;
#endif
#ifdef DRAM_TEST
    char* data;
#endif
} store_page;

typedef struct store_engine store_engine;
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    obj_io *queue;
    obj_io *queue_tail;
    store_engine *e;
    unsigned int depth; // queue depth
} store_io_thread;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    store_engine *e;
} store_maint_thread;

struct store_engine {
    pthread_mutex_t mutex; /* covers internal stacks and variables */
    pthread_mutex_t bucket_mutex[64]; /* covers internal stacks and variables */
    store_page *pages; /* directly addressable page list */
    _store_wbuf *wbuf_stack; /* wbuf freelist */
    obj_io *io_stack; /* IO's to use with submitting wbuf's */
    store_io_thread *io_threads;
    store_maint_thread *maint_thread;
    store_page *page_freelist;
    store_page **page_buckets; /* stack of pages currently allocated to each bucket */
    store_page **free_page_buckets; /* stack of use-case isolated free pages */
    size_t page_size;
#ifdef ENABLE_NVM
    swap_thread * swapThread;
    obj_io* read_io_stack;
    size_t wbuf_size;
    buf_swap_fn bufSwapFn;
    item_swap itemSwap;
#endif
    unsigned int version; /* global version counter */
    unsigned int last_io_thread; /* round robin the IO threads */
    unsigned int io_threadcount; /* count of IO threads */
    unsigned int swap_threadcount;
    unsigned int page_count;
    unsigned int page_free; /* unallocated pages */
    unsigned int page_bucketcount; /* count of potential page buckets */
    unsigned int free_page_bucketcount; /* count of free page buckets */
    unsigned int io_depth; /* FIXME: Might cache into thr struct */

    pthread_mutex_t stats_mutex;
    struct extstore_stats stats;


};
#define ASY_READ

__thread char* io_wubf_static =NULL;
static _store_wbuf *wbuf_new(size_t size) {
    _store_wbuf *b = calloc(1, sizeof(_store_wbuf));
    if (b == NULL)
        return NULL;
    b->buf = calloc(size, sizeof(char));
    if (b->buf == NULL) {
        free(b);
        return NULL;
    }
    b->buf_pos = b->buf;
    b->free = size;
    b->size = size;
    return b;
}

char *get_io_wubf_static(void* ptr){
    store_engine *e  = (store_engine*)ptr;
    if (io_wubf_static == NULL){
        io_wubf_static = malloc(e->wbuf_size);
    }
    return io_wubf_static;
}

static store_io_thread *_get_io_thread(store_engine *e) {
    int tid = -1;
    long long int low = LLONG_MAX;
#ifndef ENABLE_NVM
    pthread_mutex_lock(&e->mutex);
#endif
    // find smallest queue. ignoring lock since being wrong isn't fatal.
    // TODO: if average queue depth can be quickly tracked, can break as soon
    // as we see a thread that's less than average, and start from last_io_thread
    for (int x = 0; x < e->io_threadcount; x++) {
        if (e->io_threads[x].depth == 0) {
            tid = x;
            break;
        } else if (e->io_threads[x].depth < low) {
                tid = x;
            low = e->io_threads[x].depth;
        }
    }
#ifndef ENABLE_NVM
    pthread_mutex_unlock(&e->mutex);
#endif
    return &e->io_threads[tid];
}

static swap_thread *_get_wap_thread(store_engine *e) {
    int tid = 0;
    long long int low = LLONG_MAX;
//    pthread_mutex_lock(&e->mutex);
    // find smallest queue. ignoring lock since being wrong isn't fatal.
    // TODO: if average queue depth can be quickly tracked, can break as soon
    // as we see a thread that's less than average, and start from last_io_thread
    for (int x = 0; x < e->swap_threadcount; x++) {
        if(e->swapThread[x].stop == 1){
            continue;
        }
        if (e->swapThread[x].depth == 0) {
            tid = x;
            break;
        } else if (e->swapThread[x].depth < low) {
            tid = x;
            low = e->swapThread[x].depth;
        }
    }
//    pthread_mutex_unlock(&e->mutex);

    return &e->swapThread[tid];
}

static uint64_t _next_version(store_engine *e) {
    return e->version++;
}

#ifndef ENABLE_NVM
static void *extstore_io_thread(void *arg);
#endif
static void *extstore_maint_thread(void *arg);

/* Copies stats internal to engine and computes any derived values */
void extstore_get_stats(void *ptr, struct extstore_stats *st) {
    store_engine *e = (store_engine *)ptr;
    STAT_L(e);
    memcpy(st, &e->stats, sizeof(struct extstore_stats));
    STAT_UL(e);

    // grab pages_free/pages_used
    pthread_mutex_lock(&e->mutex);
    st->pages_free = e->page_free;
    st->pages_used = e->page_count - e->page_free;
    pthread_mutex_unlock(&e->mutex);
    st->io_queue = 0;
    for (int x = 0; x < e->io_threadcount; x++) {
        pthread_mutex_lock(&e->io_threads[x].mutex);
        st->io_queue += e->io_threads[x].depth;
        pthread_mutex_unlock(&e->io_threads[x].mutex);
    }
    // calculate bytes_fragmented.
    // note that open and yet-filled pages count against fragmentation.
    st->bytes_fragmented = st->pages_used * e->page_size -
        st->bytes_used;
}

void extstore_get_page_data(void *ptr, struct extstore_stats *st) {
    store_engine *e = (store_engine *)ptr;
    STAT_L(e);
    memcpy(st->page_data, e->stats.page_data,
            sizeof(struct extstore_page_data) * e->page_count);
    STAT_UL(e);
}

const char *extstore_err(enum extstore_res res) {
    const char *rv = "unknown error";
    switch (res) {
        case EXTSTORE_INIT_BAD_WBUF_SIZE:
            rv = "page_size must be divisible by wbuf_size";
            break;
        case EXTSTORE_INIT_NEED_MORE_WBUF:
            rv = "wbuf_count must be >= page_buckets";
            break;
        case EXTSTORE_INIT_NEED_MORE_BUCKETS:
            rv = "page_buckets must be > 0";
            break;
        case EXTSTORE_INIT_PAGE_WBUF_ALIGNMENT:
            rv = "page_size and wbuf_size must be divisible by 1024*1024*2";
            break;
        case EXTSTORE_INIT_TOO_MANY_PAGES:
            rv = "page_count must total to < 65536. Increase page_size or lower path sizes";
            break;
        case EXTSTORE_INIT_OOM:
            rv = "failed calloc for engine";
            break;
        case EXTSTORE_INIT_OPEN_FAIL:
            rv = "failed to open file";
            break;
        case EXTSTORE_INIT_THREAD_FAIL:
            break;
    }
    return rv;
}

// TODO: #define's for DEFAULT_BUCKET, FREE_VERSION, etc
void *extstore_init(struct extstore_conf_file *fh, struct extstore_conf *cf,
        enum extstore_res *res) {
    int i;
    struct extstore_conf_file *f = NULL;
    pthread_t thread;

    if (cf->page_size % cf->wbuf_size != 0) {
        *res = EXTSTORE_INIT_BAD_WBUF_SIZE;
        return NULL;
    }
    // Should ensure at least one write buffer per potential page
    if (cf->page_buckets > cf->wbuf_count) {
        *res = EXTSTORE_INIT_NEED_MORE_WBUF;
        return NULL;
    }
    if (cf->page_buckets < 1) {
        *res = EXTSTORE_INIT_NEED_MORE_BUCKETS;
        return NULL;
    }

    // TODO: More intelligence around alignment of flash erasure block sizes
#ifdef ENABLE_NVM
    if (cf->page_size % (1024 * 2) != 0 ||
    cf->wbuf_size % (1024 * 2) != 0) {
        *res = EXTSTORE_INIT_PAGE_WBUF_ALIGNMENT;
        return NULL;
    }
#else
    if (cf->page_size % (1024 * 1024 * 2) != 0 ||
    cf->wbuf_size % (1024 * 1024 * 2) != 0) {
        *res = EXTSTORE_INIT_PAGE_WBUF_ALIGNMENT;
        return NULL;
    }
#endif



    store_engine *e = calloc(1, sizeof(store_engine));
    if (e == NULL) {
        *res = EXTSTORE_INIT_OOM;
        return NULL;
    }

    e->page_size = cf->page_size;
    uint64_t temp_page_count = 0;
    for (f = fh; f != NULL; f = f->next) {
        f->fd = open(f->file, O_RDWR | O_CREAT, 0644);
        if (f->fd < 0) {
            *res = EXTSTORE_INIT_OPEN_FAIL;
#ifdef EXTSTORE_DEBUG
            perror("extstore open");
#endif
            free(e);
            return NULL;
        }
        // use an fcntl lock to help avoid double starting.
        struct flock lock;
        lock.l_type = F_WRLCK;
        lock.l_start = 0;
        lock.l_whence = SEEK_SET;
        lock.l_len = 0;
        if (fcntl(f->fd, F_SETLK, &lock) < 0) {
            *res = EXTSTORE_INIT_OPEN_FAIL;
            free(e);
            return NULL;
        }
        if (ftruncate(f->fd, 0) < 0) {
            *res = EXTSTORE_INIT_OPEN_FAIL;
            free(e);
            return NULL;
        }

        temp_page_count += f->page_count;
        f->offset = 0;
    }

    if (temp_page_count >= UINT16_MAX) {
        *res = EXTSTORE_INIT_TOO_MANY_PAGES;
        free(e);
        return NULL;
    }
    e->page_count = temp_page_count;

    fprintf(stderr, "page_count:%d\n", e->page_count);
    e->pages = calloc(e->page_count, sizeof(store_page));
    if (e->pages == NULL) {
        *res = EXTSTORE_INIT_OOM;
        // FIXME: loop-close. make error label
        free(e);
        return NULL;
    }


#ifdef ENABLE_NVM
    e->wbuf_size = cf->wbuf_size;
    e->bufSwapFn = cf->bufSwapFn;
    e->itemSwap = cf->itemSwap;
#endif

    // interleave the pages between devices
    f = NULL; // start at the first device.
    for (i = 0; i < e->page_count; i++) {
        // find next device with available pages
        while (1) {
            // restart the loop
            if (f == NULL || f->next == NULL) {
                f = fh;
            } else {
                f = f->next;
            }
            if (f->page_count) {
                f->page_count--;
                break;
            }
        }
        pthread_mutex_init(&e->pages[i].mutex, NULL);
        e->pages[i].id = i;
        e->pages[i].fd = f->fd;
        e->pages[i].free_bucket = f->free_bucket;
        e->pages[i].offset = f->offset;
        e->pages[i].free = true;
        f->offset += e->page_size;
#ifdef ENABLE_NVM
        e->pages[i].wbufSwaped = (char *)malloc( e->page_size / e->wbuf_size / 8);
        memset(e->pages[i].wbufSwaped, 0, e->page_size / e->wbuf_size / 8);
        e->pages[i].wbufs = (swap_buf **) malloc(e->page_size / e->wbuf_size * sizeof (void *));
        memset(e->pages[i].wbufs, 0, e->page_size / e->wbuf_size * sizeof (void *));
#endif
#ifdef DRAM_TEST
        e->pages[i].data = malloc(e->page_size);
        memset(e->pages[i].data, 0, e->page_size);
//        fprintf(stderr, "init page %d\n", i);
#endif
    }

    // free page buckets allows the app to organize devices by use case
    e->free_page_buckets = calloc(cf->page_buckets, sizeof(store_page *));
    e->page_bucketcount = cf->page_buckets;

    for (i = e->page_count-1; i > 0; i--) {
        e->page_free++;
        if (e->pages[i].free_bucket == 0) {
            e->pages[i].next = e->page_freelist;
            e->page_freelist = &e->pages[i];
        } else {
            int fb = e->pages[i].free_bucket;
            e->pages[i].next = e->free_page_buckets[fb];
            e->free_page_buckets[fb] = &e->pages[i];
        }
    }

    // 0 is magic "page is freed" version
    e->version = 1;

    // scratch data for stats. TODO: malloc failure handle
    e->stats.page_data =
        calloc(e->page_count, sizeof(struct extstore_page_data));
    e->stats.page_count = e->page_count;
    e->stats.page_size = e->page_size;

    // page buckets lazily have pages assigned into them
    e->page_buckets = calloc(cf->page_buckets, sizeof(store_page *));
    e->page_bucketcount = cf->page_buckets;

    // allocate write buffers
    // also IO's to use for shipping to IO thread
    for (i = 0; i < cf->wbuf_count; i++) {
        _store_wbuf *w = wbuf_new(cf->wbuf_size);
        obj_io *io = calloc(1, sizeof(obj_io));
#ifdef ENABLE_NVM
        obj_io *read_io = calloc(1, sizeof(obj_io));
        read_io->next = e->read_io_stack;
        e->read_io_stack = read_io;
#endif
        /* TODO: on error, loop again and free stack. */
        w->next = e->wbuf_stack;
        e->wbuf_stack = w;
        io->next = e->io_stack;
        e->io_stack = io;
    }

    pthread_mutex_init(&e->mutex, NULL);
    pthread_mutex_init(&e->stats_mutex, NULL);

    e->io_depth = cf->io_depth;

    // spawn threads
    e->io_threads = calloc(cf->io_threadcount, sizeof(store_io_thread));
#ifndef ENABLE_NVM
    for (i = 0; i < cf->io_threadcount; i++) {
        pthread_mutex_init(&e->io_threads[i].mutex, NULL);
        pthread_cond_init(&e->io_threads[i].cond, NULL);
        e->io_threads[i].e = e;
        // FIXME: error handling
        pthread_create(&thread, NULL, extstore_io_thread, &e->io_threads[i]);
    }
#endif
    e->io_threadcount = cf->io_threadcount;

    e->maint_thread = calloc(1, sizeof(store_maint_thread));
    e->maint_thread->e = e;
    // FIXME: error handling
    pthread_mutex_init(&e->maint_thread->mutex, NULL);
    pthread_cond_init(&e->maint_thread->cond, NULL);
    pthread_create(&thread, NULL, extstore_maint_thread, e->maint_thread);

#ifdef ENABLE_NVM
    e->swapThread = calloc(cf->swap_threadcount, sizeof(swap_thread));
    for (i = 0; i < cf->swap_threadcount; i++) {
        pthread_mutex_init(&e->swapThread[i].mutex, NULL);
        pthread_mutex_init(&e->bucket_mutex[i], NULL);
        pthread_cond_init(&e->swapThread[i].cond, NULL);
        e->swapThread[i].stop = 1;

#if   defined(ASY_READ)
        int w_count = 10240;
#else
        int w_count = 256;
#endif
        for (int j = 0; j < cf->wbuf_count * w_count; ++j) {
            swap_buf * buf = malloc(sizeof (swap_buf));
#if  !defined(ASY_READ)
            buf->buf = malloc(e->wbuf_size);
#endif
            buf->next = e->swapThread[i].swapBufStack;
            e->swapThread[i].swapBufStack = buf;
        }
        // FIXME: error handling
        e->swapThread[i].engine = e;
        e->swapThread[i].threadId = i;
        pthread_create(&thread, NULL, _swap_thread, &e->swapThread[i]);
    }

    io_wubf_static = malloc(e->wbuf_size);
#endif

    extstore_run_maint(e);

    return (void *)e;
}

void extstore_run_maint(void *ptr) {
    store_engine *e = (store_engine *)ptr;
    pthread_cond_signal(&e->maint_thread->cond);
}

// call with *e locked
static store_page *_allocate_page(store_engine *e, unsigned int bucket,
        unsigned int free_bucket) {
    assert(!e->page_buckets[bucket] || e->page_buckets[bucket]->allocated == e->page_size);
    store_page *tmp = NULL;
    // if a specific free bucket was requested, check there first
    if (free_bucket != 0 && e->free_page_buckets[free_bucket] != NULL) {
        assert(e->page_free > 0);
        tmp = e->free_page_buckets[free_bucket];
        e->free_page_buckets[free_bucket] = tmp->next;
    }
    // failing that, try the global list.
    if (tmp == NULL && e->page_freelist != NULL) {
        tmp = e->page_freelist;
        e->page_freelist = tmp->next;
    }
    E_DEBUG("EXTSTORE: allocating new page\n");
    // page_freelist can be empty if the only free pages are specialized and
    // we didn't just request one.
    if (e->page_free > 0 && tmp != NULL) {
        tmp->next = e->page_buckets[bucket];
        e->page_buckets[bucket] = tmp;
        tmp->active = true;
        tmp->free = false;
        tmp->closed = false;
        tmp->version = _next_version(e);
        tmp->bucket = bucket;
        e->page_free--;
        STAT_INCR(e, page_allocs, 1);
    } else {
        extstore_run_maint(e);
    }
    if (tmp)
        E_DEBUG("EXTSTORE: got page %u\n", tmp->id);
    return tmp;
}

// call with *p locked. locks *e
static void _allocate_wbuf(store_engine *e, store_page *p) {
    _store_wbuf *wbuf = NULL;
    assert(!p->wbuf);
    pthread_mutex_lock(&e->mutex);
    if (e->wbuf_stack) {
        wbuf = e->wbuf_stack;
        e->wbuf_stack = wbuf->next;
        wbuf->next = 0;
    }
    pthread_mutex_unlock(&e->mutex);
    if (wbuf) {
#ifdef ENABLE_NVM
        pthread_mutex_lock(&p->mutex);
        if(p->wbuf || p->allocated >= e->page_size){
            pthread_mutex_unlock(&p->mutex);
            pthread_mutex_lock(&e->mutex);
            wbuf->next = e->wbuf_stack;
            e->wbuf_stack = wbuf;
            pthread_mutex_unlock(&e->mutex);
            return;
        }
#endif
        wbuf->offset = p->allocated;
        p->allocated += wbuf->size;
        wbuf->free = wbuf->size;
        wbuf->buf_pos = wbuf->buf;
        wbuf->full = false;
        wbuf->flushed = false;

        p->wbuf = wbuf;
#ifdef ENABLE_NVM
        pthread_mutex_unlock(&p->mutex);
#endif
    } else{
        fprintf(stderr, "_allocate_wbuf fail, page:%d\n", p->id);
    }
}

#ifndef ENABLE_NVM
/* callback after wbuf is flushed. can only remove wbuf's from the head onward
 * if successfully flushed, which complicates this routine. each callback
 * attempts to free the wbuf stack, which is finally done when the head wbuf's
 * callback happens.
 * It's rare flushes would happen out of order.
 */
static void _wbuf_cb(void *ep, obj_io *io, int ret) {
    store_engine *e = (store_engine *)ep;
    store_page *p = &e->pages[io->page_id];
    _store_wbuf *w = (_store_wbuf *) io->data;

    // TODO: Examine return code. Not entirely sure how to handle errors.
    // Naive first-pass should probably cause the page to close/free.
    w->flushed = true;
    pthread_mutex_lock(&p->mutex);
    assert(p->wbuf != NULL && p->wbuf == w);
    assert(p->written == w->offset);
    p->written += w->size;
#ifndef ENABLE_NVM
    p->wbuf = NULL;
#endif

    if (p->written == e->page_size)
        p->active = false;

//    fprintf(stderr, "(%ld), return wbuf:%p, io:%p, active:%d\n",
//            pthread_self(), (void *)w, (void *) io, p->active);
    // return the wbuf
    pthread_mutex_lock(&e->mutex);
    w->next = e->wbuf_stack;
    e->wbuf_stack = w;
    // also return the IO we just used.
    io->next = e->io_stack;
    e->io_stack = io;
    pthread_mutex_unlock(&e->mutex);
    pthread_mutex_unlock(&p->mutex);
}

/* Wraps pages current wbuf in an io and submits to IO thread.
 * Called with p locked, locks e.
 */
static void _submit_wbuf(store_engine *e, store_page *p) {

#ifdef ENABLE_NVM
    _store_wbuf *w =  p->wbuf;
#ifdef DRAM_TEST
    memcpy(p->data + w->offset,w->buf, w->size);
#else
        // FIXME: Should hold refcount during write. doesn't
        // currently matter since page can't free while active.
        ret = pwrite(p->fd, w->buf, w->size, p->offset + w->offset);
#endif
        pthread_mutex_lock(&p->mutex);
        assert(p->wbuf != NULL && p->wbuf == w);
        assert(p->written == w->offset);
        p->written += w->size;

        if (p->written == e->page_size)
            p->active = false;

        //    fprintf(stderr, "(%ld), return wbuf:%p, io:%p, active:%d\n",
        //            pthread_self(), (void *)w, (void *) io, p->active);
        // return the wbuf
        pthread_mutex_lock(&e->mutex);
        w->next = e->wbuf_stack;
        e->wbuf_stack = w;
        pthread_mutex_unlock(&e->mutex);
        pthread_mutex_unlock(&p->mutex);
#else
        _store_wbuf *w;
        pthread_mutex_lock(&e->mutex);
        obj_io *io = e->io_stack;
        e->io_stack = io->next;
        pthread_mutex_unlock(&e->mutex);
        w = p->wbuf;

        // zero out the end of the wbuf to allow blind readback of data.
        memset(w->buf + (w->size - w->free), 0, w->free);

        io->next = NULL;
        io->mode = OBJ_IO_WRITE;
        io->page_id = p->id;
        io->data = w;
        io->offset = w->offset;
        io->len = w->size;
        io->buf = w->buf;
        io->cb = _wbuf_cb;
    extstore_submit(e, io);
#endif
}
#endif

#ifdef ENABLE_NVM

static void _submit_wbuf_nvm(store_engine *e, store_page *p) {

#ifdef DRAM_TEST
    memcpy(p->data + p->wbuf->offset,p->wbuf->buf, p->wbuf->size);
#else
    // FIXME: Should hold refcount during write. doesn't
    // currently matter since page can't free while active.
    pthread_mutex_unlock(&p->mutex);
    int ret = pwrite(p->fd, p->wbuf->buf, p->wbuf->size, p->offset + p->wbuf->offset);
    pthread_mutex_lock(&p->mutex);
    if(ret < 0){
        fprintf(stderr, "_submit_wbuf_nvm write error\n");
        exit(0);
    }
#endif
    p->written += p->wbuf->size;

    if (p->written == e->page_size){
        p->active = false;
        // return the wbuf
        _store_wbuf *wbuf = p->wbuf;
        p->wbuf = NULL;
        pthread_mutex_lock(&e->mutex);
        wbuf->next = e->wbuf_stack;
        e->wbuf_stack = wbuf;
        pthread_mutex_unlock(&e->mutex);
    } else{
        p->wbuf->offset = p->allocated;
        p->allocated += p->wbuf->size;
        p->wbuf->free = p->wbuf->size;
        p->wbuf->buf_pos = p->wbuf->buf;
        p->wbuf->full = false;
        p->wbuf->flushed = false;
    }
}
#endif


/* engine write function; takes engine, item_io.
 * fast fail if no available write buffer (flushing)
 * lock engine context, find active page, unlock
 * if page full, submit page/buffer to io thread.
 *
 * write is designed to be flaky; if page full, caller must try again to get
 * new page. best if used from a background thread that can harmlessly retry.
 */


#ifdef ENABLE_NVM

int extstore_write_request(void *ptr, unsigned int bucket,
        unsigned int free_bucket, obj_io *io) {
    unsigned  int local_bucket = bucket + storage_write_threadId;
    store_engine *e = (store_engine *)ptr;
    store_page *p;
    int ret = -1;
    if (local_bucket >= e->page_bucketcount)
        return ret;

    p = e->page_buckets[local_bucket];
    if (!p) {
        pthread_mutex_lock(&e->mutex);
        p = _allocate_page(e, local_bucket, free_bucket);
        pthread_mutex_unlock(&e->mutex);
        if(p){
            fprintf(stderr, "allocate new page:%d\n", p->id);
        }
    }
    if (p == NULL){
        fprintf(stderr, "allocate page error\n");
        return ret;
    }
    if (!p->wbuf && p->allocated < e->page_size) {
        _allocate_wbuf(e, p);
    }
    pthread_mutex_lock(&p->mutex);

    // FIXME: can't null out page_buckets!!!
    // page is full, clear bucket and retry later.
    if (!p->active ||
            ((!p->wbuf) && p->allocated >= e->page_size)) {
//        fprintf(stderr, "page:%d wbuf:%p is full\n", p->id, (void *)p->wbuf);
        pthread_mutex_unlock(&p->mutex);
        pthread_mutex_lock(&e->mutex);
        if(p == e->page_buckets[local_bucket]){
            _allocate_page(e, local_bucket, free_bucket);
            if(false)
                fprintf(stderr, "bucket:%d allocate new page:%d, free_bucket:%d, p->wbuf:%p\n",
                    local_bucket, p->id, free_bucket, (void *)p->wbuf);
        }
        pthread_mutex_unlock(&e->mutex);
        return ret;
    }
    // if io won't fit, submit IO for wbuf and find new one.
    if (p->wbuf && p->wbuf->free < io->len ) {
        _submit_wbuf_nvm(e,p);
    }
    if(!p->wbuf || !p->active){
        pthread_mutex_unlock(&p->mutex);
        return ret;
    }
    // hand over buffer for caller to copy into
    // leaves p locked.
    if (p->wbuf && p->wbuf->free >= io->len) {
        io->buf = p->wbuf->buf_pos;
        io->page_id = p->id;
        // make sure only one thread writing the page
        pthread_mutex_unlock(&p->mutex);
        return 0;
    }

    pthread_mutex_unlock(&p->mutex);
    // p->written is incremented post-wbuf flush
    return ret;
}
#else
int extstore_write_request(void *ptr, unsigned int bucket,
                           unsigned int free_bucket, obj_io *io) {
    store_engine *e = (store_engine *)ptr;
    store_page *p;
    int ret = -1;
    if (bucket >= e->page_bucketcount)
        return ret;

    pthread_mutex_lock(&e->mutex);
    p = e->page_buckets[bucket];
    if (!p) {
        p = _allocate_page(e, bucket, free_bucket);
        if(p){
            fprintf(stderr, "allocate new page:%d\n", p->id);
        }
    }
    pthread_mutex_unlock(&e->mutex);
    if (p == NULL){
        fprintf(stderr, "allocate page error\n");
        return ret;
    }

    pthread_mutex_lock(&p->mutex);

    // FIXME: can't null out page_buckets!!!
    // page is full, clear bucket and retry later.
    if (!p->active ||
    ((!p->wbuf) && p->allocated >= e->page_size)) {
        //        fprintf(stderr, "page:%d wbuf:%p is full\n", p->id, (void *)p->wbuf);
        pthread_mutex_unlock(&p->mutex);
        pthread_mutex_lock(&e->mutex);
        if(p == e->page_buckets[bucket]){
            _allocate_page(e, bucket, free_bucket);
            fprintf(stderr, "allocate new page:%d\n", p->id);
        }
        pthread_mutex_unlock(&e->mutex);
        return ret;
    }
    // if io won't fit, submit IO for wbuf and find new one.
    if (p->wbuf && p->wbuf->free < io->len ) {
        _submit_wbuf(e, p);
    }

    if (!p->wbuf && p->allocated < e->page_size) {
        _allocate_wbuf(e, p);
    }

    // hand over buffer for caller to copy into
    // leaves p locked.
    if (p->wbuf && p->wbuf->free >= io->len) {
        io->buf = p->wbuf->buf_pos;
        io->page_id = p->id;
        return 0;
    }

    pthread_mutex_unlock(&p->mutex);
    // p->written is incremented post-wbuf flush
    return ret;
}
#endif

/* _must_ be called after a successful write_request.
 * fills the rest of io structure.
 */
void extstore_write(void *ptr, obj_io *io) {
    store_engine *e = (store_engine *)ptr;
    store_page *p = &e->pages[io->page_id];
#ifdef ENABLE_NVM
    pthread_mutex_lock(&p->mutex);
#endif
    io->offset = p->wbuf->offset + (p->wbuf->size - p->wbuf->free);
    io->page_version = p->version;
    p->wbuf->buf_pos += io->len;
    p->wbuf->free -= io->len;
    p->bytes_used += io->len;
    p->obj_count++;
#ifndef ENABLE_NVM
    STAT_L(e);
    e->stats.bytes_written += io->len;
    e->stats.bytes_used += io->len;
    e->stats.objects_written++;
    e->stats.objects_used++;
    STAT_UL(e);
#endif
    pthread_mutex_unlock(&p->mutex);
}

void resetPageWbufWaitSwap(void *ptr, int pageId, int off){
    store_engine *e = (store_engine *)ptr;
    store_page *p = &e->pages[pageId];
    int wbuf_index = off / e->wbuf_size;
    __sync_fetch_and_and(&p->wbufSwaped[wbuf_index / 8], ~(1L <<(wbuf_index % 8)));
}

/* Finds an attached wbuf that can satisfy the read.
 * Since wbufs can potentially be flushed to disk out of order, they are only
 * removed as the head of the list successfully flushes to disk.
 */
// call with *p locked
// FIXME: protect from reading past wbuf
static inline int _read_from_wbuf(store_page *p, obj_io *io) {
    _store_wbuf *wbuf = p->wbuf;
    assert(wbuf != NULL);
    assert(io->offset < p->written + wbuf->size);
    if (io->iov == NULL) {
        memcpy(io->buf, wbuf->buf + (io->offset - wbuf->offset), io->len);
    } else {
        int x;
        unsigned int off = io->offset - wbuf->offset;
        // need to loop fill iovecs
        for (x = 0; x < io->iovcnt; x++) {
            struct iovec *iov = &io->iov[x];
            memcpy(iov->iov_base, wbuf->buf + off, iov->iov_len);
            off += iov->iov_len;
        }
    }
#ifdef ENABLE_NVM
    int len = io->len;
    io->len = 0;
    return len;
#else
    return io->len;
#endif
}

int readWbuf(void *ptr, unsigned int page_id, swap_buf * buf, size_t size, unsigned  int  offset){
    store_engine *e = (store_engine *)ptr;
    store_page *p = &e->pages[page_id];
#ifdef DRAM_TEST
    memcpy(buf->buf, p->data + offset, size);
    int ret = size;
#else
    int ret = pread(p->fd, buf->buf, size, p->offset + offset);
    __sync_fetch_and_add(&SSDread, 1);
#endif
#ifndef TUPLE_SWAP
    p->wbufs[offset /size ] = buf;
#endif
    return ret;
}

void resetWbuf(void *ptr, unsigned int page_id,  unsigned  int  offset){
    store_engine *e = (store_engine *)ptr;
    store_page *p = &e->pages[page_id];
   int wbuf_index = offset /e->wbuf_size;
   p->wbufs[wbuf_index] = NULL;
   __sync_fetch_and_and(&p->wbufSwaped[wbuf_index / 8], ~(1L <<(wbuf_index % 8)));
}

/* engine submit function; takes engine, item_io stack.
 * lock io_thread context and add stack?
 * signal io thread to wake.
 * return success.
 */
int extstore_submit(void *ptr, obj_io *io) {
    store_engine *e = (store_engine *)ptr;

    unsigned int depth = 0;
    obj_io *tio = io;
    obj_io *tail = NULL;
    while (tio != NULL) {
        tail = tio; // keep updating potential tail.
        depth++;
        tio = tio->next;
    }

    store_io_thread *t = _get_io_thread(e);
    pthread_mutex_lock(&t->mutex);

    t->depth += depth;
    if (t->queue == NULL) {
        t->queue = io;
        t->queue_tail = tail;
    } else {
        // Have to put the *io stack at the end of current queue.
        assert(tail->next == NULL);
        assert(t->queue_tail->next == NULL);
        t->queue_tail->next = io;
        t->queue_tail = tail;
    }

    pthread_mutex_unlock(&t->mutex);

    //pthread_mutex_lock(&t->mutex);
    pthread_cond_signal(&t->cond);
    //pthread_mutex_unlock(&t->mutex);
    return 0;
}
int extstore_submit_opt(void *ptr, obj_io *io, bool no_swap) {
    store_engine *e = (store_engine *)ptr;

#ifdef ENABLE_NVM
    if(io->mode == OBJ_IO_READ){
#if defined(TUPLE_SWAP)
        int do_op = 1;
        int ret = 1;
        store_page *p = &e->pages[io->page_id];


        if (!p->free && !p->closed && p->version == io->page_version) {
            if (p->active && p->wbuf && io->offset >= p->wbuf->offset
            && io->offset < (p->wbuf->offset + e->wbuf_size)) {
                io->buf = get_io_wubf_static(e);
                pthread_mutex_lock(&p->mutex);
                if (p->active && p->wbuf && io->offset >= p->wbuf->offset
                && io->offset < (p->wbuf->offset + e->wbuf_size)) {
                    ret = _read_from_wbuf(p, io);
                    do_op = 0;
                }
                pthread_mutex_unlock(&p->mutex);
            } else {
                __sync_fetch_and_and(&p->refcount, 1);
            }
        } else {
            fprintf(stderr, "extstore_io_thread: p->free:%d, p->closed:%d, p->id:%d, p->version:%d, io->version:%d\n",
                    p->free, p->closed, p->id, p->version, io->page_version);
            do_op = 0;
            ret = -2; // TODO: enum in IO for status?
        }
        if (do_op) {
            swap_thread *t = _get_wap_thread(e);
//            if(pthread_mutex_trylock(&t->mutex) != 0){
//                return -1;
//            }
            pthread_mutex_lock(&t->mutex);
            swap_buf * buf = t->swapBufStack;
            if(buf != NULL){
                t->swapBufStack = buf->next;
                buf->next = NULL;
                pthread_mutex_unlock(&t->mutex);
#ifdef ASY_READ
                buf->mode = SWAP_ITEM_NEEDREAD;
                buf->page_id = p->id;
                buf->page_version = p->version;
                buf->offset = io->offset;
                buf->len = io->len;
                buf->buf = NULL;
#else
                if (readWbuf(e, p->id, buf, io->len,  io->offset) >0){
                    buf->mode = SWAP_ITME;
                    buf->page_id = p->id;
                    buf->page_version = p->version;
                    buf->offset = io->offset;
                    buf->len = io->len;
                    io->buf = get_io_wubf_static(e);
                    memcpy(io->buf, buf->buf, io->len);
                    io->len = 1;
//                    p->wbufs[wbuf_index] = buf;
                }
#endif
                pthread_mutex_lock(&t->mutex);
                t->depth += 1;
                if (t->queue == NULL) {
                    t->queue = buf;
                    t->queue_tail = buf;
                } else {
                    // Have to put the *io stack at the end of current queue.
                    assert(t->queue_tail->next == NULL);
                    t->queue_tail->next = buf;
                    t->queue_tail = buf;
                }
                pthread_mutex_unlock(&t->mutex);
                pthread_cond_signal(&t->cond);
                __sync_fetch_and_add(&swapOp, 1);
                ret = 2;
            } else{
                pthread_mutex_unlock(&t->mutex);
                io->buf = get_io_wubf_static(e);
#ifdef DRAM_TEST
                memcpy(io->buf, p->data + io->offset, io->len);
                ret = io->len;
#else
                ret = pread(p->fd, io->buf, io->len, p->offset + io->offset);
                __sync_fetch_and_add(&SSDread, 1);
#endif
                io->len = 1;
                ret = 0;
            }
            return ret;
        } else{
            return ret;
        }
#else
        int do_op = 1;
        int ret = 1;
        store_page *p = &e->pages[io->page_id];


        if (!p->free && !p->closed && p->version == io->page_version) {
            if (p->active && p->wbuf && io->offset >= p->wbuf->offset
            && io->offset < (p->wbuf->offset + e->wbuf_size)) {
                pthread_mutex_lock(&p->mutex);
                if (p->active && p->wbuf && io->offset >= p->wbuf->offset
                && io->offset < (p->wbuf->offset + e->wbuf_size)) {
                    io->buf = get_io_wubf_static(e);
                    ret = _read_from_wbuf(p, io);
                    do_op = 0;
                }
                pthread_mutex_unlock(&p->mutex);
            }
        } else {
            fprintf(stderr, "extstore_submit_opt: p->free:%d, p->closed:%d, p->id:%d, p->version:%d, io->version:%d\n",
                    p->free, p->closed, p->id, p->version, io->page_version);
            do_op = 0;
            ret = -2; // TODO: enum in IO for status?
        }
        if (do_op) {
            int wbuf_index = io->offset / e->wbuf_size;
            if(no_swap){
                io->buf = get_io_wubf_static(e);
#ifdef DRAM_TEST
                memcpy(io->buf, p->data + io->offset, io->len);
                ret = io->len;
#else
                ret = pread(p->fd, io->buf, io->len, p->offset + io->offset);
                __sync_fetch_and_add(&SSDread, 1);
#endif
                io->len = 1;
            } else{
                if((p->wbufSwaped[wbuf_index / 8] & (1L<<(wbuf_index % 8)))== 0 &&
                ((__sync_fetch_and_or(&p->wbufSwaped[wbuf_index / 8], (1L <<(wbuf_index % 8))))
                & (1L <<(wbuf_index % 8)) ) == 0){
                    swap_thread *t = _get_wap_thread(e);
                    pthread_mutex_lock(&t->mutex);
                    swap_buf * buf = t->swapBufStack;
                    if(buf != NULL){
                        t->swapBufStack = buf->next;
                        buf->next = NULL;
#ifndef ASY_READ
                        pthread_mutex_unlock(&t->mutex);
                        if (readWbuf(e, p->id, buf, e->wbuf_size, wbuf_index * e->wbuf_size) >0){
                            buf->mode = SWAP_WBUF;
                            buf->page_id = p->id;
                            buf->page_version = p->version;
                            buf->offset = wbuf_index * e->wbuf_size;
                            buf->len = e->wbuf_size;
                            io->buf = get_io_wubf_static(e);
                            memcpy(io->buf, buf->buf + (io->offset - buf->offset), io->len);
                            io->len = 0;
                            p->wbufs[wbuf_index] = buf;
//                            fprintf(stderr, "read buf, page:%d, wbuf_index:%d\n", p->id, wbuf_index);
                        } else{
#endif
                            buf->mode = SWAP_WBUF_NEEDREAD;
                            buf->page_id = p->id;
                            buf->page_version = p->version;
                            buf->offset = wbuf_index * e->wbuf_size;
                            buf->len = e->wbuf_size;
                            buf->buf = NULL;
#ifndef ASY_READ
                        }
                        pthread_mutex_lock(&t->mutex);
#endif
                        t->depth += 1;
                        if (t->queue == NULL) {
                            t->queue = buf;
                            t->queue_tail = buf;
                        } else {
                            // Have to put the *io stack at the end of current queue.
                            assert(t->queue_tail->next == NULL);
                            t->queue_tail->next = buf;
                            t->queue_tail = buf;
                        }
                        pthread_mutex_unlock(&t->mutex);
                        pthread_cond_signal(&t->cond);
                        __sync_fetch_and_add(&swapOp, 1);
                    } else{
                        pthread_mutex_unlock(&t->mutex);
                        __sync_fetch_and_and(&p->wbufSwaped[wbuf_index / 8], ~(1L <<(wbuf_index % 8)));
                        io->buf = get_io_wubf_static(e);
#ifdef DRAM_TEST
                        memcpy(io->buf, p->data + io->offset, io->len);
                        ret = io->len;
#else
                        ret = pread(p->fd, io->buf, io->len, p->offset + io->offset);
                        __sync_fetch_and_add(&SSDread, 1);
#endif
                        io->len = 1;
                    }

                } else{
                }
            }
            return 0;
        } else{
            return ret;
        }
#endif
    }
#endif

unsigned int depth = 0;
    obj_io *tio = io;
    obj_io *tail = NULL;
    while (tio != NULL) {
        tail = tio; // keep updating potential tail.
        depth++;
        tio = tio->next;
    }

    store_io_thread *t = _get_io_thread(e);
    pthread_mutex_lock(&t->mutex);

    t->depth += depth;

    if (t->queue == NULL) {
        t->queue = io;
        t->queue_tail = tail;
    } else {
        // Have to put the *io stack at the end of current queue.
        assert(tail->next == NULL);
        assert(t->queue_tail->next == NULL);
        t->queue_tail->next = io;
        t->queue_tail = tail;
    }


    pthread_mutex_unlock(&t->mutex);
    pthread_cond_signal(&t->cond);
    return 0;
}

/* engine note delete function: takes engine, page id, size?
 * note that an item in this page is no longer valid
 */
int extstore_delete(void *ptr, unsigned int page_id, uint64_t page_version,
        unsigned int count, unsigned int bytes) {
    store_engine *e = (store_engine *)ptr;
    // FIXME: validate page_id in bounds
    store_page *p = &e->pages[page_id];
    int ret = 0;

    pthread_mutex_lock(&p->mutex);
    if (!p->closed && p->version == page_version) {
        if (p->bytes_used >= bytes) {
            p->bytes_used -= bytes;
        } else {
            p->bytes_used = 0;
        }

        if (p->obj_count >= count) {
            p->obj_count -= count;
        } else {
            p->obj_count = 0; // caller has bad accounting?
        }
        STAT_L(e);
        e->stats.bytes_used -= bytes;
        e->stats.objects_used -= count;
        STAT_UL(e);

        if (p->obj_count == 0) {
            extstore_run_maint(e);
        }
    } else {
        ret = -1;
    }
    pthread_mutex_unlock(&p->mutex);
    return ret;
}

int extstore_check(void *ptr, unsigned int page_id, uint64_t page_version) {
    store_engine *e = (store_engine *)ptr;
    store_page *p = &e->pages[page_id];
    int ret = 0;

    pthread_mutex_lock(&p->mutex);
    if (p->version != page_version)
        ret = -1;
    pthread_mutex_unlock(&p->mutex);
    return ret;
}

/* allows a compactor to say "we're done with this page, kill it. */
void extstore_close_page(void *ptr, unsigned int page_id, uint64_t page_version) {
    store_engine *e = (store_engine *)ptr;
    store_page *p = &e->pages[page_id];

    pthread_mutex_lock(&p->mutex);
    if (!p->closed && p->version == page_version) {
        p->closed = true;
        extstore_run_maint(e);
    }
    pthread_mutex_unlock(&p->mutex);
}

#ifndef ENABLE_NVM

/* engine IO thread; takes engine context
 * manage writes/reads
 * runs IO callbacks inline after each IO
 */
// FIXME: protect from reading past page
static void *extstore_io_thread(void *arg) {
    store_io_thread *me = (store_io_thread *)arg;
    store_engine *e = me->e;
    while (1) {
        obj_io *io_stack = NULL;
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
            obj_io *end = NULL;
            io_stack = me->queue;
            end = io_stack;
            for (i = 1; i < e->io_depth; i++) {
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

        obj_io *cur_io = io_stack;
        while (cur_io) {
//            fprintf(stderr, "(%ld), t:%p, io_depth:%d, cur_io:%p, next:%p\n",
//                    pthread_self(), (void *)me,  me->depth, (void *)cur_io, (void *)me->queue);

            // We need to note next before the callback in case the obj_io
            // gets reused.
            obj_io *next = cur_io->next;
            int ret = 0;
            int do_op = 1;
            store_page *p = &e->pages[cur_io->page_id];
            // TODO: loop if not enough bytes were read/written.
            switch (cur_io->mode) {
                case OBJ_IO_READ:
                    // Page is currently open. deal if read is past the end.
                    pthread_mutex_lock(&p->mutex);
                    if (!p->free && !p->closed && p->version == cur_io->page_version) {
                        if (p->active && cur_io->offset >= p->written) {

                            ret = _read_from_wbuf(p, cur_io);
                            do_op = 0;
                        } else {
                            p->refcount++;
                        }
#ifndef ENABLE_NVM
                        STAT_L(e);
                        e->stats.bytes_read += cur_io->len;
                        e->stats.objects_read++;
                        STAT_UL(e);
#endif
                    } else {
                        fprintf(stderr, "extstore_io_thread: p->free:%d, p->closed:%d, p->version:%d\n",
                                p->free, p->closed, cur_io->page_version);
                        do_op = 0;
                        ret = -2; // TODO: enum in IO for status?
                    }
                    pthread_mutex_unlock(&p->mutex);
                    if (do_op) {
#if !defined(HAVE_PREAD) || !defined(HAVE_PREADV)
                        // TODO: lseek offset is natively 64-bit on OS X, but
                        // perhaps not on all platforms? Else use lseek64()
                        ret = lseek(p->fd, p->offset + cur_io->offset, SEEK_SET);
                        if (ret >= 0) {
                            if (cur_io->iov == NULL) {
                                ret = read(p->fd, cur_io->buf, cur_io->len);
                            } else {
                                ret = readv(p->fd, cur_io->iov, cur_io->iovcnt);
                            }
                        }
#else
                        if (cur_io->iov == NULL) {
                            ret = pread(p->fd, cur_io->buf, cur_io->len, p->offset + cur_io->offset);
                        } else {
                            ret = preadv(p->fd, cur_io->iov, cur_io->iovcnt, p->offset + cur_io->offset);
                        }
#endif
                    }else{
                        int wbuf_index = cur_io->offset / e->wbuf_size;
//                        fprintf(stderr, "extstore_io_thread: reset need swap page:%d, wbuf_index:%d, no need swap\n",
//                                p->id, wbuf_index);
                        pthread_mutex_lock(&p->mutex);
                        p->wbufWaitSwap[wbuf_index / 8] &= (~(1 <<(wbuf_index % 8)) );
                        pthread_mutex_unlock(&p->mutex);
                    }
                    break;
                case OBJ_IO_WRITE:
                    do_op = 0;
#ifdef DRAM_TEST
                    memcpy(p->data + cur_io->offset, cur_io->buf, cur_io->len);
#else
                    // FIXME: Should hold refcount during write. doesn't
                    // currently matter since page can't free while active.
                    ret = pwrite(p->fd, cur_io->buf, cur_io->len, p->offset + cur_io->offset);
#endif

                    break;
            }
            if (ret == 0) {
                E_DEBUG("read returned nothing\n");
            }

#ifdef EXTSTORE_DEBUG
            if (ret == -1) {
                perror("read/write op failed");
            }
#endif
            cur_io->cb(e, cur_io, ret);
            if (do_op) {
                pthread_mutex_lock(&p->mutex);
                p->refcount--;
                pthread_mutex_unlock(&p->mutex);
            }
            cur_io = next;
        }
    }

    return NULL;
}
#endif

// call with *p locked.
static void _free_page(store_engine *e, store_page *p) {
    fprintf(stderr, "_free_page, page:%d\n", p->id);
    store_page *tmp = NULL;
    store_page *prev = NULL;
    E_DEBUG("EXTSTORE: freeing page %u\n", p->id);
#ifndef ENABLE_NVM
    STAT_L(e);
    e->stats.objects_used -= p->obj_count;
    e->stats.bytes_used -= p->bytes_used;
    e->stats.page_reclaims++;
    STAT_UL(e);
#endif
#ifdef ENABLE_NVM
    for (int i = 0; i < e->page_size / e->wbuf_size; ++i) {
        if((p->wbufSwaped[i / 8] & (1 <<(i % 8))) == 0 ){
            fprintf(stderr, "_free_page, page:%d, wbuf_index:%d\n", p->id, i);
            char buf[e->wbuf_size];
#ifdef DRAM_TEST
            memcpy(buf, p->data + e->wbuf_size * i, e->wbuf_size);
            int ret = e->wbuf_size;
#else
            int ret = pread(p->fd, buf, e->wbuf_size, p->offset + e->wbuf_size * i);
            __sync_fetch_and_add(&SSDread, 1);
#endif
            if(ret < 0){
                fprintf(stderr, "_Swap_Wbuf read error page:%ld, wbuf:%d\n", p->offset, i);
                continue;
            }
            //TODO: need to fix
            e->bufSwapFn((void *)e, buf, e->wbuf_size, p->version,  p->id,i * e->wbuf_size);
            p->wbufSwaped[i / 8] |= (1<< (i % 8));
        }
    }
#endif

    pthread_mutex_lock(&e->mutex);
    // unlink page from bucket list
    tmp = e->page_buckets[p->bucket];
    while (tmp) {
        if (tmp == p) {
            if (prev) {
                prev->next = tmp->next;
            } else {
                e->page_buckets[p->bucket] = tmp->next;
            }
            tmp->next = NULL;
            break;
        }
        prev = tmp;
        tmp = tmp->next;
    }
    // reset most values
    p->version = 0;
    p->obj_count = 0;
    p->bytes_used = 0;
    p->allocated = 0;
    p->written = 0;
    p->bucket = 0;
    p->active = false;
    p->closed = false;
    p->free = true;
    memset(p->wbufSwaped, 0, e->page_size / e->wbuf_size / 8);
    memset(p->wbufs, 0, e->page_size / e->wbuf_size * sizeof (void *));
    // add to page stack
    // TODO: free_page_buckets first class and remove redundancy?
    if (p->free_bucket != 0) {
        p->next = e->free_page_buckets[p->free_bucket];
        e->free_page_buckets[p->free_bucket] = p;
    } else {
        p->next = e->page_freelist;
        e->page_freelist = p;
    }
    e->page_free++;
    pthread_mutex_unlock(&e->mutex);
}

/* engine maint thread; takes engine context.
 * Uses version to ensure oldest possible objects are being evicted.
 * Needs interface to inform owner of pages with fewer objects or most space
 * free, which can then be actively compacted to avoid eviction.
 *
 * This gets called asynchronously after every page allocation. Could run less
 * often if more pages are free.
 *
 * Another allocation call is required if an attempted free didn't happen
 * due to the page having a refcount.
 */

// TODO: Don't over-evict pages if waiting on refcounts to drop
static void *extstore_maint_thread(void *arg) {
    store_maint_thread *me = (store_maint_thread *)arg;
    store_engine *e = me->e;
    struct extstore_page_data *pd =
        calloc(e->page_count, sizeof(struct extstore_page_data));
    pthread_mutex_lock(&me->mutex);
    while (1) {
        int i;
        bool do_evict = false;
        unsigned int low_page = 0;
        uint64_t low_version = ULLONG_MAX;

        pthread_cond_wait(&me->cond, &me->mutex);
        pthread_mutex_lock(&e->mutex);
        // default freelist requires at least one page free.
        // specialized freelists fall back to default once full.
        if (e->page_free == 0 || e->page_freelist == NULL) {
            do_evict = true;
        }
        pthread_mutex_unlock(&e->mutex);
        memset(pd, 0, sizeof(struct extstore_page_data) * e->page_count);

        for (i = 0; i < e->page_count; i++) {
            store_page *p = &e->pages[i];
            pthread_mutex_lock(&p->mutex);
            pd[p->id].free_bucket = p->free_bucket;
            if (p->active || p->free) {
                pthread_mutex_unlock(&p->mutex);
                continue;
            }
            if (p->obj_count > 0 && !p->closed) {
                pd[p->id].version = p->version;
                pd[p->id].bytes_used = p->bytes_used;
                pd[p->id].bucket = p->bucket;
                // low_version/low_page are only used in the eviction
                // scenario. when we evict, it's only to fill the default page
                // bucket again.
                // TODO: experiment with allowing evicting up to a single page
                // for any specific free bucket. this is *probably* required
                // since it could cause a load bias on default-only devices?
                if (p->free_bucket == 0 && p->version < low_version) {
                    low_version = p->version;
                    low_page = i;
                }
            }
            if ((p->obj_count == 0 || p->closed) && p->refcount == 0) {
                _free_page(e, p);
                // Found a page to free, no longer need to evict.
                do_evict = false;
            }
            pthread_mutex_unlock(&p->mutex);
        }

        if (do_evict && low_version != ULLONG_MAX) {
#ifdef ENABLE_NVM
            fprintf(stderr, "need do evict\n");
            exit(0);
#endif

            store_page *p = &e->pages[low_page];
            E_DEBUG("EXTSTORE: evicting page [%d] [v: %llu]\n",
                    p->id, (unsigned long long) p->version);
            pthread_mutex_lock(&p->mutex);
            if (!p->closed) {
                p->closed = true;
#ifndef ENABLE_NVM
                STAT_L(e);
                e->stats.page_evictions++;
                e->stats.objects_evicted += p->obj_count;
                e->stats.bytes_evicted += p->bytes_used;
                STAT_UL(e);
#endif
                if (p->refcount == 0) {
                    _free_page(e, p);
                }
            }
            pthread_mutex_unlock(&p->mutex);
        }
        if(!do_evict){
            sleep(1);
        }

        // copy the page data into engine context so callers can use it from
        // the stats lock.
#ifndef ENABLE_NVM
        STAT_L(e);
        memcpy(e->stats.page_data, pd,
                sizeof(struct extstore_page_data) * e->page_count);
        STAT_UL(e);
#endif
    }

    return NULL;
}
