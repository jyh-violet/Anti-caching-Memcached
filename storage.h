#ifndef STORAGE_H
#define STORAGE_H

void storage_delete(void *e, item *it);
#ifdef EXTSTORE
#define STORAGE_delete(e, it) \
    do { \
        storage_delete(e, it); \
    } while (0)
#else
#define STORAGE_delete(...)
#endif

// API.
void storage_stats(ADD_STAT add_stats, conn *c);
void process_extstore_stats(ADD_STAT add_stats, conn *c);
bool storage_validate_item(void *e, item *it);
int storage_get_item(conn *c, item *it, mc_resp *resp);
int nvm_aio_get_item(conn *c, item* src, int len);

// callbacks for the IO queue subsystem.
void storage_submit_cb(io_queue_t *q);
void storage_complete_cb(io_queue_t *q);
void storage_finalize_cb(io_pending_t *pending);

// Thread functions.
int start_storage_write_thread(void *arg);
void storage_write_pause(void);
void storage_write_resume(void);
int start_storage_compact_thread(void *arg);
void storage_compact_pause(void);
void storage_compact_resume(void);

// Init functions.
struct extstore_conf_file *storage_conf_parse(char *arg, unsigned int page_size);
int nvm_conf_parse(char *arg, struct settings* settings);
void *storage_init_config(struct settings *s);
#ifdef ENABLE_NVM
int storage_read_config(void *conf, void *nvm_conf, char **subopt);
int nvm_write(void *storage, const int clsid, int local_slabNum, int local_per_thread_counter,
              int local_lru_start, int local_nvm_lru_index, int local_nvm_slab_index);
int nvm_swap(void *storage, const int clsid, const int item_age,  int8_t flag);
int swap_nvm_to_dram(struct lru_pull_tail_return *it_info, item *new_it);

bool _storage_Swap_wbuf(char* buf, size_t size, int pageVersion, int pageId, int offset, int local_nvm_slab_index, int local_nvm_per_thread_counter);
bool _swap_item(char* buf, int pageVersion, int pageId, int offset,
                int local_nvm_slab_index, int local_nvm_adder, int local_nvm_per_thread_counter);
int dram_evict(const int clsid, int local_slabNum, int local_per_thread_counter,
               int local_lru_start, int local_nvm_lru_index, int local_nvm_slab_index);
int nvm_add_to_cache(const int clsid, const int item_age,  int8_t flag) ;
#else
int storage_read_config(void *conf, char **subopt);
#endif
int storage_check_config(void *conf);
void *storage_init(void *conf);

//nvm functions
void *nvm_init_config(struct settings *s);
void nvm_init(void *conf);
int start_nvm_write_thread(void *arg);
int nvm_get_item(conn *c, item *it, mc_resp *resp);
int start_nvm_swap_thread(void *arg);

bool flushItToNvm(struct lru_pull_tail_return *it_info, item *hdr_it, int local_nvm_lru_counter);
bool flushItFromDRAMToExt(void * storage, struct lru_pull_tail_return *it_info, item *hdr_it);
item* writeItToExt(void * storage, char* key, char* value, int key_n, int value_n, uint32_t hv);

#define EXEC_2_TIMES(x) x x
#define EXEC_4_TIMES(x) EXEC_2_TIMES(EXEC_2_TIMES(x))
#define EXEC_8_TIMES(x) EXEC_2_TIMES(EXEC_4_TIMES(x))
#define EXEC_16_TIMES(x) EXEC_2_TIMES(EXEC_8_TIMES(x))
#define EXEC_32_TIMES(x) EXEC_2_TIMES(EXEC_16_TIMES(x))
#define EXEC_64_TIMES(x) EXEC_2_TIMES(EXEC_32_TIMES(x))
#define EXEC_128_TIMES(x) EXEC_2_TIMES(EXEC_64_TIMES(x))
#define EXEC_256_TIMES(x) EXEC_2_TIMES(EXEC_128_TIMES(x))

#define WRITE_NT_64_ASM \
"vmovntdq %%zmm0, 0(%[addr]) \n"\

#define WRITE_NT_128_ASM \
"vmovntdq %%zmm0, 0(%[addr]) \n"\
"vmovntdq %%zmm0, 1*64(%[addr]) \n"

#define WRITE_NT_256_ASM \
"vmovntdq %%zmm0, 0(%[addr]) \n" \
"vmovntdq %%zmm0, 1*64(%[addr]) \n" \
"vmovntdq %%zmm0, 2*64(%[addr]) \n" \
"vmovntdq %%zmm0, 3*64(%[addr]) \n"

#define WRITE_NT_512_ASM \
"vmovntdq %%zmm0, 0*64(%[addr]) \n" \
"vmovntdq %%zmm0, 1*64(%[addr]) \n" \
"vmovntdq %%zmm0, 2*64(%[addr]) \n" \
"vmovntdq %%zmm0, 3*64(%[addr]) \n" \
"vmovntdq %%zmm0, 4*64(%[addr]) \n" \
"vmovntdq %%zmm0, 5*64(%[addr]) \n" \
"vmovntdq %%zmm0, 6*64(%[addr]) \n" \
"vmovntdq %%zmm0, 7*64(%[addr]) \n"

#define WRITE_NT_1024_ASM \
"vmovntdq %%zmm0, 0*64(%[addr]) \n" \
"vmovntdq %%zmm0, 1*64(%[addr]) \n" \
"vmovntdq %%zmm0, 2*64(%[addr]) \n" \
"vmovntdq %%zmm0, 3*64(%[addr]) \n" \
"vmovntdq %%zmm0, 4*64(%[addr]) \n" \
"vmovntdq %%zmm0, 5*64(%[addr]) \n" \
"vmovntdq %%zmm0, 6*64(%[addr]) \n" \
"vmovntdq %%zmm0, 7*64(%[addr]) \n" \
"vmovntdq %%zmm0, 8*64(%[addr]) \n" \
"vmovntdq %%zmm0, 9*64(%[addr]) \n" \
"vmovntdq %%zmm0, 10*64(%[addr]) \n" \
"vmovntdq %%zmm0, 11*64(%[addr]) \n" \
"vmovntdq %%zmm0, 12*64(%[addr]) \n" \
"vmovntdq %%zmm0, 13*64(%[addr]) \n" \
"vmovntdq %%zmm0, 14*64(%[addr]) \n" \
"vmovntdq %%zmm0, 15*64(%[addr]) \n" \

#define WRITE_NT_2048_ASM \
"vmovntdq %%zmm0, 0*64(%[addr]) \n" \
"vmovntdq %%zmm0, 1*64(%[addr]) \n" \
"vmovntdq %%zmm0, 2*64(%[addr]) \n" \
"vmovntdq %%zmm0, 3*64(%[addr]) \n" \
"vmovntdq %%zmm0, 4*64(%[addr]) \n" \
"vmovntdq %%zmm0, 5*64(%[addr]) \n" \
"vmovntdq %%zmm0, 6*64(%[addr]) \n" \
"vmovntdq %%zmm0, 7*64(%[addr]) \n" \
"vmovntdq %%zmm0, 8*64(%[addr]) \n" \
"vmovntdq %%zmm0, 9*64(%[addr]) \n" \
"vmovntdq %%zmm0, 10*64(%[addr]) \n" \
"vmovntdq %%zmm0, 11*64(%[addr]) \n" \
"vmovntdq %%zmm0, 12*64(%[addr]) \n" \
"vmovntdq %%zmm0, 13*64(%[addr]) \n" \
"vmovntdq %%zmm0, 14*64(%[addr]) \n" \
"vmovntdq %%zmm0, 15*64(%[addr]) \n" \
"vmovntdq %%zmm0, 16*64(%[addr]) \n" \
"vmovntdq %%zmm0, 17*64(%[addr]) \n" \
"vmovntdq %%zmm0, 18*64(%[addr]) \n" \
"vmovntdq %%zmm0, 19*64(%[addr]) \n" \
"vmovntdq %%zmm0, 20*64(%[addr]) \n" \
"vmovntdq %%zmm0, 21*64(%[addr]) \n" \
"vmovntdq %%zmm0, 22*64(%[addr]) \n" \
"vmovntdq %%zmm0, 23*64(%[addr]) \n" \
"vmovntdq %%zmm0, 24*64(%[addr]) \n" \
"vmovntdq %%zmm0, 25*64(%[addr]) \n" \
"vmovntdq %%zmm0, 26*64(%[addr]) \n" \
"vmovntdq %%zmm0, 27*64(%[addr]) \n" \
"vmovntdq %%zmm0, 28*64(%[addr]) \n" \
"vmovntdq %%zmm0, 29*64(%[addr]) \n" \
"vmovntdq %%zmm0, 30*64(%[addr]) \n" \
"vmovntdq %%zmm0, 31*64(%[addr]) \n"

#define WRITE_NT_4096_ASM \
"vmovntdq %%zmm0, 0*64(%[addr]) \n" \
"vmovntdq %%zmm0, 1*64(%[addr]) \n" \
"vmovntdq %%zmm0, 2*64(%[addr]) \n" \
"vmovntdq %%zmm0, 3*64(%[addr]) \n" \
"vmovntdq %%zmm0, 4*64(%[addr]) \n" \
"vmovntdq %%zmm0, 5*64(%[addr]) \n" \
"vmovntdq %%zmm0, 6*64(%[addr]) \n" \
"vmovntdq %%zmm0, 7*64(%[addr]) \n" \
"vmovntdq %%zmm0, 8*64(%[addr]) \n" \
"vmovntdq %%zmm0, 9*64(%[addr]) \n" \
"vmovntdq %%zmm0, 10*64(%[addr]) \n" \
"vmovntdq %%zmm0, 11*64(%[addr]) \n" \
"vmovntdq %%zmm0, 12*64(%[addr]) \n" \
"vmovntdq %%zmm0, 13*64(%[addr]) \n" \
"vmovntdq %%zmm0, 14*64(%[addr]) \n" \
"vmovntdq %%zmm0, 15*64(%[addr]) \n" \
"vmovntdq %%zmm0, 16*64(%[addr]) \n" \
"vmovntdq %%zmm0, 17*64(%[addr]) \n" \
"vmovntdq %%zmm0, 18*64(%[addr]) \n" \
"vmovntdq %%zmm0, 19*64(%[addr]) \n" \
"vmovntdq %%zmm0, 20*64(%[addr]) \n" \
"vmovntdq %%zmm0, 21*64(%[addr]) \n" \
"vmovntdq %%zmm0, 22*64(%[addr]) \n" \
"vmovntdq %%zmm0, 23*64(%[addr]) \n" \
"vmovntdq %%zmm0, 24*64(%[addr]) \n" \
"vmovntdq %%zmm0, 25*64(%[addr]) \n" \
"vmovntdq %%zmm0, 26*64(%[addr]) \n" \
"vmovntdq %%zmm0, 27*64(%[addr]) \n" \
"vmovntdq %%zmm0, 28*64(%[addr]) \n" \
"vmovntdq %%zmm0, 29*64(%[addr]) \n" \
"vmovntdq %%zmm0, 30*64(%[addr]) \n" \
"vmovntdq %%zmm0, 31*64(%[addr]) \n" \
"vmovntdq %%zmm0, 32*64(%[addr]) \n" \
"vmovntdq %%zmm0, 33*64(%[addr]) \n" \
"vmovntdq %%zmm0, 34*64(%[addr]) \n" \
"vmovntdq %%zmm0, 35*64(%[addr]) \n" \
"vmovntdq %%zmm0, 36*64(%[addr]) \n" \
"vmovntdq %%zmm0, 37*64(%[addr]) \n" \
"vmovntdq %%zmm0, 38*64(%[addr]) \n" \
"vmovntdq %%zmm0, 39*64(%[addr]) \n" \
"vmovntdq %%zmm0, 40*64(%[addr]) \n" \
"vmovntdq %%zmm0, 41*64(%[addr]) \n" \
"vmovntdq %%zmm0, 42*64(%[addr]) \n" \
"vmovntdq %%zmm0, 43*64(%[addr]) \n" \
"vmovntdq %%zmm0, 44*64(%[addr]) \n" \
"vmovntdq %%zmm0, 45*64(%[addr]) \n" \
"vmovntdq %%zmm0, 46*64(%[addr]) \n" \
"vmovntdq %%zmm0, 47*64(%[addr]) \n" \
"vmovntdq %%zmm0, 48*64(%[addr]) \n" \
"vmovntdq %%zmm0, 49*64(%[addr]) \n" \
"vmovntdq %%zmm0, 50*64(%[addr]) \n" \
"vmovntdq %%zmm0, 51*64(%[addr]) \n" \
"vmovntdq %%zmm0, 52*64(%[addr]) \n" \
"vmovntdq %%zmm0, 53*64(%[addr]) \n" \
"vmovntdq %%zmm0, 54*64(%[addr]) \n" \
"vmovntdq %%zmm0, 55*64(%[addr]) \n" \
"vmovntdq %%zmm0, 56*64(%[addr]) \n" \
"vmovntdq %%zmm0, 57*64(%[addr]) \n" \
"vmovntdq %%zmm0, 58*64(%[addr]) \n" \
"vmovntdq %%zmm0, 59*64(%[addr]) \n" \
"vmovntdq %%zmm0, 60*64(%[addr]) \n" \
"vmovntdq %%zmm0, 61*64(%[addr]) \n" \
"vmovntdq %%zmm0, 62*64(%[addr]) \n" \
"vmovntdq %%zmm0, 63*64(%[addr]) \n"

#define WRITE_NT_8192_ASM \
"vmovntdq %%zmm0, 0*64(%[addr]) \n" \
"vmovntdq %%zmm0, 1*64(%[addr]) \n" \
"vmovntdq %%zmm0, 2*64(%[addr]) \n" \
"vmovntdq %%zmm0, 3*64(%[addr]) \n" \
"vmovntdq %%zmm0, 4*64(%[addr]) \n" \
"vmovntdq %%zmm0, 5*64(%[addr]) \n" \
"vmovntdq %%zmm0, 6*64(%[addr]) \n" \
"vmovntdq %%zmm0, 7*64(%[addr]) \n" \
"vmovntdq %%zmm0, 8*64(%[addr]) \n" \
"vmovntdq %%zmm0, 9*64(%[addr]) \n" \
"vmovntdq %%zmm0, 10*64(%[addr]) \n" \
"vmovntdq %%zmm0, 11*64(%[addr]) \n" \
"vmovntdq %%zmm0, 12*64(%[addr]) \n" \
"vmovntdq %%zmm0, 13*64(%[addr]) \n" \
"vmovntdq %%zmm0, 14*64(%[addr]) \n" \
"vmovntdq %%zmm0, 15*64(%[addr]) \n" \
"vmovntdq %%zmm0, 16*64(%[addr]) \n" \
"vmovntdq %%zmm0, 17*64(%[addr]) \n" \
"vmovntdq %%zmm0, 18*64(%[addr]) \n" \
"vmovntdq %%zmm0, 19*64(%[addr]) \n" \
"vmovntdq %%zmm0, 20*64(%[addr]) \n" \
"vmovntdq %%zmm0, 21*64(%[addr]) \n" \
"vmovntdq %%zmm0, 22*64(%[addr]) \n" \
"vmovntdq %%zmm0, 23*64(%[addr]) \n" \
"vmovntdq %%zmm0, 24*64(%[addr]) \n" \
"vmovntdq %%zmm0, 25*64(%[addr]) \n" \
"vmovntdq %%zmm0, 26*64(%[addr]) \n" \
"vmovntdq %%zmm0, 27*64(%[addr]) \n" \
"vmovntdq %%zmm0, 28*64(%[addr]) \n" \
"vmovntdq %%zmm0, 29*64(%[addr]) \n" \
"vmovntdq %%zmm0, 30*64(%[addr]) \n" \
"vmovntdq %%zmm0, 31*64(%[addr]) \n" \
"vmovntdq %%zmm0, 32*64(%[addr]) \n" \
"vmovntdq %%zmm0, 33*64(%[addr]) \n" \
"vmovntdq %%zmm0, 34*64(%[addr]) \n" \
"vmovntdq %%zmm0, 35*64(%[addr]) \n" \
"vmovntdq %%zmm0, 36*64(%[addr]) \n" \
"vmovntdq %%zmm0, 37*64(%[addr]) \n" \
"vmovntdq %%zmm0, 38*64(%[addr]) \n" \
"vmovntdq %%zmm0, 39*64(%[addr]) \n" \
"vmovntdq %%zmm0, 40*64(%[addr]) \n" \
"vmovntdq %%zmm0, 41*64(%[addr]) \n" \
"vmovntdq %%zmm0, 42*64(%[addr]) \n" \
"vmovntdq %%zmm0, 43*64(%[addr]) \n" \
"vmovntdq %%zmm0, 44*64(%[addr]) \n" \
"vmovntdq %%zmm0, 45*64(%[addr]) \n" \
"vmovntdq %%zmm0, 46*64(%[addr]) \n" \
"vmovntdq %%zmm0, 47*64(%[addr]) \n" \
"vmovntdq %%zmm0, 48*64(%[addr]) \n" \
"vmovntdq %%zmm0, 49*64(%[addr]) \n" \
"vmovntdq %%zmm0, 50*64(%[addr]) \n" \
"vmovntdq %%zmm0, 51*64(%[addr]) \n" \
"vmovntdq %%zmm0, 52*64(%[addr]) \n" \
"vmovntdq %%zmm0, 53*64(%[addr]) \n" \
"vmovntdq %%zmm0, 54*64(%[addr]) \n" \
"vmovntdq %%zmm0, 55*64(%[addr]) \n" \
"vmovntdq %%zmm0, 56*64(%[addr]) \n" \
"vmovntdq %%zmm0, 57*64(%[addr]) \n" \
"vmovntdq %%zmm0, 58*64(%[addr]) \n" \
"vmovntdq %%zmm0, 59*64(%[addr]) \n" \
"vmovntdq %%zmm0, 60*64(%[addr]) \n" \
"vmovntdq %%zmm0, 61*64(%[addr]) \n" \
"vmovntdq %%zmm0, 62*64(%[addr]) \n" \
"vmovntdq %%zmm0, 63*64(%[addr]) \n" \
"vmovntdq %%zmm0, 64*64(%[addr]) \n" \
"vmovntdq %%zmm0, 65*64(%[addr]) \n" \
"vmovntdq %%zmm0, 66*64(%[addr]) \n" \
"vmovntdq %%zmm0, 67*64(%[addr]) \n" \
"vmovntdq %%zmm0, 68*64(%[addr]) \n" \
"vmovntdq %%zmm0, 69*64(%[addr]) \n" \
"vmovntdq %%zmm0, 70*64(%[addr]) \n" \
"vmovntdq %%zmm0, 71*64(%[addr]) \n" \
"vmovntdq %%zmm0, 72*64(%[addr]) \n" \
"vmovntdq %%zmm0, 73*64(%[addr]) \n" \
"vmovntdq %%zmm0, 74*64(%[addr]) \n" \
"vmovntdq %%zmm0, 75*64(%[addr]) \n" \
"vmovntdq %%zmm0, 76*64(%[addr]) \n" \
"vmovntdq %%zmm0, 77*64(%[addr]) \n" \
"vmovntdq %%zmm0, 78*64(%[addr]) \n" \
"vmovntdq %%zmm0, 79*64(%[addr]) \n" \
"vmovntdq %%zmm0, 80*64(%[addr]) \n" \
"vmovntdq %%zmm0, 81*64(%[addr]) \n" \
"vmovntdq %%zmm0, 82*64(%[addr]) \n" \
"vmovntdq %%zmm0, 83*64(%[addr]) \n" \
"vmovntdq %%zmm0, 84*64(%[addr]) \n" \
"vmovntdq %%zmm0, 85*64(%[addr]) \n" \
"vmovntdq %%zmm0, 86*64(%[addr]) \n" \
"vmovntdq %%zmm0, 87*64(%[addr]) \n" \
"vmovntdq %%zmm0, 88*64(%[addr]) \n" \
"vmovntdq %%zmm0, 89*64(%[addr]) \n" \
"vmovntdq %%zmm0, 90*64(%[addr]) \n" \
"vmovntdq %%zmm0, 91*64(%[addr]) \n" \
"vmovntdq %%zmm0, 92*64(%[addr]) \n" \
"vmovntdq %%zmm0, 93*64(%[addr]) \n" \
"vmovntdq %%zmm0, 94*64(%[addr]) \n" \
"vmovntdq %%zmm0, 95*64(%[addr]) \n" \
"vmovntdq %%zmm0, 96*64(%[addr]) \n" \
"vmovntdq %%zmm0, 97*64(%[addr]) \n" \
"vmovntdq %%zmm0, 98*64(%[addr]) \n" \
"vmovntdq %%zmm0, 99*64(%[addr]) \n" \
"vmovntdq %%zmm0, 100*64(%[addr]) \n" \
"vmovntdq %%zmm0, 101*64(%[addr]) \n" \
"vmovntdq %%zmm0, 102*64(%[addr]) \n" \
"vmovntdq %%zmm0, 103*64(%[addr]) \n" \
"vmovntdq %%zmm0, 104*64(%[addr]) \n" \
"vmovntdq %%zmm0, 105*64(%[addr]) \n" \
"vmovntdq %%zmm0, 106*64(%[addr]) \n" \
"vmovntdq %%zmm0, 107*64(%[addr]) \n" \
"vmovntdq %%zmm0, 108*64(%[addr]) \n" \
"vmovntdq %%zmm0, 109*64(%[addr]) \n" \
"vmovntdq %%zmm0, 110*64(%[addr]) \n" \
"vmovntdq %%zmm0, 111*64(%[addr]) \n" \
"vmovntdq %%zmm0, 112*64(%[addr]) \n" \
"vmovntdq %%zmm0, 113*64(%[addr]) \n" \
"vmovntdq %%zmm0, 114*64(%[addr]) \n" \
"vmovntdq %%zmm0, 115*64(%[addr]) \n" \
"vmovntdq %%zmm0, 116*64(%[addr]) \n" \
"vmovntdq %%zmm0, 117*64(%[addr]) \n" \
"vmovntdq %%zmm0, 118*64(%[addr]) \n" \
"vmovntdq %%zmm0, 119*64(%[addr]) \n" \
"vmovntdq %%zmm0, 120*64(%[addr]) \n" \
"vmovntdq %%zmm0, 121*64(%[addr]) \n" \
"vmovntdq %%zmm0, 122*64(%[addr]) \n" \
"vmovntdq %%zmm0, 123*64(%[addr]) \n" \
"vmovntdq %%zmm0, 124*64(%[addr]) \n" \
"vmovntdq %%zmm0, 125*64(%[addr]) \n" \
"vmovntdq %%zmm0, 126*64(%[addr]) \n" \
"vmovntdq %%zmm0, 127*64(%[addr]) \n"

void write_nt_64(void *dst, void *src);
void write_nt_128(void *dst, void *src);
void write_nt_256(void *dst, void *src);
void write_nt_512(void *dst, void *src);
void write_nt_1024(void *dst, void *src);
void write_nt_2048(void *dst, void *src);
void write_nt_4096(void *dst, void *src);
void write_nt_8192(void *dst, void *src);
void write_nt_16384(void *dst, void *src);
void write_nt_32768(void *dst, void *src);
void write_nt_65536(void *dst, void *src);

void memcpy_nt_128(void *dst, void *src);
void memcpy_nt_256(void *dst, void *src);
void memcpy_nt_512(void *dst, void *src);
void memcpy_nt_1024(void *dst, void *src);
void memcpy_nt_2048(void *dst, void *src);




// Ignore pointers and header bits from the CRC
#define STORE_OFFSET offsetof(item, nbytes)
#define READ_OFFSET offsetof(item, data)
#endif
