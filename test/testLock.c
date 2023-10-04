//
// Created by workshop on 7/29/2022.
//

#include <pthread.h>
#include "stdio.h"
typedef volatile char Lock_t;

inline void unlock(Lock_t * _l) __attribute__ ((always_inline)); //__attribute__((noinline));
inline void lock(Lock_t * _l) __attribute__((always_inline)); // __attribute__ ((noinline));
inline int  tas(volatile char * lock) __attribute__((always_inline));
inline int  try_lock(volatile char * lock) __attribute__((always_inline));

/*
 * Non-recursive spinlock. Using `xchg` and `ldstub` as in PostgresSQL.
 */
/* Call blocks and retunrs only when it has the lock. */
inline void lock(Lock_t * _l){
    while(tas(_l)) {
#if defined(__i386__) || defined(__x86_64__)
        __asm__ __volatile__ ("pause\n");
#endif
    }
}

/** Unlocks the lock object. */
inline void unlock(Lock_t * _l){
    *_l = 0;
}

inline int try_lock(Lock_t * _l){
    if(tas(_l)){
        return 0; //failed
    } else{
        return 1; //success
    }
}

inline int tas(volatile char * lock)
{
    register char res = 1;
#if defined(__i386__) || defined(__x86_64__)
    __asm__ __volatile__ (
            "lock xchgb %0, %1\n"
            : "+q"(res), "+m"(*lock)
            :
            : "memory", "cc");
#elif defined(__sparc__)
    __asm__ __volatile__ (
            "ldstub [%2], %0"
            : "=r"(res), "+m"(*lock)
            : "r"(lock)
            : "memory");
#else
#error TAS not defined for this architecture.
#endif
    return res;
}

Lock_t m_lock;
int num = 0;
void test(){
    int success = 0;
    int i = 0;
    while (success < 100000) {
        if(try_lock(&m_lock)){
            num ++;
            success ++;
            unlock(&m_lock);
        }
        i ++;
    }
    printf("%d\n", i);
}

int main(){
    int tCount = 50;
    pthread_t tid[tCount];
    for (int i = 0; i < tCount; ++i) {
        pthread_create(tid + i, NULL, (void *(*)(void *))(test), NULL);
    }
    for (int i = 0; i < tCount; ++i) {
        pthread_join(tid[i], NULL);
    }
    printf("%d\n", num);
    return 0;
}