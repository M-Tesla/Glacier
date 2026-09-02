#ifndef _SYS_MMAN_H
#define _SYS_MMAN_H

#include <stddef.h>

#define PROT_READ 1
#define PROT_WRITE 2
#define MAP_PRIVATE 2
#define MAP_FAILED ((void *)-1)
#define MADV_SEQUENTIAL 1
#define MADV_RANDOM 2

static inline void *mmap(void *addr, size_t len, int prot, int flags, int fd, long off) {
    (void)addr;
    (void)len;
    (void)prot;
    (void)flags;
    (void)fd;
    (void)off;
    return MAP_FAILED;
}

static inline int munmap(void *addr, size_t len) {
    (void)addr;
    (void)len;
    return -1;
}

static inline int madvise(void *addr, size_t len, int advice) {
    (void)addr;
    (void)len;
    (void)advice;
    return 0;
}

#endif
