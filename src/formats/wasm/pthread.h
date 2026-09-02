#ifndef _PTHREAD_H
#define _PTHREAD_H

/* Serial pthread stand-in for wasm32 (no threads in Glacier v1). */

typedef int pthread_t;
typedef int pthread_mutex_t;
typedef int pthread_cond_t;
typedef int pthread_key_t;
typedef int pthread_once_t;

#define PTHREAD_ONCE_INIT 0
#define PTHREAD_MUTEX_INITIALIZER 0
#define PTHREAD_COND_INITIALIZER 0

static void *glacier_wasm_tls[16];
static int glacier_wasm_next_key = 1;

static inline int pthread_mutex_init(pthread_mutex_t *m, const void *a) {
    (void)a;
    if (m) *m = 0;
    return 0;
}
static inline int pthread_mutex_destroy(pthread_mutex_t *m) {
    (void)m;
    return 0;
}
static inline int pthread_mutex_lock(pthread_mutex_t *m) {
    (void)m;
    return 0;
}
static inline int pthread_mutex_unlock(pthread_mutex_t *m) {
    (void)m;
    return 0;
}
static inline int pthread_cond_init(pthread_cond_t *c, const void *a) {
    (void)a;
    if (c) *c = 0;
    return 0;
}
static inline int pthread_cond_destroy(pthread_cond_t *c) {
    (void)c;
    return 0;
}
static inline int pthread_cond_wait(pthread_cond_t *c, pthread_mutex_t *m) {
    (void)c;
    (void)m;
    return 0;
}
static inline int pthread_cond_signal(pthread_cond_t *c) {
    (void)c;
    return 0;
}
static inline int pthread_cond_broadcast(pthread_cond_t *c) {
    (void)c;
    return 0;
}
static inline int pthread_key_create(pthread_key_t *k, void (*dtor)(void *)) {
    (void)dtor;
    if (!k) return 1;
    if (glacier_wasm_next_key >= 16) return 1;
    *k = glacier_wasm_next_key++;
    return 0;
}
static inline int pthread_key_delete(pthread_key_t k) {
    (void)k;
    return 0;
}
static inline void *pthread_getspecific(pthread_key_t k) {
    if (k <= 0 || k >= 16) return 0;
    return glacier_wasm_tls[k];
}
static inline int pthread_setspecific(pthread_key_t k, const void *p) {
    if (k <= 0 || k >= 16) return 1;
    glacier_wasm_tls[k] = (void *)p;
    return 0;
}
static inline int pthread_once(pthread_once_t *once, void (*init)(void)) {
    if (!once || !init) return 1;
    if (*once == 0) {
        init();
        *once = 1;
    }
    return 0;
}
static inline int pthread_create(pthread_t *t, const void *a, void *(*fn)(void *), void *arg) {
    (void)a;
    (void)fn;
    (void)arg;
    if (t) *t = 0;
    return 1;
}
static inline int pthread_join(pthread_t t, void **r) {
    (void)t;
    (void)r;
    return 0;
}

#endif
