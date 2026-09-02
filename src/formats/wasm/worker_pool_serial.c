#include "reader/worker_pool.h"
#include <stdlib.h>

carquet_worker_pool_t *carquet_worker_pool_create(int32_t num_threads) {
    (void)num_threads;
    return calloc(1, sizeof(carquet_worker_pool_t));
}

void carquet_worker_pool_submit(carquet_worker_pool_t *pool, carquet_task_fn fn, void *arg) {
    (void)pool;
    if (fn) fn(arg);
}

void carquet_worker_pool_submit_batch(carquet_worker_pool_t *pool, carquet_task_fn fn, void **args, int32_t count) {
    (void)pool;
    int32_t i;
    for (i = 0; i < count; i++) {
        if (fn) fn(args ? args[i] : NULL);
    }
}

void carquet_worker_pool_wait(carquet_worker_pool_t *pool) {
    (void)pool;
}

void carquet_worker_pool_parallel_for(carquet_worker_pool_t *pool, carquet_task_fn fn, void **args, int32_t count) {
    carquet_worker_pool_submit_batch(pool, fn, args, count);
}

void carquet_worker_pool_destroy(carquet_worker_pool_t *pool) {
    free(pool);
}
