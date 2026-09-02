#ifndef GLACIER_H
#define GLACIER_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define GLACIER_API_VERSION 1

/* Arrow C Data Interface (https://arrow.apache.org/docs/format/CDataInterface.html).
 * Guard matches the official header so Python/Polars can include either. */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
    const char *format;
    const char *name;
    const char *metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema **children;
    struct ArrowSchema *dictionary;
    void (*release)(struct ArrowSchema *);
    void *private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void **buffers;
    struct ArrowArray **children;
    struct ArrowArray *dictionary;
    void (*release)(struct ArrowArray *);
    void *private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

typedef struct GlacierDatabase GlacierDatabase;
typedef struct GlacierConn GlacierConn;
typedef struct GlacierResult GlacierResult;

/* Opaque handles. One connection = one thread (v1). Do not share a Conn across threads.
 *
 * glacier_open / glacier_connect: on failure return NULL and set *err (caller glacier_free).
 * glacier_query: NULL only for missing args / OOM (then *err is set). SQL and engine errors
 * return a Result; check glacier_result_error (owned by the Result, do not glacier_free).
 */
/* path NULL = empty session (SELECT 1; attach bytes with glacier_open_buffer). */
GlacierDatabase *glacier_open(const char *path, char **err);
void glacier_close(GlacierDatabase *db);

/* Copy `n` bytes and open as a parquet table. */
GlacierDatabase *glacier_open_buffer(const void *buf, size_t n, char **err);

/* Sized heap for WASM (JS copies a Uint8Array into linear memory). */
void *glacier_malloc(size_t n);
void glacier_malloc_free(void *p, size_t n);

GlacierConn *glacier_connect(GlacierDatabase *db, char **err);
void glacier_disconnect(GlacierConn *conn);

GlacierResult *glacier_query(GlacierConn *conn, const char *sql, char **err);
void glacier_result_destroy(GlacierResult *result);
const char *glacier_result_error(GlacierResult *result);

/* Fills Arrow C Data. Caller must ArrowArray.release / ArrowSchema.release when non-NULL. */
int glacier_result_arrow(GlacierResult *result, struct ArrowArray *array, struct ArrowSchema *schema);

const char *glacier_version(void);
int glacier_api_version(void);

void glacier_free(void *p);

#ifdef __cplusplus
}
#endif

#endif /* GLACIER_H */
