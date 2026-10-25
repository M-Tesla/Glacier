#ifndef GLACIER_H
#define GLACIER_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define GLACIER_API_VERSION 2

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

/* Opaque handles. One connection = one thread (v1), except glacier_cancel, which
 * may be called from another thread on that same connection. Do not share a Conn
 * for query or fetch.
 *
 * glacier_open / glacier_connect: on failure return NULL and set *err (caller glacier_free).
 * glacier_query: NULL only for missing args / OOM (then *err is set). SQL and engine errors
 * return a Result; check glacier_result_error (owned by the Result, do not glacier_free).
 *
 * A database is a catalog session. glacier_open(path) is the shortcut:
 * NULL = empty session (SELECT 1; ATTACH more catalogs with SQL);
 * a parquet / avro / .glacier / Iceberg dir / Hadoop warehouse = one default catalog;
 * http(s):// that is not a data file = Iceberg REST Catalog (token from ICEBERG_TOKEN).
 * glacier_open_catalog sets URI / warehouse / token without env.
 */
GlacierDatabase *glacier_open(const char *path, char **err);
void glacier_close(GlacierDatabase *db);

/* Copy `n` bytes and open as a parquet table. */
GlacierDatabase *glacier_open_buffer(const void *buf, size_t n, char **err);

/* Iceberg REST Catalog. uri is required. warehouse and token may be NULL or "". */
GlacierDatabase *glacier_open_catalog(
    const char *uri,
    const char *warehouse,
    const char *token,
    char **err);

/* Sized heap for WASM (JS copies a Uint8Array into linear memory). */
void *glacier_malloc(size_t n);
void glacier_malloc_free(void *p, size_t n);

GlacierConn *glacier_connect(GlacierDatabase *db, char **err);
void glacier_disconnect(GlacierConn *conn);

/* Runs every statement in `sql`, in order, on this connection. Returns the last
 * result. A failure stops the rest; earlier statements keep their effect.
 * glacier_open(path) is still the one-catalog shortcut. */
GlacierResult *glacier_query(GlacierConn *conn, const char *sql, char **err);
void glacier_result_destroy(GlacierResult *result);
const char *glacier_result_error(GlacierResult *result);

/* glacier_result_arrow fills every row (one array). glacier_result_next_arrow
 * yields the next record batch: 1 = batch, 0 = end, -1 = error.
 * Caller must ArrowArray.release / ArrowSchema.release when non-NULL. */
int glacier_result_arrow(GlacierResult *result, struct ArrowArray *array, struct ArrowSchema *schema);
int glacier_result_next_arrow(GlacierResult *result, struct ArrowArray *array, struct ArrowSchema *schema);

/* Ask the running statement on this connection to stop. The next glacier_query
 * also fails once if cancel was requested while idle. The connection stays open. */
void glacier_cancel(GlacierConn *conn);

const char *glacier_version(void);
int glacier_api_version(void);

void glacier_free(void *p);

#ifdef __cplusplus
}
#endif

#endif /* GLACIER_H */
