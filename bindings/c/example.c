/* C ABI smoke. No Zig in this file.
 * Usage: glacier-c-smoke <parquet>   (default SQL: SELECT *)
 */
#include "glacier.h"

#include <stdio.h>
#include <stdint.h>

int main(int argc, char **argv) {
    char *err = NULL;
    const char *path = argc > 1 ? argv[1] : "tests/formats/sales.parquet";

    GlacierDatabase *db = glacier_open(path, &err);
    if (!db) {
        fprintf(stderr, "%s\n", err ? err : "open failed");
        glacier_free(err);
        return 1;
    }

    GlacierConn *conn = glacier_connect(db, &err);
    GlacierResult *result = glacier_query(conn, "SELECT *", &err);
    if (!result || glacier_result_error(result)) {
        fprintf(stderr, "%s\n", result ? glacier_result_error(result) : (err ? err : "query failed"));
        if (result) glacier_result_destroy(result);
        glacier_free(err);
        glacier_close(db);
        return 1;
    }

    struct ArrowArray array = {0};
    struct ArrowSchema schema = {0};
    if (glacier_result_arrow(result, &array, &schema) != 0) return 1;
    if (schema.n_children < 1 || array.length != 10) return 1;

    const struct ArrowArray *id = array.children[0];
    const int64_t *ids = (const int64_t *)id->buffers[1];
    int64_t i;
    for (i = 0; i < array.length; i++) printf("%lld\n", (long long)ids[i]);
    if (ids[0] != 1 || ids[9] != 10) return 1;

    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
    glacier_result_destroy(result);
    glacier_disconnect(conn);
    glacier_close(db);
    return 0;
}
