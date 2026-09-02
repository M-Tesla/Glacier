/* C query client. SQL comes from argv; the engine parses and executes it.
 * Usage: glacier-c-example <parquet-or-iceberg> <sql>
 */

#include "glacier.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void fail(GlacierDatabase *db, GlacierConn *conn, GlacierResult *result, char *err, const char *what) {
    fprintf(stderr, "%s: %s\n", what, err ? err : "unknown");
    glacier_free(err);
    if (result) glacier_result_destroy(result);
    if (conn) glacier_disconnect(conn);
    if (db) glacier_close(db);
}

static const char *fmt_of(const struct ArrowSchema *s) {
    return (s && s->format) ? s->format : "";
}

static void print_cell(const struct ArrowSchema *schema, const struct ArrowArray *array, int64_t row) {
    const char *fmt = fmt_of(schema);
    const int64_t i = row + array->offset;
    if (!array->buffers) {
        printf("?");
        return;
    }

    if (strcmp(fmt, "l") == 0 && array->n_buffers >= 2 && array->buffers[1]) {
        printf("%lld", (long long)((const int64_t *)array->buffers[1])[i]);
        return;
    }
    if (strcmp(fmt, "i") == 0 && array->n_buffers >= 2 && array->buffers[1]) {
        printf("%d", ((const int32_t *)array->buffers[1])[i]);
        return;
    }
    if (strcmp(fmt, "g") == 0 && array->n_buffers >= 2 && array->buffers[1]) {
        printf("%g", ((const double *)array->buffers[1])[i]);
        return;
    }
    if (strcmp(fmt, "f") == 0 && array->n_buffers >= 2 && array->buffers[1]) {
        printf("%g", (double)((const float *)array->buffers[1])[i]);
        return;
    }
    if (strcmp(fmt, "b") == 0 && array->n_buffers >= 2 && array->buffers[1]) {
        const uint8_t *bits = (const uint8_t *)array->buffers[1];
        printf("%d", (bits[i / 8] >> (i % 8)) & 1);
        return;
    }
    if (strcmp(fmt, "u") == 0 && array->n_buffers >= 3 && array->buffers[1] && array->buffers[2]) {
        const int32_t *off = (const int32_t *)array->buffers[1];
        const char *bytes = (const char *)array->buffers[2];
        const int32_t start = off[i];
        const int32_t n = off[i + 1] - start;
        fwrite(bytes + start, 1, (size_t)n, stdout);
        return;
    }
    printf("<%s>", fmt);
}

static void print_table(const struct ArrowSchema *schema, const struct ArrowArray *array) {
    const int64_t n_cols = schema->n_children;
    int64_t c, r;

    for (c = 0; c < n_cols; c++) {
        if (c) fputs(" | ", stdout);
        const struct ArrowSchema *col = (schema->children && schema->children[c]) ? schema->children[c] : NULL;
        fputs(col && col->name ? col->name : "?", stdout);
    }
    fputc('\n', stdout);

    for (r = 0; r < array->length; r++) {
        for (c = 0; c < n_cols; c++) {
            if (c) fputs(" | ", stdout);
            if (!array->children || !array->children[c] || !schema->children || !schema->children[c]) {
                fputc('?', stdout);
                continue;
            }
            print_cell(schema->children[c], array->children[c], r);
        }
        fputc('\n', stdout);
    }
    printf("%lld rows, %lld cols\n", (long long)array->length, (long long)n_cols);
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s <parquet-or-iceberg-dir> <sql>\n", argv[0]);
        fprintf(stderr, "example: %s tests/formats/sales.parquet \"SELECT category, COUNT(*) AS n GROUP BY category\"\n", argv[0]);
        return 2;
    }

    const char *path = argv[1];
    const char *sql = argv[2];
    char *err = NULL;

    GlacierDatabase *db = glacier_open(path, &err);
    if (!db) {
        fail(NULL, NULL, NULL, err, "open");
        return 1;
    }

    GlacierConn *conn = glacier_connect(db, &err);
    if (!conn) {
        fail(db, NULL, NULL, err, "connect");
        return 1;
    }

    GlacierResult *result = glacier_query(conn, sql, &err);
    if (!result) {
        fail(db, conn, NULL, err, "query");
        return 1;
    }
    if (glacier_result_error(result)) {
        fprintf(stderr, "query: %s\n", glacier_result_error(result));
        glacier_result_destroy(result);
        glacier_disconnect(conn);
        glacier_close(db);
        return 1;
    }

    struct ArrowArray array = {0};
    struct ArrowSchema schema = {0};
    if (glacier_result_arrow(result, &array, &schema) != 0) {
        fail(db, conn, result, NULL, "arrow");
        return 1;
    }

    print_table(&schema, &array);

    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
    glacier_result_destroy(result);
    glacier_disconnect(conn);
    glacier_close(db);
    return 0;
}
