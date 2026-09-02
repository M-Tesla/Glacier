/* N-API wrapper around include/glacier.h. No query engine here. */

#include "glacier.h"

#include <node_api.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    GlacierDatabase *db;
    GlacierConn *conn;
} Conn;

static napi_ref conn_ctor;

static void throw_err(napi_env env, const char *msg) {
    napi_throw_error(env, NULL, msg ? msg : "glacier error");
}

static int bit_at(const uint8_t *buf, int64_t i) {
    return (buf[i >> 3] >> (i & 7)) & 1;
}

static int valid_at(const struct ArrowArray *a, int64_t i) {
    if (a->null_count == 0 || a->n_buffers < 1 || !a->buffers || !a->buffers[0]) return 1;
    return bit_at((const uint8_t *)a->buffers[0], i);
}

static napi_value cell_at(napi_env env, const struct ArrowSchema *s, const struct ArrowArray *a, int64_t i) {
    napi_value out;
    if (!valid_at(a, i)) {
        napi_get_null(env, &out);
        return out;
    }
    const char *fmt = s->format ? s->format : "";
    if (fmt[0] == 'b') {
        const uint8_t *bits = (const uint8_t *)a->buffers[1];
        napi_get_boolean(env, bit_at(bits, i) != 0, &out);
        return out;
    }
    if (fmt[0] == 'i') {
        const int32_t *d = (const int32_t *)a->buffers[1];
        napi_create_int32(env, d[i], &out);
        return out;
    }
    if (fmt[0] == 'l') {
        const int64_t *d = (const int64_t *)a->buffers[1];
        napi_create_int64(env, d[i], &out);
        return out;
    }
    if (fmt[0] == 'f') {
        const float *d = (const float *)a->buffers[1];
        napi_create_double(env, (double)d[i], &out);
        return out;
    }
    if (fmt[0] == 'g') {
        const double *d = (const double *)a->buffers[1];
        napi_create_double(env, d[i], &out);
        return out;
    }
    if (fmt[0] == 'u') {
        const int32_t *off = (const int32_t *)a->buffers[1];
        const char *bytes = (const char *)a->buffers[2];
        int32_t start = off[i], end = off[i + 1];
        napi_create_string_utf8(env, bytes + start, (size_t)(end - start), &out);
        return out;
    }
    napi_get_null(env, &out);
    return out;
}

static napi_value rows_from_result(napi_env env, GlacierResult *result) {
    struct ArrowArray array;
    struct ArrowSchema schema;
    memset(&array, 0, sizeof(array));
    memset(&schema, 0, sizeof(schema));
    if (glacier_result_arrow(result, &array, &schema) != 0) {
        throw_err(env, "failed to export Arrow");
        return NULL;
    }
    napi_value rows;
    napi_create_array_with_length(env, (size_t)array.length, &rows);
    if (!schema.format || strcmp(schema.format, "+s") != 0) {
        if (array.release) array.release(&array);
        if (schema.release) schema.release(&schema);
        throw_err(env, "expected struct Arrow batch");
        return NULL;
    }
    int64_t n_cols = schema.n_children;
    int64_t r;
    for (r = 0; r < array.length; r++) {
        napi_value row;
        napi_create_array_with_length(env, (size_t)n_cols, &row);
        int64_t c;
        for (c = 0; c < n_cols; c++) {
            napi_value cell = cell_at(env, schema.children[c], array.children[c], r);
            napi_set_element(env, row, (uint32_t)c, cell);
        }
        napi_set_element(env, rows, (uint32_t)r, row);
    }
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
    return rows;
}

static napi_value query_sql(napi_env env, Conn *c, const char *sql) {
    char *err = NULL;
    GlacierResult *result = glacier_query(c->conn, sql, &err);
    if (!result) {
        throw_err(env, err ? err : "query failed");
        if (err) glacier_free(err);
        return NULL;
    }
    const char *qerr = glacier_result_error(result);
    if (qerr) {
        throw_err(env, qerr);
        glacier_result_destroy(result);
        return NULL;
    }
    napi_value rows = rows_from_result(env, result);
    glacier_result_destroy(result);
    return rows;
}

static void conn_finalize(napi_env env, void *data, void *hint) {
    (void)env;
    (void)hint;
    Conn *c = data;
    if (c && c->db) {
        glacier_disconnect(c->conn);
        glacier_close(c->db);
        c->db = NULL;
        c->conn = NULL;
    }
    free(c);
}

static napi_value wrap_db(napi_env env, GlacierDatabase *db) {
    char *err = NULL;
    GlacierConn *conn = glacier_connect(db, &err);
    if (!conn) {
        throw_err(env, err ? err : "connect failed");
        if (err) glacier_free(err);
        glacier_close(db);
        return NULL;
    }
    Conn *c = calloc(1, sizeof(*c));
    if (!c) {
        glacier_disconnect(conn);
        glacier_close(db);
        throw_err(env, "out of memory");
        return NULL;
    }
    c->db = db;
    c->conn = conn;

    napi_value ctor, js;
    napi_get_reference_value(env, conn_ctor, &ctor);
    napi_new_instance(env, ctor, 0, NULL, &js);
    napi_wrap(env, js, c, conn_finalize, NULL, NULL);
    return js;
}

static Conn *unwrap(napi_env env, napi_callback_info info, size_t *argc, napi_value *argv) {
    napi_value self;
    napi_get_cb_info(env, info, argc, argv, &self, NULL);
    Conn *c = NULL;
    napi_unwrap(env, self, (void **)&c);
    if (!c || !c->db) {
        throw_err(env, "connection is closed");
        return NULL;
    }
    return c;
}

static napi_value ConnExecute(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    Conn *c = unwrap(env, info, &argc, argv);
    if (!c) return NULL;
    if (argc < 1) {
        throw_err(env, "sql is required");
        return NULL;
    }
    size_t len = 0;
    napi_get_value_string_utf8(env, argv[0], NULL, 0, &len);
    char *sql = malloc(len + 1);
    if (!sql) {
        throw_err(env, "out of memory");
        return NULL;
    }
    napi_get_value_string_utf8(env, argv[0], sql, len + 1, &len);
    napi_value rows = query_sql(env, c, sql);
    free(sql);
    return rows;
}

static napi_value ConnReadParquet(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    Conn *c = unwrap(env, info, &argc, argv);
    if (!c) return NULL;
    if (argc < 1) {
        throw_err(env, "buffer is required");
        return NULL;
    }
    void *data = NULL;
    size_t len = 0;
    napi_status st = napi_get_buffer_info(env, argv[0], &data, &len);
    if (st != napi_ok || !data) {
        throw_err(env, "readParquet expects a Buffer");
        return NULL;
    }
    char *err = NULL;
    GlacierDatabase *db = glacier_open_buffer(data, len, &err);
    if (!db) {
        throw_err(env, err ? err : "open buffer failed");
        if (err) glacier_free(err);
        return NULL;
    }
    glacier_disconnect(c->conn);
    glacier_close(c->db);
    GlacierConn *conn = glacier_connect(db, &err);
    if (!conn) {
        throw_err(env, err ? err : "connect failed");
        if (err) glacier_free(err);
        glacier_close(db);
        c->db = NULL;
        c->conn = NULL;
        return NULL;
    }
    c->db = db;
    c->conn = conn;
    return query_sql(env, c, "SELECT *");
}

static napi_value ConnClose(napi_env env, napi_callback_info info) {
    size_t argc = 0;
    napi_value self;
    napi_get_cb_info(env, info, &argc, NULL, &self, NULL);
    Conn *c = NULL;
    napi_unwrap(env, self, (void **)&c);
    if (c && c->db) {
        glacier_disconnect(c->conn);
        glacier_close(c->db);
        c->db = NULL;
        c->conn = NULL;
    }
    napi_value undef;
    napi_get_undefined(env, &undef);
    return undef;
}

static napi_value ConnCtor(napi_env env, napi_callback_info info) {
    napi_value self;
    napi_get_cb_info(env, info, NULL, NULL, &self, NULL);
    return self;
}

static napi_value Connect(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    char *err = NULL;
    GlacierDatabase *db = NULL;
    if (argc >= 1) {
        napi_valuetype t;
        napi_typeof(env, argv[0], &t);
        if (t != napi_undefined && t != napi_null) {
            size_t len = 0;
            napi_get_value_string_utf8(env, argv[0], NULL, 0, &len);
            char *path = malloc(len + 1);
            if (!path) {
                throw_err(env, "out of memory");
                return NULL;
            }
            napi_get_value_string_utf8(env, argv[0], path, len + 1, &len);
            db = glacier_open(path, &err);
            free(path);
        }
    }
    if (!db && !err) db = glacier_open(NULL, &err);
    if (!db) {
        throw_err(env, err ? err : "open failed");
        if (err) glacier_free(err);
        return NULL;
    }
    return wrap_db(env, db);
}

static napi_value Version(napi_env env, napi_callback_info info) {
    (void)info;
    napi_value v;
    napi_create_string_utf8(env, glacier_version(), NAPI_AUTO_LENGTH, &v);
    return v;
}

static napi_value ApiVersion(napi_env env, napi_callback_info info) {
    (void)info;
    napi_value v;
    napi_create_int32(env, glacier_api_version(), &v);
    return v;
}

static napi_value Init(napi_env env, napi_value exports) {
    napi_property_descriptor conn_props[] = {
        {"execute", NULL, ConnExecute, NULL, NULL, NULL, napi_default, NULL},
        {"readParquet", NULL, ConnReadParquet, NULL, NULL, NULL, napi_default, NULL},
        {"close", NULL, ConnClose, NULL, NULL, NULL, napi_default, NULL},
    };
    napi_value ctor;
    napi_define_class(env, "Connection", NAPI_AUTO_LENGTH, ConnCtor, NULL, 3, conn_props, &ctor);
    napi_create_reference(env, ctor, 1, &conn_ctor);

    napi_property_descriptor desc[] = {
        {"connect", NULL, Connect, NULL, NULL, NULL, napi_default, NULL},
        {"version", NULL, Version, NULL, NULL, NULL, napi_default, NULL},
        {"apiVersion", NULL, ApiVersion, NULL, NULL, NULL, napi_default, NULL},
    };
    napi_define_properties(env, exports, 3, desc);
    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
