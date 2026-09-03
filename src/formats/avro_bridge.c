#define _GNU_SOURCE
#include "visibility_hidden.h"
#include <avro.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    GAVRO_BOOL = 0,
    GAVRO_INT32 = 1,
    GAVRO_INT64 = 2,
    GAVRO_DOUBLE = 5,
    GAVRO_UTF8 = 6,
};

typedef struct glacier_avro_bound {
    int32_t field_id;
    uint8_t *bytes;
    size_t len;
} glacier_avro_bound;

typedef struct glacier_avro_data_file {
    char *path;
    char *format;
    int32_t status;
    int32_t content;
    int64_t record_count;
    glacier_avro_bound *lower;
    size_t n_lower;
    glacier_avro_bound *upper;
    size_t n_upper;
} glacier_avro_data_file;

typedef struct {
    int type;
    char *name;
    uint8_t *bools;
    int32_t *i32s;
    int64_t *i64s;
    double *f64s;
    char **strs;
    int32_t *str_lens;
} gavro_col;

typedef struct glacier_avro_reader {
    gavro_col *cols;
    int n_cols;
    int64_t n_rows;
} glacier_avro_reader;

static void set_err(char *err, size_t err_len, const char *msg) {
    if (!err || err_len == 0) return;
    snprintf(err, err_len, "%s", msg ? msg : "avro error");
}

static char *dup_str(const char *s) {
    if (!s) s = "";
    size_t n = strlen(s) + 1;
    char *out = malloc(n);
    if (out) memcpy(out, s, n);
    return out;
}

static char *dup_n(const void *p, size_t n) {
    char *out = malloc(n + 1);
    if (!out) return NULL;
    if (n) memcpy(out, p, n);
    out[n] = 0;
    return out;
}

static avro_schema_t unwrap_schema(avro_schema_t s) {
    int guard = 0;
    while (s && guard++ < 16) {
        if (is_avro_link(s)) {
            s = avro_schema_link_target(s);
            continue;
        }
        if (is_avro_union(s)) {
            size_t n = avro_schema_union_size(s);
            avro_schema_t chosen = NULL;
            for (size_t i = 0; i < n; i++) {
                avro_schema_t b = avro_schema_union_branch(s, (int)i);
                if (b && !is_avro_null(b)) {
                    if (chosen) return NULL;
                    chosen = b;
                }
            }
            s = chosen;
            continue;
        }
        break;
    }
    return s;
}

static int unwrap_value(const avro_value_t *in, avro_value_t *out, avro_type_t *type) {
    avro_value_t cur = *in;
    for (int guard = 0; guard < 16; guard++) {
        avro_type_t t = avro_value_get_type(&cur);
        if (t == AVRO_UNION) {
            avro_value_t branch;
            if (avro_value_get_current_branch(&cur, &branch)) return -1;
            cur = branch;
            continue;
        }
        *type = t;
        *out = cur;
        return 0;
    }
    return -1;
}

static int field_value(const avro_value_t *rec, const char *name, avro_value_t *out, avro_type_t *type) {
    avro_value_t child;
    if (avro_value_get_by_name(rec, name, &child, NULL)) return -1;
    return unwrap_value(&child, out, type);
}

static int get_string_field(const avro_value_t *rec, const char *name, char **out) {
    avro_value_t v;
    avro_type_t t;
    if (field_value(rec, name, &v, &t)) return -1;
    if (t == AVRO_NULL) {
        *out = dup_str("");
        return *out ? 0 : -1;
    }
    if (t != AVRO_STRING) return -1;
    const char *s = NULL;
    size_t n = 0;
    if (avro_value_get_string(&v, &s, &n)) return -1;
    if (n > 0) n -= 1;
    *out = dup_n(s ? s : "", n);
    return *out ? 0 : -1;
}

static int get_i32_field(const avro_value_t *rec, const char *name, int32_t *out, int32_t dflt) {
    avro_value_t v;
    avro_type_t t;
    if (field_value(rec, name, &v, &t)) {
        *out = dflt;
        return 0;
    }
    if (t == AVRO_NULL) {
        *out = dflt;
        return 0;
    }
    if (t == AVRO_INT32) return avro_value_get_int(&v, out) ? -1 : 0;
    if (t == AVRO_INT64) {
        int64_t x;
        if (avro_value_get_long(&v, &x)) return -1;
        *out = (int32_t)x;
        return 0;
    }
    *out = dflt;
    return 0;
}

static int get_i64_field(const avro_value_t *rec, const char *name, int64_t *out, int64_t dflt) {
    avro_value_t v;
    avro_type_t t;
    if (field_value(rec, name, &v, &t)) {
        *out = dflt;
        return 0;
    }
    if (t == AVRO_NULL) {
        *out = dflt;
        return 0;
    }
    if (t == AVRO_INT64) return avro_value_get_long(&v, out) ? -1 : 0;
    if (t == AVRO_INT32) {
        int32_t x;
        if (avro_value_get_int(&v, &x)) return -1;
        *out = x;
        return 0;
    }
    *out = dflt;
    return 0;
}

static int schema_col_type(avro_schema_t s) {
    s = unwrap_schema(s);
    if (!s) return -1;
    if (is_avro_boolean(s)) return GAVRO_BOOL;
    if (is_avro_int32(s)) return GAVRO_INT32;
    if (is_avro_int64(s)) return GAVRO_INT64;
    if (is_avro_float(s) || is_avro_double(s)) return GAVRO_DOUBLE;
    if (is_avro_string(s) || is_avro_bytes(s)) return GAVRO_UTF8;
    return -1;
}

static int open_mem_reader(const void *buf, size_t size, avro_file_reader_t *out, FILE **fp_out, char *err, size_t err_len) {
    if (!buf || size == 0) {
        set_err(err, err_len, "empty avro buffer");
        return -1;
    }
    FILE *fp = fmemopen((void *)buf, size, "rb");
    if (!fp) {
        set_err(err, err_len, "fmemopen failed");
        return -1;
    }
    if (avro_file_reader_fp(fp, "memory.avro", 1, out)) {
        set_err(err, err_len, avro_strerror());
        return -1;
    }
    *fp_out = fp;
    return 0;
}

static glacier_avro_bound *read_bounds(const avro_value_t *data_file, const char *name, size_t *n_out) {
    avro_value_t raw;
    avro_type_t t;
    *n_out = 0;
    if (field_value(data_file, name, &raw, &t)) return NULL;
    if (t == AVRO_NULL) return NULL;

    size_t n = 0;
    if (avro_value_get_size(&raw, &n) || n == 0) return NULL;
    glacier_avro_bound *out = calloc(n, sizeof(*out));
    if (!out) return NULL;
    size_t kept = 0;
    for (size_t i = 0; i < n; i++) {
        avro_value_t elem;
        const char *key = NULL;
        if (avro_value_get_by_index(&raw, i, &elem, &key)) continue;
        avro_value_t val = elem;
        avro_type_t et = avro_value_get_type(&elem);
        int32_t field_id = 0;
        const void *bytes = NULL;
        size_t blen = 0;

        if (et == AVRO_RECORD) {
            avro_value_t k, v;
            avro_type_t kt, vt;
            if (field_value(&elem, "key", &k, &kt) == 0 && kt == AVRO_INT32) {
                avro_value_get_int(&k, &field_id);
            } else if (key) {
                field_id = (int32_t)atoi(key);
            }
            if (field_value(&elem, "value", &v, &vt) == 0) {
                if (vt == AVRO_BYTES) avro_value_get_bytes(&v, &bytes, &blen);
            }
        } else {
            if (key) field_id = (int32_t)atoi(key);
            avro_type_t vt;
            if (unwrap_value(&elem, &val, &vt) == 0 && vt == AVRO_BYTES) {
                avro_value_get_bytes(&val, &bytes, &blen);
            }
        }
        if (field_id == 0 || !bytes) continue;
        out[kept].field_id = field_id;
        out[kept].bytes = malloc(blen);
        if (!out[kept].bytes) continue;
        memcpy(out[kept].bytes, bytes, blen);
        out[kept].len = blen;
        kept++;
    }
    *n_out = kept;
    return out;
}

static void free_bounds(glacier_avro_bound *b, size_t n) {
    if (!b) return;
    for (size_t i = 0; i < n; i++) free(b[i].bytes);
    free(b);
}

GLACIER_INTERNAL void glacier_avro_free_iceberg_entries(glacier_avro_data_file *files, size_t n) {
    if (!files) return;
    for (size_t i = 0; i < n; i++) {
        free(files[i].path);
        free(files[i].format);
        free_bounds(files[i].lower, files[i].n_lower);
        free_bounds(files[i].upper, files[i].n_upper);
    }
    free(files);
}

GLACIER_INTERNAL void glacier_avro_free_strings(char **s, size_t n) {
    if (!s) return;
    for (size_t i = 0; i < n; i++) free(s[i]);
    free(s);
}

static int fill_data_file(const avro_value_t *rec, glacier_avro_data_file *dst) {
    memset(dst, 0, sizeof(*dst));
    avro_value_t data_file = *rec;
    avro_value_t nested;
    avro_type_t t;
    if (field_value(rec, "data_file", &nested, &t) == 0 && t == AVRO_RECORD) {
        data_file = nested;
        get_i32_field(rec, "status", &dst->status, 1);
    } else {
        dst->status = 1;
        get_i32_field(rec, "status", &dst->status, 1);
    }
    get_i32_field(&data_file, "content", &dst->content, 0);
    if (get_string_field(&data_file, "file_path", &dst->path)) {
        if (get_string_field(rec, "manifest_path", &dst->path)) return -1;
    }
    if (get_string_field(&data_file, "file_format", &dst->format)) {
        dst->format = dup_str("PARQUET");
    }
    get_i64_field(&data_file, "record_count", &dst->record_count, 0);
    dst->lower = read_bounds(&data_file, "lower_bounds", &dst->n_lower);
    dst->upper = read_bounds(&data_file, "upper_bounds", &dst->n_upper);
    return dst->path ? 0 : -1;
}

GLACIER_INTERNAL int glacier_avro_read_iceberg_entries(
    const void *buf,
    size_t size,
    glacier_avro_data_file **out,
    size_t *out_n,
    char *err,
    size_t err_len
) {
    *out = NULL;
    *out_n = 0;
    avro_file_reader_t reader;
    FILE *fp = NULL;
    if (open_mem_reader(buf, size, &reader, &fp, err, err_len)) return -1;

    avro_schema_t schema = avro_file_reader_get_writer_schema(reader);
    avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(iface, &value);

    glacier_avro_data_file *files = NULL;
    size_t n = 0, cap = 0;
    int rval;
    while ((rval = avro_file_reader_read_value(reader, &value)) == 0) {
        if (n == cap) {
            cap = cap ? cap * 2 : 4;
            glacier_avro_data_file *next = realloc(files, cap * sizeof(*files));
            if (!next) {
                rval = ENOMEM;
                break;
            }
            files = next;
        }
        if (fill_data_file(&value, &files[n]) == 0) n++;
        avro_value_reset(&value);
    }

    avro_file_reader_close(reader);
    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avro_schema_decref(schema);

    if (rval != EOF) {
        glacier_avro_free_iceberg_entries(files, n);
        set_err(err, err_len, avro_strerror());
        return -1;
    }
    *out = files;
    *out_n = n;
    return 0;
}

GLACIER_INTERNAL int glacier_avro_collect_field_strings(
    const void *buf,
    size_t size,
    const char *field_name,
    char ***out,
    size_t *out_n,
    char *err,
    size_t err_len
) {
    *out = NULL;
    *out_n = 0;
    avro_file_reader_t reader;
    FILE *fp = NULL;
    if (open_mem_reader(buf, size, &reader, &fp, err, err_len)) return -1;

    avro_schema_t schema = avro_file_reader_get_writer_schema(reader);
    avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(iface, &value);

    char **paths = NULL;
    size_t n = 0, cap = 0;
    int rval;
    while ((rval = avro_file_reader_read_value(reader, &value)) == 0) {
        char *s = NULL;
        if (get_string_field(&value, field_name, &s) == 0 && s && s[0]) {
            if (n == cap) {
                cap = cap ? cap * 2 : 4;
                char **next = realloc(paths, cap * sizeof(*paths));
                if (!next) {
                    free(s);
                    rval = ENOMEM;
                    break;
                }
                paths = next;
            }
            paths[n++] = s;
        } else {
            free(s);
        }
        avro_value_reset(&value);
    }

    avro_file_reader_close(reader);
    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avro_schema_decref(schema);

    if (rval != EOF) {
        glacier_avro_free_strings(paths, n);
        set_err(err, err_len, avro_strerror());
        return -1;
    }
    *out = paths;
    *out_n = n;
    return 0;
}

static void free_reader(glacier_avro_reader *r) {
    if (!r) return;
    for (int i = 0; i < r->n_cols; i++) {
        free(r->cols[i].name);
        free(r->cols[i].bools);
        free(r->cols[i].i32s);
        free(r->cols[i].i64s);
        free(r->cols[i].f64s);
        if (r->cols[i].strs) {
            for (int64_t row = 0; row < r->n_rows; row++) free(r->cols[i].strs[row]);
        }
        free(r->cols[i].strs);
        free(r->cols[i].str_lens);
    }
    free(r->cols);
    free(r);
}

GLACIER_INTERNAL void glacier_avro_close(void *reader) {
    free_reader((glacier_avro_reader *)reader);
}

static int grow_col(gavro_col *col, int64_t n) {
    if (n == 0) return 0;
    switch (col->type) {
    case GAVRO_BOOL:
        col->bools = realloc(col->bools, (size_t)n);
        return col->bools ? 0 : -1;
    case GAVRO_INT32:
        col->i32s = realloc(col->i32s, (size_t)n * sizeof(int32_t));
        return col->i32s ? 0 : -1;
    case GAVRO_INT64:
        col->i64s = realloc(col->i64s, (size_t)n * sizeof(int64_t));
        return col->i64s ? 0 : -1;
    case GAVRO_DOUBLE:
        col->f64s = realloc(col->f64s, (size_t)n * sizeof(double));
        return col->f64s ? 0 : -1;
    case GAVRO_UTF8:
        col->strs = realloc(col->strs, (size_t)n * sizeof(char *));
        col->str_lens = realloc(col->str_lens, (size_t)n * sizeof(int32_t));
        return (col->strs && col->str_lens) ? 0 : -1;
    default:
        return -1;
    }
}

static int store_value(gavro_col *col, int64_t row, const avro_value_t *v) {
    avro_value_t u;
    avro_type_t t;
    if (unwrap_value(v, &u, &t)) return -1;
    switch (col->type) {
    case GAVRO_BOOL: {
        int b = 0;
        if (t != AVRO_NULL && avro_value_get_boolean(&u, &b)) return -1;
        col->bools[row] = (uint8_t)(b ? 1 : 0);
        return 0;
    }
    case GAVRO_INT32: {
        int32_t x = 0;
        if (t == AVRO_INT32) avro_value_get_int(&u, &x);
        else if (t == AVRO_INT64) {
            int64_t y = 0;
            avro_value_get_long(&u, &y);
            x = (int32_t)y;
        }
        col->i32s[row] = x;
        return 0;
    }
    case GAVRO_INT64: {
        int64_t x = 0;
        if (t == AVRO_INT64) avro_value_get_long(&u, &x);
        else if (t == AVRO_INT32) {
            int32_t y = 0;
            avro_value_get_int(&u, &y);
            x = y;
        }
        col->i64s[row] = x;
        return 0;
    }
    case GAVRO_DOUBLE: {
        double x = 0;
        if (t == AVRO_DOUBLE) avro_value_get_double(&u, &x);
        else if (t == AVRO_FLOAT) {
            float f = 0;
            avro_value_get_float(&u, &f);
            x = f;
        } else if (t == AVRO_INT64) {
            int64_t y = 0;
            avro_value_get_long(&u, &y);
            x = (double)y;
        } else if (t == AVRO_INT32) {
            int32_t y = 0;
            avro_value_get_int(&u, &y);
            x = (double)y;
        }
        col->f64s[row] = x;
        return 0;
    }
    case GAVRO_UTF8: {
        const char *s = "";
        size_t n = 0;
        if (t == AVRO_STRING) {
            avro_value_get_string(&u, &s, &n);
            if (n > 0) n -= 1;
        } else if (t == AVRO_BYTES) {
            const void *p = NULL;
            avro_value_get_bytes(&u, &p, &n);
            s = p ? p : "";
        } else {
            n = 0;
            s = "";
        }
        col->strs[row] = dup_n(s, n);
        col->str_lens[row] = (int32_t)n;
        return col->strs[row] ? 0 : -1;
    }
    default:
        return -1;
    }
}

GLACIER_INTERNAL void *glacier_avro_open_buffer(const void *buf, size_t size, char *err, size_t err_len) {
    avro_file_reader_t reader;
    FILE *fp = NULL;
    if (open_mem_reader(buf, size, &reader, &fp, err, err_len)) return NULL;

    avro_schema_t schema = avro_file_reader_get_writer_schema(reader);
    avro_schema_t rec = unwrap_schema(schema);
    if (!rec || !is_avro_record(rec)) {
        set_err(err, err_len, "avro root is not a record");
        avro_file_reader_close(reader);
        avro_schema_decref(schema);
        return NULL;
    }

    glacier_avro_reader *out = calloc(1, sizeof(*out));
    if (!out) {
        avro_file_reader_close(reader);
        avro_schema_decref(schema);
        return NULL;
    }
    out->n_cols = (int)avro_schema_record_size(rec);
    out->cols = calloc((size_t)out->n_cols, sizeof(gavro_col));
    if (!out->cols) {
        free(out);
        avro_file_reader_close(reader);
        avro_schema_decref(schema);
        return NULL;
    }
    for (int i = 0; i < out->n_cols; i++) {
        const char *name = avro_schema_record_field_name(rec, i);
        avro_schema_t fs = avro_schema_record_field_get_by_index(rec, i);
        int ty = schema_col_type(fs);
        if (ty < 0) {
            set_err(err, err_len, "unsupported avro column type");
            free_reader(out);
            avro_file_reader_close(reader);
            avro_schema_decref(schema);
            return NULL;
        }
        out->cols[i].type = ty;
        out->cols[i].name = dup_str(name ? name : "");
    }

    avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(iface, &value);

    size_t cap = 0;
    int rval;
    while ((rval = avro_file_reader_read_value(reader, &value)) == 0) {
        if ((size_t)out->n_rows == cap) {
            cap = cap ? cap * 2 : 8;
            for (int i = 0; i < out->n_cols; i++) {
                if (grow_col(&out->cols[i], (int64_t)cap)) {
                    rval = ENOMEM;
                    break;
                }
            }
            if (rval == ENOMEM) break;
        }
        for (int i = 0; i < out->n_cols; i++) {
            avro_value_t child;
            if (avro_value_get_by_index(&value, (size_t)i, &child, NULL)) {
                rval = EINVAL;
                break;
            }
            if (store_value(&out->cols[i], out->n_rows, &child)) {
                rval = EINVAL;
                break;
            }
        }
        if (rval != 0) break;
        out->n_rows++;
        avro_value_reset(&value);
    }

    avro_file_reader_close(reader);
    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avro_schema_decref(schema);

    if (rval != EOF) {
        set_err(err, err_len, avro_strerror());
        free_reader(out);
        return NULL;
    }
    return out;
}

GLACIER_INTERNAL int64_t glacier_avro_num_rows(void *reader) {
    return reader ? ((glacier_avro_reader *)reader)->n_rows : 0;
}

GLACIER_INTERNAL int32_t glacier_avro_num_columns(void *reader) {
    return reader ? ((glacier_avro_reader *)reader)->n_cols : 0;
}

GLACIER_INTERNAL const char *glacier_avro_column_name(void *reader, int32_t index) {
    glacier_avro_reader *r = reader;
    if (!r || index < 0 || index >= r->n_cols) return NULL;
    return r->cols[index].name;
}

GLACIER_INTERNAL int glacier_avro_column_type(void *reader, int32_t index) {
    glacier_avro_reader *r = reader;
    if (!r || index < 0 || index >= r->n_cols) return -1;
    return r->cols[index].type;
}

GLACIER_INTERNAL int64_t glacier_avro_read_all(void *reader, int32_t col, void *out, int64_t max) {
    glacier_avro_reader *r = reader;
    if (!r || col < 0 || col >= r->n_cols || !out) return -1;
    int64_t n = r->n_rows;
    if (max < n) n = max;
    gavro_col *c = &r->cols[col];
    switch (c->type) {
    case GAVRO_BOOL:
        memcpy(out, c->bools, (size_t)n);
        return n;
    case GAVRO_INT32:
        memcpy(out, c->i32s, (size_t)n * sizeof(int32_t));
        return n;
    case GAVRO_INT64:
        memcpy(out, c->i64s, (size_t)n * sizeof(int64_t));
        return n;
    case GAVRO_DOUBLE:
        memcpy(out, c->f64s, (size_t)n * sizeof(double));
        return n;
    case GAVRO_UTF8: {
        typedef struct { uint8_t *data; int32_t length; } ba;
        ba *dst = out;
        for (int64_t i = 0; i < n; i++) {
            dst[i].data = (uint8_t *)c->strs[i];
            dst[i].length = c->str_lens[i];
        }
        return n;
    }
    default:
        return -1;
    }
}

static void write_le_i64(uint8_t out[8], int64_t v) {
    for (int i = 0; i < 8; i++) out[i] = (uint8_t)((v >> (8 * i)) & 0xff);
}

static int set_bound_map(avro_value_t *data_file, const char *name, int32_t field_id, int64_t v) {
    avro_value_t field, map, elem;
    if (avro_value_get_by_name(data_file, name, &field, NULL)) return -1;
    if (avro_value_set_branch(&field, 1, &map)) return -1;
    char key[16];
    snprintf(key, sizeof(key), "%d", field_id);
    if (avro_value_add(&map, key, &elem, NULL, NULL)) return -1;
    uint8_t bytes[8];
    write_le_i64(bytes, v);
    return avro_value_set_bytes(&elem, bytes, sizeof(bytes));
}

GLACIER_INTERNAL int glacier_avro_write_iceberg_manifest_list(const char *path, const char *manifest_path) {
    static const char schema_json[] =
        "{\"type\":\"record\",\"name\":\"manifest_file\",\"fields\":["
        "{\"name\":\"manifest_path\",\"type\":\"string\"},"
        "{\"name\":\"manifest_length\",\"type\":\"long\"}"
        "]}";
    avro_schema_t schema;
    if (avro_schema_from_json_length(schema_json, sizeof(schema_json) - 1, &schema)) return -1;
    avro_file_writer_t writer;
    if (avro_file_writer_create(path, schema, &writer)) {
        avro_schema_decref(schema);
        return -1;
    }
    avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
    avro_value_t rec;
    avro_generic_value_new(iface, &rec);
    avro_value_t f;
    avro_value_get_by_name(&rec, "manifest_path", &f, NULL);
    avro_value_set_string(&f, manifest_path);
    avro_value_get_by_name(&rec, "manifest_length", &f, NULL);
    avro_value_set_long(&f, 0);
    int rc = avro_file_writer_append_value(writer, &rec);
    avro_value_decref(&rec);
    avro_value_iface_decref(iface);
    avro_file_writer_close(writer);
    avro_schema_decref(schema);
    return rc ? -1 : 0;
}

GLACIER_INTERNAL int glacier_avro_write_iceberg_manifest(
    const char *path,
    const char **file_paths,
    const int64_t *counts,
    const int64_t *lower,
    const int64_t *upper,
    int n,
    int32_t bound_field_id,
    const int32_t *contents
) {
    static const char schema_json[] =
        "{\"type\":\"record\",\"name\":\"manifest_entry\",\"fields\":["
        "{\"name\":\"status\",\"type\":\"int\"},"
        "{\"name\":\"data_file\",\"type\":{\"type\":\"record\",\"name\":\"r2\",\"fields\":["
        "{\"name\":\"content\",\"type\":\"int\"},"
        "{\"name\":\"file_path\",\"type\":\"string\"},"
        "{\"name\":\"file_format\",\"type\":\"string\"},"
        "{\"name\":\"record_count\",\"type\":\"long\"},"
        "{\"name\":\"file_size_in_bytes\",\"type\":\"long\"},"
        "{\"name\":\"lower_bounds\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"bytes\"}]},"
        "{\"name\":\"upper_bounds\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"bytes\"}]}"
        "]}}"
        "]}";
    avro_schema_t schema;
    if (avro_schema_from_json_length(schema_json, sizeof(schema_json) - 1, &schema)) return -1;
    avro_file_writer_t writer;
    if (avro_file_writer_create(path, schema, &writer)) {
        avro_schema_decref(schema);
        return -1;
    }
    avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
    int rc = 0;
    for (int i = 0; i < n && rc == 0; i++) {
        avro_value_t rec, df, f;
        avro_generic_value_new(iface, &rec);
        avro_value_get_by_name(&rec, "status", &f, NULL);
        avro_value_set_int(&f, 1);
        avro_value_get_by_name(&rec, "data_file", &df, NULL);
        avro_value_get_by_name(&df, "content", &f, NULL);
        avro_value_set_int(&f, contents ? contents[i] : 0);
        avro_value_get_by_name(&df, "file_path", &f, NULL);
        avro_value_set_string(&f, file_paths[i]);
        avro_value_get_by_name(&df, "file_format", &f, NULL);
        avro_value_set_string(&f, "PARQUET");
        avro_value_get_by_name(&df, "record_count", &f, NULL);
        avro_value_set_long(&f, counts[i]);
        avro_value_get_by_name(&df, "file_size_in_bytes", &f, NULL);
        avro_value_set_long(&f, 0);
        if (bound_field_id != 0) {
            if (set_bound_map(&df, "lower_bounds", bound_field_id, lower[i])) rc = -1;
            if (set_bound_map(&df, "upper_bounds", bound_field_id, upper[i])) rc = -1;
        }
        if (rc == 0 && avro_file_writer_append_value(writer, &rec)) rc = -1;
        avro_value_decref(&rec);
    }
    avro_value_iface_decref(iface);
    avro_file_writer_close(writer);
    avro_schema_decref(schema);
    return rc;
}

GLACIER_INTERNAL int glacier_avro_write_sales_rows(
    const char *path,
    const int64_t *ids,
    const int64_t *prices,
    const char *const *cats,
    int n,
    const char *codec
) {
    static const char schema_json[] =
        "{\"type\":\"record\",\"name\":\"sales\",\"fields\":["
        "{\"name\":\"id\",\"type\":\"long\"},"
        "{\"name\":\"price\",\"type\":\"long\"},"
        "{\"name\":\"category\",\"type\":\"string\"}"
        "]}";
    avro_schema_t schema;
    if (avro_schema_from_json_length(schema_json, sizeof(schema_json) - 1, &schema)) return -1;
    avro_file_writer_t writer;
    const char *codec_name = (codec && codec[0]) ? codec : "null";
    if (avro_file_writer_create_with_codec(path, schema, &writer, codec_name, 0)) {
        avro_schema_decref(schema);
        return -1;
    }
    avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
    int rc = 0;
    for (int i = 0; i < n && rc == 0; i++) {
        avro_value_t rec, f;
        avro_generic_value_new(iface, &rec);
        avro_value_get_by_name(&rec, "id", &f, NULL);
        avro_value_set_long(&f, ids[i]);
        avro_value_get_by_name(&rec, "price", &f, NULL);
        avro_value_set_long(&f, prices[i]);
        avro_value_get_by_name(&rec, "category", &f, NULL);
        avro_value_set_string(&f, cats[i]);
        if (avro_file_writer_append_value(writer, &rec)) rc = -1;
        avro_value_decref(&rec);
    }
    avro_value_iface_decref(iface);
    avro_file_writer_close(writer);
    avro_schema_decref(schema);
    return rc;
}
