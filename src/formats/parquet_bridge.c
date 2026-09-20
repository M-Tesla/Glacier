#include "visibility_hidden.h"
#include <carquet/carquet.h>
#include "reader/reader_internal.h"
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct glacier_logical {
    int32_t id;
    int32_t precision;
    int32_t scale;
    int32_t time_unit;
    int32_t utc;
    int32_t type_length;
} glacier_logical_t;

static const parquet_schema_element_t *glacier_leaf_elem(void *reader, int32_t index) {
    const carquet_schema_t *schema = carquet_reader_schema((carquet_reader_t *)reader);
    if (!schema || index < 0 || index >= schema->num_leaves) return NULL;
    return &schema->elements[schema->leaf_indices[index]];
}

GLACIER_INTERNAL int glacier_carquet_init(void) {
    return (int)carquet_init();
}

GLACIER_INTERNAL void glacier_carquet_cleanup(void) {
    carquet_cleanup();
}

GLACIER_INTERNAL void *glacier_carquet_open_buffer(const void *buf, size_t size, char *err_buf, size_t err_len) {
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_reader_t *reader = carquet_reader_open_buffer(buf, size, NULL, &err);
    if (!reader && err_buf && err_len > 0) {
        strncpy(err_buf, err.message, err_len - 1);
        err_buf[err_len - 1] = 0;
    }
    return reader;
}

GLACIER_INTERNAL void *glacier_carquet_open_path(const void *path, size_t path_len, char *err_buf, size_t err_len) {
    carquet_error_t err = CARQUET_ERROR_INIT;
    if (!path || path_len == 0 || path_len >= 4096) {
        if (err_buf && err_len > 0) {
            strncpy(err_buf, "Invalid path", err_len - 1);
            err_buf[err_len - 1] = 0;
        }
        return NULL;
    }
    char tmp[4096];
    memcpy(tmp, path, path_len);
    tmp[path_len] = 0;

    carquet_reader_options_t opts;
    carquet_reader_options_init(&opts);
    opts.use_mmap = true;
    opts.num_threads = 1;
    carquet_reader_t *reader = carquet_reader_open(tmp, &opts, &err);
    if (!reader && err_buf && err_len > 0) {
        strncpy(err_buf, err.message, err_len - 1);
        err_buf[err_len - 1] = 0;
    }
    return reader;
}

GLACIER_INTERNAL void *glacier_carquet_batch_open(
    void *reader,
    const int32_t *cols,
    int32_t n_cols,
    int32_t batch_size,
    char *err_buf,
    size_t err_len)
{
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_batch_reader_config_t cfg;
    carquet_batch_reader_config_init(&cfg);
    cfg.num_threads = 1;
    if (batch_size > 0) cfg.batch_size = batch_size;
    if (cols && n_cols > 0) {
        cfg.column_indices = cols;
        cfg.num_columns = n_cols;
    }
    carquet_batch_reader_t *br = carquet_batch_reader_create((carquet_reader_t *)reader, &cfg, &err);
    if (!br && err_buf && err_len > 0) {
        strncpy(err_buf, err.message, err_len - 1);
        err_buf[err_len - 1] = 0;
    }
    return br;
}

GLACIER_INTERNAL int glacier_carquet_batch_next(void *br, void **out_batch) {
    if (!out_batch) return -1;
    carquet_row_batch_t *batch = NULL;
    const carquet_status_t st = carquet_batch_reader_next((carquet_batch_reader_t *)br, &batch);
    if (st == CARQUET_ERROR_END_OF_DATA || (st == CARQUET_OK && !batch)) {
        *out_batch = NULL;
        return 0;
    }
    if (st != CARQUET_OK) {
        *out_batch = NULL;
        return -1;
    }
    *out_batch = batch;
    return 1;
}

GLACIER_INTERNAL void glacier_carquet_batch_close(void *br) {
    carquet_batch_reader_free((carquet_batch_reader_t *)br);
}

GLACIER_INTERNAL void glacier_carquet_row_batch_free(void *batch) {
    carquet_row_batch_free((carquet_row_batch_t *)batch);
}

GLACIER_INTERNAL int64_t glacier_carquet_rb_nrows(void *batch) {
    return carquet_row_batch_num_rows((const carquet_row_batch_t *)batch);
}

GLACIER_INTERNAL int glacier_carquet_rb_column(
    void *batch,
    int32_t index,
    const void **data,
    const uint8_t **nulls,
    int64_t *n)
{
    const uint8_t *bitmap = NULL;
    if (!data || !n) return -1;
    const carquet_status_t st = carquet_row_batch_column(
        (const carquet_row_batch_t *)batch,
        index,
        data,
        nulls ? nulls : &bitmap,
        n);
    return st == CARQUET_OK ? 0 : -1;
}

GLACIER_INTERNAL int glacier_carquet_rb_column_list(
    void *batch,
    int32_t index,
    const int32_t **offsets,
    int64_t *num_lists,
    const void **values,
    const uint8_t **value_validity,
    int64_t *num_values,
    const uint8_t **list_validity)
{
    if (!offsets || !num_lists || !values || !num_values) return -1;
    const carquet_status_t st = carquet_row_batch_column_list(
        (const carquet_row_batch_t *)batch,
        index,
        offsets,
        num_lists,
        values,
        value_validity,
        num_values,
        list_validity);
    return st == CARQUET_OK ? 0 : -1;
}

GLACIER_INTERNAL void glacier_carquet_close(void *reader) {
    carquet_reader_close((carquet_reader_t *)reader);
}

GLACIER_INTERNAL int64_t glacier_carquet_num_rows(void *reader) {
    return carquet_reader_num_rows((carquet_reader_t *)reader);
}

GLACIER_INTERNAL int32_t glacier_carquet_num_columns(void *reader) {
    return carquet_reader_num_columns((carquet_reader_t *)reader);
}

GLACIER_INTERNAL int32_t glacier_carquet_num_row_groups(void *reader) {
    return carquet_reader_num_row_groups((carquet_reader_t *)reader);
}

GLACIER_INTERNAL const char *glacier_carquet_column_name(void *reader, int32_t index) {
    const carquet_schema_t *schema = carquet_reader_schema((carquet_reader_t *)reader);
    return carquet_schema_column_name(schema, index);
}

GLACIER_INTERNAL int glacier_carquet_column_type(void *reader, int32_t index) {
    const carquet_schema_t *schema = carquet_reader_schema((carquet_reader_t *)reader);
    return (int)carquet_schema_column_type(schema, index);
}

GLACIER_INTERNAL int glacier_carquet_column_logical(void *reader, int32_t index, glacier_logical_t *out) {
    if (!out) return -1;
    memset(out, 0, sizeof(*out));
    const parquet_schema_element_t *elem = glacier_leaf_elem(reader, index);
    if (!elem) return -1;
    out->type_length = elem->type_length;
    if (elem->has_logical_type) {
        out->id = (int32_t)elem->logical_type.id;
        out->precision = elem->logical_type.params.decimal.precision;
        out->scale = elem->logical_type.params.decimal.scale;
        if (elem->logical_type.id == CARQUET_LOGICAL_TIMESTAMP) {
            out->time_unit = (int32_t)elem->logical_type.params.timestamp.unit;
            out->utc = elem->logical_type.params.timestamp.is_adjusted_to_utc ? 1 : 0;
        }
    }
    return 0;
}

typedef struct glacier_col_stats {
    int32_t has_min_max;
    int32_t has_null_count;
    int64_t null_count;
    int64_t num_values;
    int32_t min_len;
    int32_t max_len;
    uint8_t min_bytes[32];
    uint8_t max_bytes[32];
} glacier_col_stats_t;

GLACIER_INTERNAL int glacier_carquet_column_stats(
    void *reader,
    int32_t row_group,
    int32_t column,
    glacier_col_stats_t *out
) {
    if (!reader || !out) return -1;
    memset(out, 0, sizeof(*out));
    carquet_column_statistics_t st;
    if (carquet_reader_column_statistics((carquet_reader_t *)reader, row_group, column, &st) != CARQUET_OK)
        return -1;
    out->has_min_max = st.has_min_max ? 1 : 0;
    out->has_null_count = st.has_null_count ? 1 : 0;
    out->null_count = st.null_count;
    out->num_values = st.num_values;
    if (st.has_min_max && st.min_value && st.min_value_size > 0) {
        const int32_t n = st.min_value_size < 32 ? st.min_value_size : 32;
        memcpy(out->min_bytes, st.min_value, (size_t)n);
        out->min_len = n;
    }
    if (st.has_min_max && st.max_value && st.max_value_size > 0) {
        const int32_t n = st.max_value_size < 32 ? st.max_value_size : 32;
        memcpy(out->max_bytes, st.max_value, (size_t)n);
        out->max_len = n;
    }
    return 0;
}

GLACIER_INTERNAL int glacier_carquet_write_i64_fixture(const char *path, int compression) {
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_schema_t *schema = carquet_schema_create(&err);
    if (!schema) return -1;
    if (carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT64, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK) {
        carquet_schema_free(schema);
        return -1;
    }

    carquet_writer_options_t opts;
    carquet_writer_options_init(&opts);
    opts.compression = (carquet_compression_t)compression;
    opts.write_crc = false;
    opts.write_page_index = false;
    opts.write_bloom_filters = false;

    carquet_writer_t *writer = carquet_writer_create(path, schema, &opts, &err);
    carquet_schema_free(schema);
    if (!writer) return -1;

    const int64_t ids[] = {1, 2, 3, 4, 5};
    if (carquet_writer_write_batch(writer, 0, ids, 5, NULL, NULL) != CARQUET_OK) {
        (void)carquet_writer_close(writer);
        return -1;
    }
    return carquet_writer_close(writer) == CARQUET_OK ? 0 : -1;
}

GLACIER_INTERNAL int glacier_carquet_write_sales_fixture(const char *path) {
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_schema_t *schema = carquet_schema_create(&err);
    if (!schema) return -1;

    carquet_logical_type_t str_lt = {0};
    str_lt.id = CARQUET_LOGICAL_STRING;

    if (carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT64, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "price", CARQUET_PHYSICAL_INT64, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "category", CARQUET_PHYSICAL_BYTE_ARRAY, &str_lt,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK) {
        carquet_schema_free(schema);
        return -1;
    }

    carquet_writer_options_t opts;
    carquet_writer_options_init(&opts);
    opts.compression = CARQUET_COMPRESSION_SNAPPY;
    opts.write_crc = false;
    opts.write_page_index = false;
    opts.write_bloom_filters = false;

    carquet_writer_t *writer = carquet_writer_create(path, schema, &opts, &err);
    carquet_schema_free(schema);
    if (!writer) return -1;

    const int64_t ids[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    const int64_t prices[] = {50, 80, 100, 120, 150, 200, 90, 110, 300, 75};
    static const char *cats[] = {
        "fruit", "fruit", "veg", "veg", "fruit", "dairy", "fruit", "veg", "dairy", "fruit",
    };
    carquet_byte_array_t categories[10];
    for (int i = 0; i < 10; i++) {
        categories[i].data = (uint8_t *)cats[i];
        categories[i].length = (int32_t)strlen(cats[i]);
    }

    if (carquet_writer_write_batch(writer, 0, ids, 10, NULL, NULL) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 1, prices, 10, NULL, NULL) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 2, categories, 10, NULL, NULL) != CARQUET_OK) {
        (void)carquet_writer_close(writer);
        return -1;
    }
    return carquet_writer_close(writer) == CARQUET_OK ? 0 : -1;
}

GLACIER_INTERNAL int glacier_carquet_write_sales_rows(
    const char *path,
    const int64_t *ids,
    const int64_t *prices,
    const char *const *cats,
    int32_t n
) {
    if (n <= 0) return -1;
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_schema_t *schema = carquet_schema_create(&err);
    if (!schema) return -1;

    carquet_logical_type_t str_lt = {0};
    str_lt.id = CARQUET_LOGICAL_STRING;

    if (carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT64, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "price", CARQUET_PHYSICAL_INT64, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "category", CARQUET_PHYSICAL_BYTE_ARRAY, &str_lt,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK) {
        carquet_schema_free(schema);
        return -1;
    }

    carquet_writer_options_t opts;
    carquet_writer_options_init(&opts);
    opts.compression = CARQUET_COMPRESSION_SNAPPY;
    opts.write_crc = false;
    opts.write_page_index = false;
    opts.write_bloom_filters = false;

    carquet_writer_t *writer = carquet_writer_create(path, schema, &opts, &err);
    carquet_schema_free(schema);
    if (!writer) return -1;

    carquet_byte_array_t *categories = malloc((size_t)n * sizeof(*categories));
    if (!categories) {
        (void)carquet_writer_close(writer);
        return -1;
    }
    for (int32_t i = 0; i < n; i++) {
        categories[i].data = (uint8_t *)cats[i];
        categories[i].length = (int32_t)strlen(cats[i]);
    }

    const int ok =
        carquet_writer_write_batch(writer, 0, ids, n, NULL, NULL) == CARQUET_OK &&
        carquet_writer_write_batch(writer, 1, prices, n, NULL, NULL) == CARQUET_OK &&
        carquet_writer_write_batch(writer, 2, categories, n, NULL, NULL) == CARQUET_OK;
    free(categories);
    if (!ok) {
        (void)carquet_writer_close(writer);
        return -1;
    }
    return carquet_writer_close(writer) == CARQUET_OK ? 0 : -1;
}

GLACIER_INTERNAL int glacier_carquet_write_nulls_fixture(const char *path) {
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_schema_t *schema = carquet_schema_create(&err);
    if (!schema) return -1;

    carquet_logical_type_t str_lt = {0};
    str_lt.id = CARQUET_LOGICAL_STRING;

    if (carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT64, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "qty", CARQUET_PHYSICAL_INT64, NULL,
                                 CARQUET_REPETITION_OPTIONAL, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "note", CARQUET_PHYSICAL_BYTE_ARRAY, &str_lt,
                                 CARQUET_REPETITION_OPTIONAL, 0, 0) != CARQUET_OK) {
        carquet_schema_free(schema);
        return -1;
    }

    carquet_writer_options_t opts;
    carquet_writer_options_init(&opts);
    opts.compression = CARQUET_COMPRESSION_UNCOMPRESSED;
    opts.write_crc = false;
    opts.write_page_index = false;
    opts.write_bloom_filters = false;

    carquet_writer_t *writer = carquet_writer_create(path, schema, &opts, &err);
    carquet_schema_free(schema);
    if (!writer) return -1;

    const int64_t ids[] = {1, 2, 3, 4};
    const int64_t qtys[] = {10, 30};
    const int16_t qty_def[] = {1, 0, 1, 0};
    static const char *notes[] = {"a", "c", "d"};
    carquet_byte_array_t note_ba[3];
    for (int i = 0; i < 3; i++) {
        note_ba[i].data = (uint8_t *)notes[i];
        note_ba[i].length = (int32_t)strlen(notes[i]);
    }
    const int16_t note_def[] = {1, 0, 1, 1};

    if (carquet_writer_write_batch(writer, 0, ids, 4, NULL, NULL) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 1, qtys, 4, qty_def, NULL) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 2, note_ba, 4, note_def, NULL) != CARQUET_OK) {
        (void)carquet_writer_close(writer);
        return -1;
    }
    return carquet_writer_close(writer) == CARQUET_OK ? 0 : -1;
}

static size_t glacier_physical_size(void *reader, int32_t col) {
    const int t = glacier_carquet_column_type(reader, col);
    switch (t) {
        case CARQUET_PHYSICAL_BOOLEAN:
            return 1;
        case CARQUET_PHYSICAL_INT32:
        case CARQUET_PHYSICAL_FLOAT:
            return 4;
        case CARQUET_PHYSICAL_INT64:
        case CARQUET_PHYSICAL_DOUBLE:
            return 8;
        case CARQUET_PHYSICAL_BYTE_ARRAY:
            return sizeof(carquet_byte_array_t);
        case CARQUET_PHYSICAL_FIXED_LEN_BYTE_ARRAY: {
            const parquet_schema_element_t *elem = glacier_leaf_elem(reader, col);
            if (!elem || elem->type_length <= 0) return 0;
            return (size_t)elem->type_length;
        }
        default:
            return 0;
    }
}

static void glacier_free_cloned_bas(carquet_byte_array_t *bas, int64_t n) {
    if (!bas) return;
    for (int64_t i = 0; i < n; i++) {
        free(bas[i].data);
        bas[i].data = NULL;
        bas[i].length = 0;
    }
}

/* BYTE_ARRAY values point into the column reader's dictionary/page buffers.
 * Those die in carquet_column_reader_free, so copy payloads first. */
static int glacier_clone_byte_arrays(carquet_byte_array_t *bas, int64_t n) {
    for (int64_t i = 0; i < n; i++) {
        const int32_t len = bas[i].length;
        const uint8_t *src = bas[i].data;
        if (len <= 0 || src == NULL) {
            bas[i].data = NULL;
            if (len < 0) bas[i].length = 0;
            continue;
        }
        uint8_t *copy = malloc((size_t)len);
        if (!copy) {
            glacier_free_cloned_bas(bas, i);
            return -1;
        }
        memcpy(copy, src, (size_t)len);
        bas[i].data = copy;
    }
    return 0;
}

GLACIER_INTERNAL void glacier_carquet_free_byte_arrays(void *out, int64_t n) {
    if (n <= 0) return;
    glacier_free_cloned_bas((carquet_byte_array_t *)out, n);
}

GLACIER_INTERNAL int64_t glacier_carquet_read_all(void *reader, int32_t col, void *out, int64_t max) {
    carquet_reader_t *r = (carquet_reader_t *)reader;
    const size_t esize = glacier_physical_size(reader, col);
    if (esize == 0 || max < 0) return -1;

    const int is_ba = glacier_carquet_column_type(reader, col) == CARQUET_PHYSICAL_BYTE_ARRAY;
    int64_t total = 0;
    const int32_t nrg = carquet_reader_num_row_groups(r);
    uint8_t *dst = (uint8_t *)out;
    for (int32_t rg = 0; rg < nrg; rg++) {
        carquet_error_t err = CARQUET_ERROR_INIT;
        carquet_column_reader_t *cr = carquet_reader_get_column(r, rg, col, &err);
        if (!cr) {
            if (is_ba) glacier_free_cloned_bas((carquet_byte_array_t *)dst, total);
            return -1;
        }
        const int64_t cap = max - total;
        carquet_byte_array_t *batch = (carquet_byte_array_t *)(dst + (size_t)total * esize);
        const int64_t n = carquet_column_read_batch(cr, dst + (size_t)total * esize, cap, NULL, NULL);
        if (n < 0) {
            carquet_column_reader_free(cr);
            if (is_ba) glacier_free_cloned_bas((carquet_byte_array_t *)dst, total);
            return -1;
        }
        if (is_ba && glacier_clone_byte_arrays(batch, n) != 0) {
            carquet_column_reader_free(cr);
            glacier_free_cloned_bas((carquet_byte_array_t *)dst, total);
            return -1;
        }
        carquet_column_reader_free(cr);
        total += n;
        if (total >= max) break;
    }
    return total;
}

GLACIER_INTERNAL int64_t glacier_carquet_read_i64_prefix(void *reader, int32_t col, int64_t *out, int64_t max) {
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_column_reader_t *cr = carquet_reader_get_column((carquet_reader_t *)reader, 0, col, &err);
    if (!cr) return -1;
    const int t = glacier_carquet_column_type(reader, col);
    int64_t n = 0;
    if (t == CARQUET_PHYSICAL_INT64) {
        n = carquet_column_read_batch(cr, out, max, NULL, NULL);
    } else if (t == CARQUET_PHYSICAL_INT32) {
        int32_t tmp[64];
        const int64_t cap = max < 64 ? max : 64;
        n = carquet_column_read_batch(cr, tmp, cap, NULL, NULL);
        for (int64_t i = 0; i < n; i++) out[i] = tmp[i];
    } else {
        n = 0;
    }
    carquet_column_reader_free(cr);
    return n;
}

GLACIER_INTERNAL int16_t glacier_carquet_column_rep_level(void *reader, int32_t index) {
    const carquet_schema_t *schema = carquet_reader_schema((carquet_reader_t *)reader);
    return carquet_schema_max_rep_level(schema, index);
}

GLACIER_INTERNAL int glacier_carquet_write_types_fixture(const char *path) {
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_schema_t *schema = carquet_schema_create(&err);
    if (!schema) return -1;

    carquet_logical_type_t str_lt = {0};
    str_lt.id = CARQUET_LOGICAL_STRING;

    if (carquet_schema_add_column(schema, "flag", CARQUET_PHYSICAL_BOOLEAN, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "i32", CARQUET_PHYSICAL_INT32, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "i64", CARQUET_PHYSICAL_INT64, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "f32", CARQUET_PHYSICAL_FLOAT, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "f64", CARQUET_PHYSICAL_DOUBLE, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "name", CARQUET_PHYSICAL_BYTE_ARRAY, &str_lt,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK) {
        carquet_schema_free(schema);
        return -1;
    }

    carquet_writer_options_t opts;
    carquet_writer_options_init(&opts);
    opts.compression = CARQUET_COMPRESSION_UNCOMPRESSED;
    opts.write_crc = false;
    opts.write_page_index = false;
    opts.write_bloom_filters = false;

    carquet_writer_t *writer = carquet_writer_create(path, schema, &opts, &err);
    carquet_schema_free(schema);
    if (!writer) return -1;

    const uint8_t flags[] = {1, 0, 1};
    const int32_t i32s[] = {1, 2, 3};
    const int64_t i64s[] = {10, 20, 30};
    const float f32s[] = {1.5f, 2.5f, 3.5f};
    const double f64s[] = {1.25, 2.25, 3.25};
    static const char *names[] = {"a", "b", "c"};
    carquet_byte_array_t bas[3];
    int i;
    for (i = 0; i < 3; i++) {
        bas[i].data = (uint8_t *)names[i];
        bas[i].length = (int32_t)strlen(names[i]);
    }

    if (carquet_writer_write_batch(writer, 0, flags, 3, NULL, NULL) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 1, i32s, 3, NULL, NULL) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 2, i64s, 3, NULL, NULL) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 3, f32s, 3, NULL, NULL) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 4, f64s, 3, NULL, NULL) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 5, bas, 3, NULL, NULL) != CARQUET_OK) {
        (void)carquet_writer_close(writer);
        return -1;
    }
    return carquet_writer_close(writer) == CARQUET_OK ? 0 : -1;
}

GLACIER_INTERNAL int glacier_carquet_write_row_groups_fixture(const char *path) {
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_schema_t *schema = carquet_schema_create(&err);
    if (!schema) return -1;
    if (carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT64, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK) {
        carquet_schema_free(schema);
        return -1;
    }

    carquet_writer_options_t opts;
    carquet_writer_options_init(&opts);
    opts.compression = CARQUET_COMPRESSION_UNCOMPRESSED;
    opts.write_crc = false;
    opts.write_page_index = false;
    opts.write_bloom_filters = false;

    carquet_writer_t *writer = carquet_writer_create(path, schema, &opts, &err);
    carquet_schema_free(schema);
    if (!writer) return -1;

    const int64_t a[] = {1, 2, 3};
    const int64_t b[] = {4, 5, 6};
    if (carquet_writer_write_batch(writer, 0, a, 3, NULL, NULL) != CARQUET_OK ||
        carquet_writer_new_row_group(writer) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 0, b, 3, NULL, NULL) != CARQUET_OK) {
        (void)carquet_writer_close(writer);
        return -1;
    }
    return carquet_writer_close(writer) == CARQUET_OK ? 0 : -1;
}

GLACIER_INTERNAL int glacier_carquet_write_nested_fixture(const char *path) {
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_schema_t *schema = carquet_schema_create(&err);
    if (!schema) return -1;
    if (carquet_schema_add_list(schema, "nums", CARQUET_PHYSICAL_INT32, NULL,
                               CARQUET_REPETITION_OPTIONAL, 0, 0) < 0) {
        carquet_schema_free(schema);
        return -1;
    }

    carquet_writer_options_t opts;
    carquet_writer_options_init(&opts);
    opts.compression = CARQUET_COMPRESSION_UNCOMPRESSED;
    opts.write_crc = false;
    opts.write_page_index = false;
    opts.write_bloom_filters = false;

    carquet_writer_t *writer = carquet_writer_create(path, schema, &opts, &err);
    carquet_schema_free(schema);
    if (!writer) return -1;

    const int32_t vals[] = {1, 2, 3};
    const int16_t def[] = {3, 3, 3};
    const int16_t rep[] = {0, 1, 1};
    if (carquet_writer_write_batch(writer, 0, vals, 3, def, rep) != CARQUET_OK) {
        (void)carquet_writer_close(writer);
        return -1;
    }
    return carquet_writer_close(writer) == CARQUET_OK ? 0 : -1;
}

GLACIER_INTERNAL int glacier_carquet_write_logical_fixture(const char *path) {
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_schema_t *schema = carquet_schema_create(&err);
    if (!schema) return -1;

    carquet_logical_type_t dec = {0};
    dec.id = CARQUET_LOGICAL_DECIMAL;
    dec.params.decimal.precision = 10;
    dec.params.decimal.scale = 2;

    carquet_logical_type_t uuid_lt = {0};
    uuid_lt.id = CARQUET_LOGICAL_UUID;

    carquet_logical_type_t ts = {0};
    ts.id = CARQUET_LOGICAL_TIMESTAMP;
    ts.params.timestamp.unit = CARQUET_TIME_UNIT_MICROS;
    ts.params.timestamp.is_adjusted_to_utc = false;

    carquet_logical_type_t tstz = {0};
    tstz.id = CARQUET_LOGICAL_TIMESTAMP;
    tstz.params.timestamp.unit = CARQUET_TIME_UNIT_MICROS;
    tstz.params.timestamp.is_adjusted_to_utc = true;

    if (carquet_schema_add_column(schema, "amount", CARQUET_PHYSICAL_INT64, &dec,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_FIXED_LEN_BYTE_ARRAY, &uuid_lt,
                                 CARQUET_REPETITION_REQUIRED, 16, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "ts", CARQUET_PHYSICAL_INT64, &ts,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "tstz", CARQUET_PHYSICAL_INT64, &tstz,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK) {
        carquet_schema_free(schema);
        return -1;
    }

    carquet_writer_options_t opts;
    carquet_writer_options_init(&opts);
    opts.compression = CARQUET_COMPRESSION_UNCOMPRESSED;
    opts.write_crc = false;
    opts.write_page_index = false;
    opts.write_bloom_filters = false;

    carquet_writer_t *writer = carquet_writer_create(path, schema, &opts, &err);
    carquet_schema_free(schema);
    if (!writer) return -1;

    const int64_t amounts[] = {1050, 2000, 50};
    static const uint8_t uuids[3][16] = {
        {0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00},
        {0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8},
        {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
    };
    const int64_t ts_us[] = {1700000000000000LL, 1700000001000000LL, 1700000002000000LL};
    const int64_t tstz_us[] = {1700000000000000LL, 1700000060000000LL, 1700000120000000LL};

    if (carquet_writer_write_batch(writer, 0, amounts, 3, NULL, NULL) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 1, uuids, 3, NULL, NULL) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 2, ts_us, 3, NULL, NULL) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 3, tstz_us, 3, NULL, NULL) != CARQUET_OK) {
        (void)carquet_writer_close(writer);
        return -1;
    }
    return carquet_writer_close(writer) == CARQUET_OK ? 0 : -1;
}

GLACIER_INTERNAL int glacier_carquet_write_pos_deletes(
    const char *path,
    const char *const *file_paths,
    const int64_t *positions,
    int32_t n
) {
    if (n <= 0) return -1;
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_schema_t *schema = carquet_schema_create(&err);
    if (!schema) return -1;

    carquet_logical_type_t str_lt = {0};
    str_lt.id = CARQUET_LOGICAL_STRING;

    if (carquet_schema_add_column(schema, "file_path", CARQUET_PHYSICAL_BYTE_ARRAY, &str_lt,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "pos", CARQUET_PHYSICAL_INT64, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK) {
        carquet_schema_free(schema);
        return -1;
    }

    carquet_writer_options_t opts;
    carquet_writer_options_init(&opts);
    opts.compression = CARQUET_COMPRESSION_UNCOMPRESSED;
    opts.write_crc = false;
    opts.write_page_index = false;
    opts.write_bloom_filters = false;

    carquet_writer_t *writer = carquet_writer_create(path, schema, &opts, &err);
    carquet_schema_free(schema);
    if (!writer) return -1;

    carquet_byte_array_t *paths = malloc((size_t)n * sizeof(*paths));
    if (!paths) {
        (void)carquet_writer_close(writer);
        return -1;
    }
    for (int32_t i = 0; i < n; i++) {
        paths[i].data = (uint8_t *)file_paths[i];
        paths[i].length = (int32_t)strlen(file_paths[i]);
    }
    const int ok =
        carquet_writer_write_batch(writer, 0, paths, n, NULL, NULL) == CARQUET_OK &&
        carquet_writer_write_batch(writer, 1, positions, n, NULL, NULL) == CARQUET_OK;
    free(paths);
    if (!ok) {
        (void)carquet_writer_close(writer);
        return -1;
    }
    return carquet_writer_close(writer) == CARQUET_OK ? 0 : -1;
}

GLACIER_INTERNAL int glacier_carquet_write_eq_i64_deletes(
    const char *path,
    const char *col_name,
    const int64_t *vals,
    int32_t n
) {
    if (n <= 0 || !col_name) return -1;
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_schema_t *schema = carquet_schema_create(&err);
    if (!schema) return -1;

    if (carquet_schema_add_column(schema, col_name, CARQUET_PHYSICAL_INT64, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK) {
        carquet_schema_free(schema);
        return -1;
    }

    carquet_writer_options_t opts;
    carquet_writer_options_init(&opts);
    opts.compression = CARQUET_COMPRESSION_UNCOMPRESSED;
    opts.write_crc = false;
    opts.write_page_index = false;
    opts.write_bloom_filters = false;

    carquet_writer_t *writer = carquet_writer_create(path, schema, &opts, &err);
    carquet_schema_free(schema);
    if (!writer) return -1;

    if (carquet_writer_write_batch(writer, 0, vals, n, NULL, NULL) != CARQUET_OK) {
        (void)carquet_writer_close(writer);
        return -1;
    }
    return carquet_writer_close(writer) == CARQUET_OK ? 0 : -1;
}

GLACIER_INTERNAL int glacier_carquet_write_struct_fixture(const char *path) {
    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_schema_t *schema = carquet_schema_create(&err);
    if (!schema) return -1;

    carquet_logical_type_t str_lt = {0};
    str_lt.id = CARQUET_LOGICAL_STRING;

    const int32_t addr = carquet_schema_add_group(schema, "addr", CARQUET_REPETITION_REQUIRED, 0);
    if (addr < 0 ||
        carquet_schema_add_column(schema, "id", CARQUET_PHYSICAL_INT64, NULL,
                                 CARQUET_REPETITION_REQUIRED, 0, 0) != CARQUET_OK ||
        carquet_schema_add_column(schema, "city", CARQUET_PHYSICAL_BYTE_ARRAY, &str_lt,
                                 CARQUET_REPETITION_REQUIRED, 0, addr) != CARQUET_OK) {
        carquet_schema_free(schema);
        return -1;
    }

    carquet_writer_options_t opts;
    carquet_writer_options_init(&opts);
    opts.compression = CARQUET_COMPRESSION_UNCOMPRESSED;
    opts.write_crc = false;
    opts.write_page_index = false;
    opts.write_bloom_filters = false;

    carquet_writer_t *writer = carquet_writer_create(path, schema, &opts, &err);
    carquet_schema_free(schema);
    if (!writer) return -1;

    const int64_t ids[] = {1, 2};
    static const char *cities[] = {"oslo", "bergen"};
    carquet_byte_array_t city_ba[2];
    city_ba[0].data = (uint8_t *)cities[0];
    city_ba[0].length = (int32_t)strlen(cities[0]);
    city_ba[1].data = (uint8_t *)cities[1];
    city_ba[1].length = (int32_t)strlen(cities[1]);

    if (carquet_writer_write_batch(writer, 0, ids, 2, NULL, NULL) != CARQUET_OK ||
        carquet_writer_write_batch(writer, 1, city_ba, 2, NULL, NULL) != CARQUET_OK) {
        (void)carquet_writer_close(writer);
        return -1;
    }
    return carquet_writer_close(writer) == CARQUET_OK ? 0 : -1;
}

typedef struct glacier_write_col {
    const char *name;
    int32_t physical;
    int32_t logical;
    const void *values;
    const uint32_t *utf8_offsets;
    const uint8_t *utf8_bytes;
    int64_t n_rows;
} glacier_write_col_t;

GLACIER_INTERNAL int glacier_carquet_write_columns(
    const char *path,
    const glacier_write_col_t *cols,
    int32_t n_cols
) {
    if (!path || !cols || n_cols <= 0) return -1;
    const int64_t n = cols[0].n_rows;
    if (n <= 0) return -1;

    carquet_error_t err = CARQUET_ERROR_INIT;
    carquet_schema_t *schema = carquet_schema_create(&err);
    if (!schema) return -1;

    for (int32_t i = 0; i < n_cols; i++) {
        if (!cols[i].name || cols[i].n_rows != n) {
            carquet_schema_free(schema);
            return -1;
        }
        carquet_logical_type_t lt = {0};
        const carquet_logical_type_t *ltp = NULL;
        if (cols[i].logical == CARQUET_LOGICAL_STRING) {
            lt.id = CARQUET_LOGICAL_STRING;
            ltp = &lt;
        }
        if (carquet_schema_add_column(
                schema,
                cols[i].name,
                (carquet_physical_type_t)cols[i].physical,
                ltp,
                CARQUET_REPETITION_REQUIRED,
                0,
                0) != CARQUET_OK) {
            carquet_schema_free(schema);
            return -1;
        }
    }

    carquet_writer_options_t opts;
    carquet_writer_options_init(&opts);
    opts.compression = CARQUET_COMPRESSION_SNAPPY;
    opts.write_crc = false;
    opts.write_page_index = false;
    opts.write_bloom_filters = false;
    opts.write_statistics = true;

    carquet_writer_t *writer = carquet_writer_create(path, schema, &opts, &err);
    carquet_schema_free(schema);
    if (!writer) return -1;

    for (int32_t i = 0; i < n_cols; i++) {
        const glacier_write_col_t *col = &cols[i];
        carquet_byte_array_t *bas = NULL;
        const void *values = col->values;
        if (col->physical == CARQUET_PHYSICAL_BYTE_ARRAY) {
            if (!col->utf8_offsets) {
                (void)carquet_writer_close(writer);
                return -1;
            }
            bas = malloc((size_t)n * sizeof(*bas));
            if (!bas) {
                (void)carquet_writer_close(writer);
                return -1;
            }
            for (int64_t r = 0; r < n; r++) {
                const uint32_t start = col->utf8_offsets[r];
                const uint32_t end = col->utf8_offsets[r + 1];
                bas[r].length = (int32_t)(end - start);
                if (bas[r].length > 0 && col->utf8_bytes) {
                    bas[r].data = (uint8_t *)(col->utf8_bytes + start);
                } else {
                    bas[r].data = (uint8_t *)"";
                    bas[r].length = 0;
                }
            }
            values = bas;
        } else if (!values) {
            (void)carquet_writer_close(writer);
            return -1;
        }
        const carquet_status_t st = carquet_writer_write_batch(writer, i, values, n, NULL, NULL);
        free(bas);
        if (st != CARQUET_OK) {
            (void)carquet_writer_close(writer);
            return -1;
        }
    }
    return carquet_writer_close(writer) == CARQUET_OK ? 0 : -1;
}
