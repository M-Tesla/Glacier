#include "visibility_hidden.h"
#include "nanoarrow.h"

#include <errno.h>
#include <stdint.h>
#include <string.h>

enum {
    GLACIER_ARROW_BOOL = 0,
    GLACIER_ARROW_I32 = 1,
    GLACIER_ARROW_I64 = 2,
    GLACIER_ARROW_F64 = 3,
    GLACIER_ARROW_UTF8 = 4,
    GLACIER_ARROW_F32 = 5,
    GLACIER_ARROW_TIMESTAMP = 6,
    GLACIER_ARROW_TIMESTAMPTZ = 7,
    GLACIER_ARROW_UUID = 8,
    GLACIER_ARROW_DECIMAL128 = 9,
};

typedef struct GlacierArrowCol {
    const char *name;
    int32_t type;
    const uint8_t *bools;
    const int32_t *i32s;
    const int64_t *i64s;
    const float *f32s;
    const double *f64s;
    const uint32_t *utf8_offsets;
    const uint8_t *utf8_bytes;
    const uint8_t *uuids;
    const uint8_t *i128s_le;
    int32_t decimal_precision;
    int32_t decimal_scale;
    const uint8_t *valid;
} GlacierArrowCol;

static int glacier_nanoarrow_type(int32_t t, enum ArrowType *out) {
    switch (t) {
        case GLACIER_ARROW_BOOL:
            *out = NANOARROW_TYPE_BOOL;
            return 0;
        case GLACIER_ARROW_I32:
            *out = NANOARROW_TYPE_INT32;
            return 0;
        case GLACIER_ARROW_I64:
            *out = NANOARROW_TYPE_INT64;
            return 0;
        case GLACIER_ARROW_F64:
            *out = NANOARROW_TYPE_DOUBLE;
            return 0;
        case GLACIER_ARROW_F32:
            *out = NANOARROW_TYPE_FLOAT;
            return 0;
        case GLACIER_ARROW_UTF8:
            *out = NANOARROW_TYPE_STRING;
            return 0;
        default:
            return EINVAL;
    }
}

static int glacier_init_child(
    struct ArrowSchema *schema,
    struct ArrowArray *array,
    const GlacierArrowCol *col
) {
    struct ArrowError err;
    memset(&err, 0, sizeof(err));
    int rc;
    switch (col->type) {
        case GLACIER_ARROW_TIMESTAMP:
            ArrowSchemaInit(schema);
            rc = ArrowSchemaSetTypeDateTime(
                schema, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MICRO, NULL
            );
            if (rc != NANOARROW_OK) return rc;
            return ArrowArrayInitFromSchema(array, schema, &err);
        case GLACIER_ARROW_TIMESTAMPTZ:
            ArrowSchemaInit(schema);
            rc = ArrowSchemaSetTypeDateTime(
                schema, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MICRO, "UTC"
            );
            if (rc != NANOARROW_OK) return rc;
            return ArrowArrayInitFromSchema(array, schema, &err);
        case GLACIER_ARROW_UUID:
            ArrowSchemaInit(schema);
            rc = ArrowSchemaSetTypeFixedSize(schema, NANOARROW_TYPE_FIXED_SIZE_BINARY, 16);
            if (rc != NANOARROW_OK) return rc;
            return ArrowArrayInitFromSchema(array, schema, &err);
        case GLACIER_ARROW_DECIMAL128:
            ArrowSchemaInit(schema);
            rc = ArrowSchemaSetTypeDecimal(
                schema,
                NANOARROW_TYPE_DECIMAL128,
                col->decimal_precision,
                col->decimal_scale
            );
            if (rc != NANOARROW_OK) return rc;
            return ArrowArrayInitFromSchema(array, schema, &err);
        default: {
            enum ArrowType t;
            rc = glacier_nanoarrow_type(col->type, &t);
            if (rc != NANOARROW_OK) return rc;
            rc = ArrowSchemaInitFromType(schema, t);
            if (rc != NANOARROW_OK) return rc;
            return ArrowArrayInitFromType(array, t);
        }
    }
}

static int glacier_append_cell(struct ArrowArray *child, const GlacierArrowCol *col, int64_t row) {
    if (col->valid && col->valid[row] == 0) return ArrowArrayAppendNull(child, 1);
    switch (col->type) {
        case GLACIER_ARROW_BOOL:
            return ArrowArrayAppendInt(child, col->bools[row] != 0);
        case GLACIER_ARROW_I32:
            return ArrowArrayAppendInt(child, col->i32s[row]);
        case GLACIER_ARROW_I64:
        case GLACIER_ARROW_TIMESTAMP:
        case GLACIER_ARROW_TIMESTAMPTZ:
            return ArrowArrayAppendInt(child, col->i64s[row]);
        case GLACIER_ARROW_F64:
            return ArrowArrayAppendDouble(child, col->f64s[row]);
        case GLACIER_ARROW_F32:
            return ArrowArrayAppendDouble(child, (double)col->f32s[row]);
        case GLACIER_ARROW_UTF8: {
            const uint32_t start = col->utf8_offsets[row];
            const uint32_t end = col->utf8_offsets[row + 1];
            struct ArrowStringView sv = {
                .data = (const char *)(col->utf8_bytes + start),
                .size_bytes = (int64_t)(end - start),
            };
            return ArrowArrayAppendString(child, sv);
        }
        case GLACIER_ARROW_UUID: {
            struct ArrowBufferView bv;
            memset(&bv, 0, sizeof(bv));
            bv.data.data = col->uuids + ((size_t)row * 16);
            bv.size_bytes = 16;
            return ArrowArrayAppendBytes(child, bv);
        }
        case GLACIER_ARROW_DECIMAL128: {
            struct ArrowDecimal dec;
            ArrowDecimalInit(&dec, 128, col->decimal_precision, col->decimal_scale);
            ArrowDecimalSetBytes(&dec, col->i128s_le + ((size_t)row * 16));
            return ArrowArrayAppendDecimal(child, &dec);
        }
        default:
            return EINVAL;
    }
}

GLACIER_INTERNAL int glacier_arrow_export(
    const GlacierArrowCol *cols,
    int32_t n_cols,
    int64_t n_rows,
    struct ArrowArray *out_array,
    struct ArrowSchema *out_schema
) {
    memset(out_array, 0, sizeof(*out_array));
    memset(out_schema, 0, sizeof(*out_schema));

    if (n_cols < 0 || n_rows < 0) return EINVAL;

    int rc = ArrowSchemaInitFromType(out_schema, NANOARROW_TYPE_STRUCT);
    if (rc != NANOARROW_OK) return rc;
    rc = ArrowSchemaAllocateChildren(out_schema, n_cols);
    if (rc != NANOARROW_OK) goto fail;

    rc = ArrowArrayInitFromType(out_array, NANOARROW_TYPE_STRUCT);
    if (rc != NANOARROW_OK) goto fail;
    rc = ArrowArrayAllocateChildren(out_array, n_cols);
    if (rc != NANOARROW_OK) goto fail;

    int32_t i;
    for (i = 0; i < n_cols; i++) {
        rc = glacier_init_child(out_schema->children[i], out_array->children[i], &cols[i]);
        if (rc != NANOARROW_OK) goto fail;
        rc = ArrowSchemaSetName(out_schema->children[i], cols[i].name ? cols[i].name : "");
        if (rc != NANOARROW_OK) goto fail;
    }

    rc = ArrowArrayStartAppending(out_array);
    if (rc != NANOARROW_OK) goto fail;

    int64_t row;
    for (row = 0; row < n_rows; row++) {
        for (i = 0; i < n_cols; i++) {
            rc = glacier_append_cell(out_array->children[i], &cols[i], row);
            if (rc != NANOARROW_OK) goto fail;
        }
        rc = ArrowArrayFinishElement(out_array);
        if (rc != NANOARROW_OK) goto fail;
    }

    struct ArrowError err;
    memset(&err, 0, sizeof(err));
    rc = ArrowArrayFinishBuildingDefault(out_array, &err);
    if (rc != NANOARROW_OK) goto fail;
    return NANOARROW_OK;

fail:
    if (out_schema->release) out_schema->release(out_schema);
    if (out_array->release) out_array->release(out_array);
    memset(out_array, 0, sizeof(*out_array));
    memset(out_schema, 0, sizeof(*out_schema));
    return rc == 0 ? EINVAL : rc;
}
