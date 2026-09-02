#include "visibility_hidden.h"
#include "snappy-c.h"
#include <carquet/error.h>
#include <stdint.h>

carquet_status_t carquet_snappy_compress(
    const uint8_t *src,
    size_t src_size,
    uint8_t *dst,
    size_t dst_capacity,
    size_t *dst_size);
carquet_status_t carquet_snappy_decompress(
    const uint8_t *src,
    size_t src_size,
    uint8_t *dst,
    size_t dst_capacity,
    size_t *dst_size);
size_t carquet_snappy_compress_bound(size_t src_size);
carquet_status_t carquet_snappy_get_uncompressed_length(const uint8_t *src, size_t src_size, size_t *length);

static snappy_status map_status(carquet_status_t st) {
    if (st == CARQUET_OK) return SNAPPY_OK;
    if (st == CARQUET_ERROR_COMPRESSION) return SNAPPY_BUFFER_TOO_SMALL;
    return SNAPPY_INVALID_INPUT;
}

GLACIER_INTERNAL snappy_status snappy_compress(
    const char *input,
    size_t input_length,
    char *compressed,
    size_t *compressed_length
) {
    if (!compressed_length) return SNAPPY_INVALID_INPUT;
    size_t out = 0;
    const carquet_status_t st = carquet_snappy_compress(
        (const uint8_t *)input,
        input_length,
        (uint8_t *)compressed,
        *compressed_length,
        &out);
    if (st == CARQUET_OK) *compressed_length = out;
    return map_status(st);
}

GLACIER_INTERNAL snappy_status snappy_uncompress(
    const char *compressed,
    size_t compressed_length,
    char *uncompressed,
    size_t *uncompressed_length
) {
    if (!uncompressed_length) return SNAPPY_INVALID_INPUT;
    size_t out = 0;
    const carquet_status_t st = carquet_snappy_decompress(
        (const uint8_t *)compressed,
        compressed_length,
        (uint8_t *)uncompressed,
        *uncompressed_length,
        &out);
    if (st == CARQUET_OK) *uncompressed_length = out;
    return map_status(st);
}

GLACIER_INTERNAL size_t snappy_max_compressed_length(size_t source_length) {
    return carquet_snappy_compress_bound(source_length);
}

GLACIER_INTERNAL snappy_status snappy_uncompressed_length(
    const char *compressed,
    size_t compressed_length,
    size_t *result
) {
    if (!result) return SNAPPY_INVALID_INPUT;
    return map_status(carquet_snappy_get_uncompressed_length(
        (const uint8_t *)compressed,
        compressed_length,
        result));
}
