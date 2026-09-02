#include <carquet/error.h>
#include <stdint.h>
#include <stddef.h>

int carquet_zstd_decompress(
    const uint8_t *src,
    size_t src_size,
    uint8_t *dst,
    size_t dst_capacity,
    size_t *dst_size) {
    (void)src;
    (void)src_size;
    (void)dst;
    (void)dst_capacity;
    (void)dst_size;
    return CARQUET_ERROR_UNSUPPORTED_CODEC;
}

int carquet_zstd_compress(
    const uint8_t *src,
    size_t src_size,
    uint8_t *dst,
    size_t dst_capacity,
    size_t *dst_size,
    int level) {
    (void)src;
    (void)src_size;
    (void)dst;
    (void)dst_capacity;
    (void)dst_size;
    (void)level;
    return CARQUET_ERROR_UNSUPPORTED_CODEC;
}

size_t carquet_zstd_compress_bound(size_t src_size) {
    return src_size;
}

void carquet_zstd_init_tables(void) {}

void carquet_zstd_cleanup(void) {}
