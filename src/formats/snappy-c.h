/* C API expected by libavro (`#include <snappy-c.h>`). Implemented by
 * snappy_c_shim.c on top of carquet's C Snappy — not a Zig codec. */
#ifndef SNAPPY_C_H
#define SNAPPY_C_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    SNAPPY_OK = 0,
    SNAPPY_INVALID_INPUT = 1,
    SNAPPY_BUFFER_TOO_SMALL = 2
} snappy_status;

snappy_status snappy_compress(const char *input, size_t input_length, char *compressed, size_t *compressed_length);
snappy_status snappy_uncompress(const char *compressed, size_t compressed_length, char *uncompressed, size_t *uncompressed_length);
size_t snappy_max_compressed_length(size_t source_length);
snappy_status snappy_uncompressed_length(const char *compressed, size_t compressed_length, size_t *result);

#ifdef __cplusplus
}
#endif

#endif
