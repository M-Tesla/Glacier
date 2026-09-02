/* Public ABI is Zig `pub export` in glacier.h. C bridges stay inside the .so. */
#if defined(__GNUC__) || defined(__clang__)
#define GLACIER_INTERNAL __attribute__((visibility("hidden")))
#else
#define GLACIER_INTERNAL
#endif
