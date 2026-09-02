/* Minimal libc for wasm32-wasi without linking wasi-libc (Zig 0.16
 * wasi libc.a fails to compile on this toolchain). */

#include <stddef.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/time.h>

static unsigned char heap[8 * 1024 * 1024];
static size_t heap_off;

void *malloc(size_t n) {
    if (n == 0) n = 1;
    n = (n + 15) & ~(size_t)15;
    if (heap_off + n > sizeof(heap)) return 0;
    void *p = heap + heap_off;
    heap_off += n;
    return p;
}

void free(void *p) {
    (void)p;
}

void *calloc(size_t a, size_t b) {
    size_t n = a * b;
    unsigned char *p = malloc(n);
    if (!p) return 0;
    for (size_t i = 0; i < n; i++) p[i] = 0;
    return p;
}

void *realloc(void *p, size_t n) {
    if (!p) return malloc(n);
    void *q = malloc(n);
    if (!q) return 0;
    unsigned char *d = q;
    unsigned char *s = p;
    for (size_t i = 0; i < n; i++) d[i] = s[i];
    return q;
}

void abort(void) {
    __builtin_trap();
}

void exit(int code) {
    (void)code;
    __builtin_trap();
}

int atexit(void (*f)(void)) {
    (void)f;
    return 0;
}

void *memcpy(void *dst, const void *src, size_t n) {
    unsigned char *d = dst;
    const unsigned char *s = src;
    for (size_t i = 0; i < n; i++) d[i] = s[i];
    return dst;
}

void *memmove(void *dst, const void *src, size_t n) {
    unsigned char *d = dst;
    const unsigned char *s = src;
    if (d < s) {
        for (size_t i = 0; i < n; i++) d[i] = s[i];
    } else {
        for (size_t i = n; i > 0; i--) d[i - 1] = s[i - 1];
    }
    return dst;
}

void *memset(void *dst, int c, size_t n) {
    unsigned char *d = dst;
    for (size_t i = 0; i < n; i++) d[i] = (unsigned char)c;
    return dst;
}

int memcmp(const void *a, const void *b, size_t n) {
    const unsigned char *x = a, *y = b;
    for (size_t i = 0; i < n; i++) {
        if (x[i] != y[i]) return (int)x[i] - (int)y[i];
    }
    return 0;
}

size_t strlen(const char *s) {
    size_t n = 0;
    while (s[n]) n++;
    return n;
}

int strcmp(const char *a, const char *b) {
    while (*a && *a == *b) {
        a++;
        b++;
    }
    return (unsigned char)*a - (unsigned char)*b;
}

int strncmp(const char *a, const char *b, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (a[i] != b[i] || a[i] == 0) return (unsigned char)a[i] - (unsigned char)b[i];
    }
    return 0;
}

char *strcpy(char *d, const char *s) {
    char *o = d;
    while ((*d++ = *s++)) {}
    return o;
}

char *strncpy(char *d, const char *s, size_t n) {
    size_t i = 0;
    for (; i < n && s[i]; i++) d[i] = s[i];
    for (; i < n; i++) d[i] = 0;
    return d;
}

char *strcat(char *d, const char *s) {
    strcpy(d + strlen(d), s);
    return d;
}

char *strchr(const char *s, int c) {
    char ch = (char)c;
    for (; *s; s++) if (*s == ch) return (char *)s;
    return ch == 0 ? (char *)s : 0;
}

char *strrchr(const char *s, int c) {
    char ch = (char)c;
    const char *last = 0;
    for (; *s; s++) if (*s == ch) last = s;
    return ch == 0 ? (char *)s : (char *)last;
}

char *strstr(const char *h, const char *n) {
    if (!*n) return (char *)h;
    for (; *h; h++) {
        const char *a = h, *b = n;
        while (*a && *b && *a == *b) {
            a++;
            b++;
        }
        if (!*b) return (char *)h;
    }
    return 0;
}

char *strdup(const char *s) {
    size_t n = strlen(s) + 1;
    char *p = malloc(n);
    if (!p) return 0;
    memcpy(p, s, n);
    return p;
}

long strtol(const char *s, char **end, int base) {
    long v = 0;
    int sign = 1;
    if (*s == '-') {
        sign = -1;
        s++;
    }
    if (base == 0) base = 10;
    while (*s) {
        int d;
        if (*s >= '0' && *s <= '9') d = *s - '0';
        else break;
        if (d >= base) break;
        v = v * base + d;
        s++;
    }
    if (end) *end = (char *)s;
    return v * sign;
}

int atoi(const char *s) {
    return (int)strtol(s, 0, 10);
}

int abs(int x) {
    return x < 0 ? -x : x;
}

int snprintf(char *buf, size_t n, const char *fmt, ...) {
    if (!buf || n == 0) return 0;
    size_t i = 0;
    if (fmt) {
        while (fmt[i] && i + 1 < n) {
            buf[i] = fmt[i];
            i++;
        }
    }
    buf[i] = 0;
    return (int)i;
}

int vsnprintf(char *buf, size_t n, const char *fmt, va_list ap) {
    (void)fmt;
    (void)ap;
    if (n) buf[0] = 0;
    return 0;
}

int sprintf(char *buf, const char *fmt, ...) {
    (void)fmt;
    buf[0] = 0;
    return 0;
}

int printf(const char *fmt, ...) {
    (void)fmt;
    return 0;
}

int fprintf(FILE *f, const char *fmt, ...) {
    (void)f;
    (void)fmt;
    return 0;
}

int vfprintf(FILE *f, const char *fmt, va_list ap) {
    (void)f;
    (void)fmt;
    (void)ap;
    return 0;
}

int fflush(FILE *f) {
    (void)f;
    return 0;
}

int fputc(int c, FILE *f) {
    (void)f;
    return c;
}

int fputs(const char *s, FILE *f) {
    (void)f;
    (void)s;
    return 0;
}

size_t fwrite(const void *p, size_t sz, size_t n, FILE *f) {
    (void)p;
    (void)f;
    return sz * n;
}

int isspace(int c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f';
}

int isdigit(int c) {
    return c >= '0' && c <= '9';
}

int isalpha(int c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

int isalnum(int c) {
    return isalpha(c) || isdigit(c);
}

int toupper(int c) {
    return (c >= 'a' && c <= 'z') ? c - 'a' + 'A' : c;
}

int tolower(int c) {
    return (c >= 'A' && c <= 'Z') ? c - 'A' + 'a' : c;
}

double strtod(const char *s, char **end) {
    if (end) *end = (char *)s;
    return 0;
}

float strtof(const char *s, char **end) {
    if (end) *end = (char *)s;
    return 0;
}

void qsort(void *base, size_t n, size_t w, int (*cmp)(const void *, const void *)) {
    unsigned char *b = base;
    unsigned char tmp[256];
    for (size_t i = 0; i < n; i++) {
        for (size_t j = i + 1; j < n; j++) {
            if (cmp(b + i * w, b + j * w) > 0) {
                if (w > sizeof(tmp)) continue;
                memcpy(tmp, b + i * w, w);
                memcpy(b + i * w, b + j * w, w);
                memcpy(b + j * w, tmp, w);
            }
        }
    }
}

int posix_memalign(void **p, size_t align, size_t n) {
    (void)align;
    *p = malloc(n);
    return *p ? 0 : 1;
}

char *getenv(const char *n) {
    (void)n;
    return 0;
}

int isatty(int fd) {
    (void)fd;
    return 0;
}

long sysconf(int name) {
    (void)name;
    return -1;
}

void *dlopen(const char *a, int b) {
    (void)a;
    (void)b;
    return 0;
}
void *dlsym(void *a, const char *b) {
    (void)a;
    (void)b;
    return 0;
}
int dlclose(void *a) {
    (void)a;
    return 0;
}
int errno;

FILE *fmemopen(void *buf, size_t size, const char *mode) {
    (void)mode;
    struct MemFile {
        unsigned char *data;
        size_t size;
        size_t pos;
    };
    struct MemFile *f = malloc(sizeof(*f));
    if (!f) return 0;
    f->data = buf;
    f->size = size;
    f->pos = 0;
    return (FILE *)f;
}

FILE *fopen(const char *path, const char *mode) {
    (void)path;
    (void)mode;
    return 0;
}

int fclose(FILE *fp) {
    free(fp);
    return 0;
}

size_t fread(void *ptr, size_t sz, size_t n, FILE *fp) {
    struct MemFile {
        unsigned char *data;
        size_t size;
        size_t pos;
    };
    struct MemFile *f = (void *)fp;
    if (!f || !sz) return 0;
    size_t want = sz * n;
    size_t left = f->size > f->pos ? f->size - f->pos : 0;
    if (want > left) want = left - (left % sz);
    memcpy(ptr, f->data + f->pos, want);
    f->pos += want;
    return want / sz;
}

int fseek(FILE *fp, long off, int whence) {
    struct MemFile {
        unsigned char *data;
        size_t size;
        size_t pos;
    };
    struct MemFile *f = (void *)fp;
    if (!f) return -1;
    size_t np = f->pos;
    if (whence == 0) np = (size_t)off;
    else if (whence == 1) np = f->pos + (size_t)off;
    else if (whence == 2) np = f->size + (size_t)off;
    if (np > f->size) return -1;
    f->pos = np;
    return 0;
}

int feof(FILE *fp) {
    struct MemFile {
        unsigned char *data;
        size_t size;
        size_t pos;
    };
    struct MemFile *f = (void *)fp;
    return f && f->pos >= f->size;
}

long ftell(FILE *fp) {
    struct MemFile {
        unsigned char *data;
        size_t size;
        size_t pos;
    };
    struct MemFile *f = (void *)fp;
    return f ? (long)f->pos : -1;
}

int open(const char *p, int flags, ...) {
    (void)p;
    (void)flags;
    return -1;
}
int close(int fd) {
    (void)fd;
    return -1;
}
ssize_t read(int fd, void *b, size_t n) {
    (void)fd;
    (void)b;
    (void)n;
    return -1;
}

int sched_yield(void) {
    return 0;
}
int getpid(void) {
    return 1;
}

#include <sys/time.h>

int gettimeofday(struct timeval *tv, void *tz) {
    (void)tz;
    if (tv) {
        tv->tv_sec = 0;
        tv->tv_usec = 0;
    }
    return 0;
}

void *memchr(const void *s, int c, size_t n) {
    const unsigned char *p = s;
    unsigned char ch = (unsigned char)c;
    for (size_t i = 0; i < n; i++) if (p[i] == ch) return (void *)(p + i);
    return 0;
}

char *strerror(int e) {
    (void)e;
    return "error";
}

long long strtoll(const char *s, char **end, int base) {
    return (long long)strtol(s, end, base);
}

struct lconv {
    char *decimal_point;
};
static char glacier_dot[] = ".";
static struct lconv glacier_lconv = { .decimal_point = glacier_dot };
struct lconv *localeconv(void) {
    return &glacier_lconv;
}
