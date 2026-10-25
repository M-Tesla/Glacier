#include <openssl/ssl.h>
#include <stdlib.h>

typedef struct GlacierTls {
    SSL_CTX *ctx;
} GlacierTls;

GlacierTls *glacier_tls_server(const char *cert, const char *key) {
    OPENSSL_init_ssl(0, NULL);
    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    if (!ctx) return NULL;
    if (SSL_CTX_use_certificate_file(ctx, cert, SSL_FILETYPE_PEM) != 1 ||
        SSL_CTX_use_PrivateKey_file(ctx, key, SSL_FILETYPE_PEM) != 1) {
        SSL_CTX_free(ctx);
        return NULL;
    }
    GlacierTls *box = calloc(1, sizeof(*box));
    if (!box) {
        SSL_CTX_free(ctx);
        return NULL;
    }
    box->ctx = ctx;
    return box;
}

void glacier_tls_free(GlacierTls *box) {
    if (!box) return;
    SSL_CTX_free(box->ctx);
    free(box);
}

SSL *glacier_tls_accept(GlacierTls *box, int fd) {
    SSL *ssl = SSL_new(box->ctx);
    if (!ssl) return NULL;
    SSL_set_fd(ssl, fd);
    if (SSL_accept(ssl) != 1) {
        SSL_free(ssl);
        return NULL;
    }
    return ssl;
}

int glacier_tls_read(SSL *ssl, void *buf, int n) {
    return SSL_read(ssl, buf, n);
}

int glacier_tls_write(SSL *ssl, const void *buf, int n) {
    return SSL_write(ssl, buf, n);
}

void glacier_tls_close(SSL *ssl) {
    if (!ssl) return;
    SSL_shutdown(ssl);
    SSL_free(ssl);
}
