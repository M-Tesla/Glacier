#include <openssl/bn.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>

int glacier_jwt_rs256(
    const unsigned char *n, int nlen,
    const unsigned char *e, int elen,
    const unsigned char *msg, int mlen,
    const unsigned char *sig, int slen) {
    BIGNUM *bn = BN_bin2bn(n, nlen, NULL);
    BIGNUM *be = BN_bin2bn(e, elen, NULL);
    RSA *rsa;
    EVP_PKEY *pkey;
    EVP_MD_CTX *ctx;
    int ok;
    if (!bn || !be) {
        BN_free(bn);
        BN_free(be);
        return 0;
    }
    rsa = RSA_new();
    if (!rsa) {
        BN_free(bn);
        BN_free(be);
        return 0;
    }
    if (RSA_set0_key(rsa, bn, be, NULL) != 1) {
        BN_free(bn);
        BN_free(be);
        RSA_free(rsa);
        return 0;
    }
    pkey = EVP_PKEY_new();
    if (!pkey) {
        RSA_free(rsa);
        return 0;
    }
    if (EVP_PKEY_assign_RSA(pkey, rsa) != 1) {
        RSA_free(rsa);
        EVP_PKEY_free(pkey);
        return 0;
    }
    ctx = EVP_MD_CTX_new();
    if (!ctx) {
        EVP_PKEY_free(pkey);
        return 0;
    }
    ok = EVP_DigestVerifyInit(ctx, NULL, EVP_sha256(), NULL, pkey) == 1 &&
        EVP_DigestVerifyUpdate(ctx, msg, (size_t)mlen) == 1 &&
        EVP_DigestVerifyFinal(ctx, sig, (size_t)slen) == 1;
    EVP_MD_CTX_free(ctx);
    EVP_PKEY_free(pkey);
    return ok ? 1 : 0;
}
