/* JWT verification: token rules, malformed and fuzzed input, and the JWKS
 * cache (maximum age, Cache-Control, fail-closed expiry, unknown-kid limit)
 * against a loopback JWKS server with a fake monotonic clock. */
#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <openssl/bn.h>
#include <openssl/core_names.h>
#include <openssl/ec.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <pthread.h>
#include <sodium.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "jwt_internal.h"

#define CHECK(condition)                                                    \
    do {                                                                    \
        if (!(condition)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition); \
            abort();                                                        \
        }                                                                   \
    } while (0)

static const char *ISSUER = "https://auth.orchard.test";
static const char *AUDIENCE = "https://desk.orchard.test/d/dep_123";

/* ---------------------------------------------------------------------- */
/* Minting                                                                 */
/* ---------------------------------------------------------------------- */

static char *b64(const unsigned char *data, size_t size) {
    size_t n = sodium_base64_ENCODED_LEN(size, sodium_base64_VARIANT_URLSAFE_NO_PADDING);
    char *out = malloc(n);
    CHECK(out);
    sodium_bin2base64(out, n, data, size, sodium_base64_VARIANT_URLSAFE_NO_PADDING);
    return out;
}
static char *b64s(const char *text) { return b64((const unsigned char *)text, strlen(text)); }

struct TestKey {
    EVP_PKEY *pkey;
    const char *kid;
    int ec_bits; /* 0 for RSA */
};
static struct TestKey ec_key(const char *kid, const char *curve, int bits) {
    struct TestKey k = {EVP_EC_gen(curve), kid, bits};
    CHECK(k.pkey);
    return k;
}
static struct TestKey rsa_key(const char *kid) {
    struct TestKey k = {EVP_RSA_gen(2048), kid, 0};
    CHECK(k.pkey);
    return k;
}
static char *bn_b64(EVP_PKEY *pkey, const char *name) {
    BIGNUM *bn = NULL;
    CHECK(EVP_PKEY_get_bn_param(pkey, name, &bn) == 1);
    unsigned char buffer[1024];
    int n = BN_bn2bin(bn, buffer);
    BN_free(bn);
    return b64(buffer, (size_t)n);
}
/* One JWK object (no surrounding braces' trailing comma). */
static void jwk(char *out, size_t size, const struct TestKey *k) {
    if (k->ec_bits) {
        unsigned char point[200];
        size_t length = 0;
        CHECK(EVP_PKEY_get_octet_string_param(k->pkey, OSSL_PKEY_PARAM_PUB_KEY, point,
                                              sizeof(point), &length) == 1);
        size_t c = (length - 1) / 2;
        char *x = b64(point + 1, c), *y = b64(point + 1 + c, c);
        snprintf(out, size,
                 "{\"kty\":\"EC\",\"crv\":\"%s\",\"kid\":\"%s\",\"x\":\"%s\",\"y\":\"%s\"}",
                 k->ec_bits == 256   ? "P-256"
                 : k->ec_bits == 384 ? "P-384"
                                     : "P-521",
                 k->kid, x, y);
        free(x);
        free(y);
    } else {
        char *n = bn_b64(k->pkey, OSSL_PKEY_PARAM_RSA_N),
             *e = bn_b64(k->pkey, OSSL_PKEY_PARAM_RSA_E);
        snprintf(out, size, "{\"kty\":\"RSA\",\"kid\":\"%s\",\"n\":\"%s\",\"e\":\"%s\"}", k->kid, n,
                 e);
        free(n);
        free(e);
    }
}
static char *jwks(const struct TestKey *keys, size_t count) {
    size_t size = 4096 * (count + 1);
    char *out = malloc(size);
    CHECK(out);
    strcpy(out, "{\"keys\":[");
    for (size_t i = 0; i < count; ++i) {
        char one[4096];
        jwk(one, sizeof(one), &keys[i]);
        if (i) strcat(out, ",");
        strcat(out, one);
    }
    strcat(out, "]}");
    return out;
}
static unsigned char *sign(const struct TestKey *k, const char *alg, const char *message,
                           size_t *size) {
    const char *digest = strstr(alg, "384") ? "SHA384" : strstr(alg, "512") ? "SHA512" : "SHA256";
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_PKEY_CTX *pctx = NULL;
    CHECK(EVP_DigestSignInit_ex(ctx, &pctx, digest, NULL, NULL, k->pkey, NULL) == 1);
    if (alg[0] == 'P') {
        CHECK(EVP_PKEY_CTX_set_rsa_padding(pctx, RSA_PKCS1_PSS_PADDING) == 1);
        CHECK(EVP_PKEY_CTX_set_rsa_pss_saltlen(pctx, RSA_PSS_SALTLEN_DIGEST) == 1);
    }
    size_t length = 0;
    CHECK(EVP_DigestSign(ctx, NULL, &length, (const unsigned char *)message, strlen(message)) == 1);
    unsigned char *der = malloc(length);
    CHECK(EVP_DigestSign(ctx, der, &length, (const unsigned char *)message, strlen(message)) == 1);
    EVP_MD_CTX_free(ctx);
    if (!k->ec_bits) {
        *size = length;
        return der;
    }
    const unsigned char *p = der;
    ECDSA_SIG *sig = d2i_ECDSA_SIG(NULL, &p, (long)length);
    CHECK(sig);
    size_t c = ((size_t)k->ec_bits + 7) / 8;
    unsigned char *raw = malloc(2 * c);
    CHECK(BN_bn2binpad(ECDSA_SIG_get0_r(sig), raw, (int)c) == (int)c);
    CHECK(BN_bn2binpad(ECDSA_SIG_get0_s(sig), raw + c, (int)c) == (int)c);
    ECDSA_SIG_free(sig);
    free(der);
    *size = 2 * c;
    return raw;
}
/* header and claims are raw JSON text. */
static char *mint_raw(const struct TestKey *k, const char *alg, const char *header,
                      const char *claims) {
    char *h = b64s(header), *c = b64s(claims);
    size_t n = strlen(h) + strlen(c) + 2;
    char *message = malloc(n);
    snprintf(message, n, "%s.%s", h, c);
    size_t sig_size = 0;
    unsigned char *sig = sign(k, alg, message, &sig_size);
    char *s = b64(sig, sig_size);
    char *token = malloc(n + strlen(s) + 1);
    sprintf(token, "%s.%s", message, s);
    free(h);
    free(c);
    free(message);
    free(sig);
    free(s);
    return token;
}
/* extra is appended inside the claims object, e.g. ",\"x\":1". */
static char *claims_json(long long now, const char *iss, const char *aud, const char *extra,
                         int with_exp) {
    static char buffer[32768];
    char exp[64] = "";
    if (with_exp) snprintf(exp, sizeof(exp), ",\"exp\":%lld", now + 900);
    snprintf(buffer, sizeof(buffer),
             "{\"iss\":\"%s\",\"aud\":%s,\"sub\":\"user_01M7KNJYC18T\",\"kind\":\"user\","
             "\"email\":\"ada@example.com\",\"email_verified\":true,\"name\":\"Ada Lovelace\","
             "\"org_id\":\"org_01M7\",\"org_name\":\"Acme Analytics\",\"client_id\":\"vgi-duckdb\","
             "\"iat\":%lld,\"nbf\":%lld,\"jti\":\"jti-1\"%s%s}",
             iss, aud, now, now, exp, extra ? extra : "");
    return buffer;
}
static char *mint(const struct TestKey *k, const char *alg, const char *typ, const char *claims) {
    char header[512];
    if (typ)
        snprintf(header, sizeof(header), "{\"alg\":\"%s\",\"typ\":\"%s\",\"kid\":\"%s\"}", alg, typ,
                 k->kid);
    else
        snprintf(header, sizeof(header), "{\"alg\":\"%s\",\"kid\":\"%s\"}", alg, k->kid);
    return mint_raw(k, alg, header, claims);
}
static char *pass_for(const struct TestKey *k, long long now) {
    char aud[256];
    snprintf(aud, sizeof(aud), "\"%s\"", AUDIENCE);
    return mint(k, "ES256", "at+jwt", claims_json(now, ISSUER, aud, NULL, 1));
}

/* ---------------------------------------------------------------------- */
/* Offline token rules                                                     */
/* ---------------------------------------------------------------------- */

static struct VgiJwtVerifier *hosted_verifier(const char *jwks_uri, const char *algorithms) {
    struct VgiJwtOptions o;
    VgiJwtOptionsInit(&o);
    o.issuer = ISSUER;
    o.audience = AUDIENCE;
    o.jwks_uri = jwks_uri;
    o.algorithms = algorithms;
    o.required_typ = "at+jwt";
    o.require_exp = 1;
    o.single_audience = 1;
    o.leeway_seconds = 60;
    o.jwks_max_age_seconds = 600;
    o.jwks_refresh_min_interval_seconds = 60;
    o.jwks_fetch_timeout_seconds = 5;
    o.domain = "orchard";
    struct VgiJwtVerifier *v = NULL;
    struct ArrowError error = {{0}};
    CHECK(VgiJwtVerifierNew(&o, &v, &error) == 0);
    return v;
}
static int verify(struct VgiJwtVerifier *v, struct KeySet *keys, const char *token, double now) {
    char *claims = NULL, *principal = NULL;
    const char *reason = NULL;
    int rc = VgiJwtVerifyWithKeys(v, keys, token, now, &claims, &principal, &reason);
    if (rc == 0) {
        CHECK(claims && principal && !strcmp(principal, "user_01M7KNJYC18T"));
        CHECK(reason == NULL);
    } else {
        CHECK(!claims && !principal);
        CHECK(rc == -1 || reason);
    }
    free(claims);
    free(principal);
    return rc;
}

static struct TestKey EC1, EC2, EC384, RSA1;

static void option_tests(void) {
    unsigned bits = 0;
    struct ArrowError error = {{0}};
    CHECK(VgiJwtParseAlgorithms(NULL, &bits, &error) == 0 && bits == 1);
    CHECK(VgiJwtParseAlgorithms("", &bits, &error) == 0 && bits == 1);
    CHECK(VgiJwtParseAlgorithms(" ES256 , RS256 ", &bits, &error) == 0 && bits == (1u | 8u));
    CHECK(VgiJwtParseAlgorithms("ES256,HS256", &bits, &error) == EINVAL);
    CHECK(VgiJwtParseAlgorithms("none", &bits, &error) == EINVAL);
    CHECK(VgiJwtParseAlgorithms("NONE", &bits, &error) == EINVAL);
    CHECK(VgiJwtParseAlgorithms("hs512", &bits, &error) == EINVAL);
    CHECK(VgiJwtParseAlgorithms("es256", &bits, &error) == EINVAL);
    CHECK(VgiJwtParseAlgorithms("EdDSA", &bits, &error) == EINVAL);
    CHECK(VgiJwtParseAlgorithms(" , ", &bits, &error) == EINVAL);

    struct VgiJwtOptions o;
    VgiJwtOptionsInit(&o);
    struct VgiJwtVerifier *v = NULL;
    CHECK(VgiJwtVerifierNew(&o, &v, &error) == EINVAL && !v);
    o.issuer = ISSUER;
    o.audience = AUDIENCE;
    o.jwks_uri = "file:///etc/passwd";
    CHECK(VgiJwtVerifierNew(&o, &v, &error) == EINVAL);
    o.jwks_uri = "https://auth.orchard.test/jwks";
    o.required_typ = "";
    CHECK(VgiJwtVerifierNew(&o, &v, &error) == EINVAL);
    o.required_typ = NULL;
    CHECK(VgiJwtVerifierNew(&o, &v, &error) == 0 && v);
    VgiJwtVerifierFree(v);

    double age = 0;
    CHECK(VgiJwtCacheControlMaxAge("max-age=60", &age) && age == 60);
    CHECK(VgiJwtCacheControlMaxAge("public, max-age=30", &age) && age == 30);
    CHECK(VgiJwtCacheControlMaxAge("MAX-AGE = \"45\" , no-transform", &age) && age == 45);
    CHECK(!VgiJwtCacheControlMaxAge("s-maxage=10", &age));
    CHECK(!VgiJwtCacheControlMaxAge("max-age=abc", &age));
    CHECK(!VgiJwtCacheControlMaxAge("max-age=10x", &age));
    CHECK(!VgiJwtCacheControlMaxAge("max-age=\"10", &age));
    CHECK(!VgiJwtCacheControlMaxAge("no-cache", &age));
    CHECK(!VgiJwtCacheControlMaxAge("", &age));
    CHECK(!VgiJwtCacheControlMaxAge(NULL, &age));
}

static void token_tests(void) {
    struct VgiJwtVerifier *v = hosted_verifier("http://127.0.0.1:9/jwks", NULL);
    struct TestKey published[] = {EC1, EC384, RSA1};
    char *set_json = jwks(published, 3);
    struct KeySet *keys = NULL;
    CHECK(VgiJwtKeySetParse(set_json, strlen(set_json), &keys) == 0);
    CHECK(VgiJwtKeySetCount(keys) == 3);
    double now = (double)time(NULL);
    long long t = (long long)now;
    char aud[256];
    snprintf(aud, sizeof(aud), "\"%s\"", AUDIENCE);
    char *token;

#define EXPECT(expression, expected)                      \
    do {                                                  \
        token = (expression);                             \
        CHECK(verify(v, keys, token, now) == (expected)); \
        free(token);                                      \
    } while (0)

    /* The good Pass, and its claims verbatim. */
    token = pass_for(&EC1, t);
    {
        char *claims = NULL, *principal = NULL;
        const char *reason = NULL;
        CHECK(VgiJwtVerifyWithKeys(v, keys, token, now, &claims, &principal, &reason) == 0);
        CHECK(!strcmp(claims, claims_json(t, ISSUER, aud, NULL, 1)));
        free(claims);
        free(principal);
    }
    free(token);
    /* typ: case-insensitive, application/ optional, required. */
    EXPECT(mint(&EC1, "ES256", "AT+JWT", claims_json(t, ISSUER, aud, NULL, 1)), 0);
    EXPECT(mint(&EC1, "ES256", "application/at+jwt", claims_json(t, ISSUER, aud, NULL, 1)), 0);
    EXPECT(mint(&EC1, "ES256", "JWT", claims_json(t, ISSUER, aud, NULL, 1)), EACCES);
    EXPECT(mint(&EC1, "ES256", NULL, claims_json(t, ISSUER, aud, NULL, 1)), EACCES);
    EXPECT(mint_raw(&EC1, "ES256", "{\"alg\":\"ES256\",\"typ\":7,\"kid\":\"ec1\"}",
                    claims_json(t, ISSUER, aud, NULL, 1)),
           EACCES);
    /* Algorithms: only the allow-list; keys must fit. */
    EXPECT(mint(&EC384, "ES384", "at+jwt", claims_json(t, ISSUER, aud, NULL, 1)), EACCES);
    EXPECT(mint(&RSA1, "RS256", "at+jwt", claims_json(t, ISSUER, aud, NULL, 1)), EACCES);
    EXPECT(mint_raw(&EC1, "ES256",
                    "{\"alg\":\"ES256\",\"typ\":\"at+jwt\",\"kid\":\"ec1\",\"crit\":[\"exp\"]}",
                    claims_json(t, ISSUER, aud, NULL, 1)),
           EACCES);
    {
        /* alg none: an unsigned token, with and without a signature. */
        char *h = b64s("{\"alg\":\"none\",\"typ\":\"at+jwt\",\"kid\":\"ec1\"}");
        char *c = b64s(claims_json(t, ISSUER, aud, NULL, 1));
        char unsigned_token[4096];
        snprintf(unsigned_token, sizeof(unsigned_token), "%s.%s.", h, c);
        CHECK(verify(v, keys, unsigned_token, now) == EACCES);
        snprintf(unsigned_token, sizeof(unsigned_token), "%s.%s.AAAA", h, c);
        CHECK(verify(v, keys, unsigned_token, now) == EACCES);
        free(h);
        free(c);
        /* HS256 keyed with the EC public key's bytes (key confusion). */
        h = b64s("{\"alg\":\"HS256\",\"typ\":\"at+jwt\",\"kid\":\"ec1\"}");
        c = b64s(claims_json(t, ISSUER, aud, NULL, 1));
        snprintf(unsigned_token, sizeof(unsigned_token), "%s.%s.%s", h, c,
                 "c2lnbmF0dXJlLWJ5dGVzLW9mLXRoZS1yaWdodC1sZW5ndGgtMDAwMDA");
        CHECK(verify(v, keys, unsigned_token, now) == EACCES);
        free(h);
        free(c);
    }
    /* A header alg the key does not fit: ES256 header, P-384 key. */
    {
        struct TestKey wrong = EC384;
        char header[256];
        snprintf(header, sizeof(header), "{\"alg\":\"ES256\",\"typ\":\"at+jwt\",\"kid\":\"%s\"}",
                 wrong.kid);
        EXPECT(mint_raw(&EC1, "ES256", header, claims_json(t, ISSUER, aud, NULL, 1)), EACCES);
    }
    /* iss and aud: exact, one audience. */
    EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, "https://evil.test", aud, NULL, 1)),
           EACCES);
    EXPECT(
        mint(&EC1, "ES256", "at+jwt", claims_json(t, "https://auth.orchard.test/", aud, NULL, 1)),
        EACCES);
    EXPECT(mint(&EC1, "ES256", "at+jwt",
                claims_json(t, ISSUER, "\"https://desk.orchard.test/d/dep_other\"", NULL, 1)),
           EACCES);
    {
        char list[512];
        snprintf(list, sizeof(list), "[\"%s\",\"https://desk.orchard.test/d/dep_other\"]",
                 AUDIENCE);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, list, NULL, 1)), EACCES);
        snprintf(list, sizeof(list), "[\"%s\"]", AUDIENCE);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, list, NULL, 1)), 0);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, "[]", NULL, 1)), EACCES);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, "7", NULL, 1)), EACCES);
    }
    /* exp: required, numeric, 60 s leeway; expiry is ESTALE. */
    EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, NULL, 0)), EACCES);
    {
        char extra[128];
        snprintf(extra, sizeof(extra), ",\"exp\":%lld", t - 60);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, extra, 0)), ESTALE);
        snprintf(extra, sizeof(extra), ",\"exp\":%lld", t - 59);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, extra, 0)), 0);
        snprintf(extra, sizeof(extra), ",\"exp\":%lld.5", t + 100);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, extra, 0)), 0);
        snprintf(extra, sizeof(extra), ",\"exp\":\"%lld\"", t + 100);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, extra, 0)), EACCES);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, ",\"exp\":true", 0)),
               EACCES);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, ",\"exp\":null", 0)),
               EACCES);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, ",\"exp\":1e400", 0)),
               EACCES);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, ",\"exp\":NaN", 0)),
               EACCES);
    }
    /* nbf and iat: not in the future beyond the leeway. Later keys win. */
    {
        char extra[128];
        snprintf(extra, sizeof(extra), ",\"nbf\":%lld", t + 61);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, extra, 1)), EACCES);
        snprintf(extra, sizeof(extra), ",\"nbf\":%lld", t + 59);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, extra, 1)), 0);
        snprintf(extra, sizeof(extra), ",\"iat\":%lld", t + 61);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, extra, 1)), EACCES);
        EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, ",\"nbf\":\"x\"", 1)),
               EACCES);
    }
    /* sub: a non-empty string. */
    EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, ",\"sub\":\"\"", 1)), EACCES);
    EXPECT(mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, ",\"sub\":42", 1)), EACCES);
    /* Unknown kid (-1: worth a re-fetch), and no kid at all. */
    EXPECT(pass_for(&EC2, t), -1);
    EXPECT(mint_raw(&EC1, "ES256", "{\"alg\":\"ES256\",\"typ\":\"at+jwt\"}",
                    claims_json(t, ISSUER, aud, NULL, 1)),
           0);
    EXPECT(mint_raw(&EC2, "ES256", "{\"alg\":\"ES256\",\"typ\":\"at+jwt\"}",
                    claims_json(t, ISSUER, aud, NULL, 1)),
           EACCES);
    EXPECT(mint_raw(&EC1, "ES256", "{\"alg\":\"ES256\",\"typ\":\"at+jwt\",\"kid\":5}",
                    claims_json(t, ISSUER, aud, NULL, 1)),
           EACCES);
    /* Tampering. */
    {
        token = pass_for(&EC1, t);
        char *dot1 = strchr(token, '.'), *dot2 = strchr(dot1 + 1, '.');
        char forged[8192];
        char *evil = b64s(claims_json(t, ISSUER, aud, ",\"org_id\":\"org_EVIL\"", 1));
        snprintf(forged, sizeof(forged), "%.*s.%s%s", (int)(dot1 - token), token, evil, dot2);
        CHECK(verify(v, keys, forged, now) == EACCES);
        free(evil);
        snprintf(forged, sizeof(forged), "%s", token);
        char *sig = strrchr(forged, '.') + 1;
        sig[3] = sig[3] == 'A' ? 'B' : 'A';
        CHECK(verify(v, keys, forged, now) == EACCES);
        snprintf(forged, sizeof(forged), "%sAAAA", token);
        CHECK(verify(v, keys, forged, now) == EACCES);
        snprintf(forged, sizeof(forged), "%s.x", token);
        CHECK(verify(v, keys, forged, now) == EACCES);
        free(token);
    }
    VgiJwtKeySetFree(keys);
    free(set_json);
    VgiJwtVerifierFree(v);

    /* RS256 and PS256 when the allow-list names them. */
    v = hosted_verifier("http://127.0.0.1:9/jwks", "RS256,PS256,ES384");
    set_json = jwks(published, 3);
    CHECK(VgiJwtKeySetParse(set_json, strlen(set_json), &keys) == 0);
    EXPECT(mint(&RSA1, "RS256", "at+jwt", claims_json(t, ISSUER, aud, NULL, 1)), 0);
    EXPECT(mint(&RSA1, "PS256", "at+jwt", claims_json(t, ISSUER, aud, NULL, 1)), 0);
    EXPECT(mint(&EC384, "ES384", "at+jwt", claims_json(t, ISSUER, aud, NULL, 1)), 0);
    EXPECT(pass_for(&EC1, t), EACCES);
    VgiJwtKeySetFree(keys);
    free(set_json);
    VgiJwtVerifierFree(v);
#undef EXPECT
}

/* ---------------------------------------------------------------------- */
/* Malformed and fuzzed input                                              */
/* ---------------------------------------------------------------------- */

static uint64_t rng = 0x9E3779B97F4A7C15ull;
static uint64_t next_random(void) {
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}
static char *segment(const char *json) { return b64s(json); }
static void malformed_tests(void) {
    struct VgiJwtVerifier *v = hosted_verifier("http://127.0.0.1:9/jwks", NULL);
    struct TestKey published[] = {EC1};
    char *set_json = jwks(published, 1);
    struct KeySet *keys = NULL;
    CHECK(VgiJwtKeySetParse(set_json, strlen(set_json), &keys) == 0);
    double now = (double)time(NULL);
    long long t = (long long)now;
    char aud[256];
    snprintf(aud, sizeof(aud), "\"%s\"", AUDIENCE);

    static const char *literal[] = {"",
                                    ".",
                                    "..",
                                    "...",
                                    "a.b",
                                    "a.b.c.d",
                                    "not-a-jwt",
                                    "eyJ.eyJ.",
                                    ".eyJ.sig",
                                    "eyJhbGciOiJFUzI1NiJ9..sig",
                                    "====.====.====",
                                    "e30.e30.e30",
                                    "e30=.e30.e30",
                                    "e30.e30.e3",
                                    "\xff\xfe.\x01.\x02",
                                    " e30.e30.e30"};
    for (size_t i = 0; i < sizeof(literal) / sizeof(*literal); ++i)
        CHECK(verify(v, keys, literal[i], now) == EACCES);

    /* Well-encoded segments holding hostile JSON. */
    static const char *hostile[] = {"[]",
                                    "1",
                                    "\"x\"",
                                    "null",
                                    "{",
                                    "{}x",
                                    "{\"alg\":\"ES256\"}{}",
                                    "{\"alg\":\"ES256\",}",
                                    "{'alg':'ES256'}",
                                    "{\"alg\":NaN}",
                                    "{\"alg\":\"ES\\u0000256\"}",
                                    "{\"alg\":\"\xc3\x28\"}"};
    char *good_claims = segment(claims_json(t, ISSUER, aud, NULL, 1));
    char *good_header = segment("{\"alg\":\"ES256\",\"typ\":\"at+jwt\",\"kid\":\"ec1\"}");
    for (size_t i = 0; i < sizeof(hostile) / sizeof(*hostile); ++i) {
        char *bad = segment(hostile[i]);
        char token[8192];
        snprintf(token, sizeof(token), "%s.%s.AAAA", bad, good_claims);
        CHECK(verify(v, keys, token, now) == EACCES);
        snprintf(token, sizeof(token), "%s.%s.AAAA", good_header, bad);
        CHECK(verify(v, keys, token, now) == EACCES);
        free(bad);
    }
    {
        /* An embedded NUL inside a segment's decoded JSON. */
        unsigned char nul[] = "{\"alg\":\"ES256\"}\0{}";
        char *bad = b64(nul, sizeof(nul) - 1);
        char token[512];
        snprintf(token, sizeof(token), "%s.%s.AAAA", bad, good_claims);
        CHECK(verify(v, keys, token, now) == EACCES);
        free(bad);
    }
    {
        /* Deep nesting stops at the depth limit, signed or not. */
        size_t depth = 5000;
        char *deep = malloc(depth * 2 + 64);
        size_t n = 0;
        n += (size_t)sprintf(deep, "{\"alg\":\"ES256\",\"typ\":\"at+jwt\",\"x\":");
        for (size_t i = 0; i < depth; ++i) deep[n++] = '[';
        for (size_t i = 0; i < depth; ++i) deep[n++] = ']';
        deep[n++] = '}';
        deep[n] = 0;
        char *signed_deep = mint_raw(&EC1, "ES256", deep, claims_json(t, ISSUER, aud, NULL, 1));
        CHECK(verify(v, keys, signed_deep, now) == EACCES);
        free(signed_deep);
        char nested[200];
        strcpy(nested, "{\"alg\":\"ES256\",\"typ\":\"at+jwt\",\"kid\":\"ec1\",\"x\":");
        for (int i = 0; i < 14; ++i) strcat(nested, "[");
        for (int i = 0; i < 14; ++i) strcat(nested, "]");
        strcat(nested, "}");
        signed_deep = mint_raw(&EC1, "ES256", nested, claims_json(t, ISSUER, aud, NULL, 1));
        CHECK(verify(v, keys, signed_deep, now) == 0);
        free(signed_deep);
        free(deep);
    }
    {
        /* Oversized: a validly signed Pass just over 16 KiB is refused. */
        char *padding = malloc(13000);
        memset(padding, 'p', 12999);
        padding[12999] = 0;
        char *extra = malloc(13100);
        sprintf(extra, ",\"pad\":\"%s\"", padding);
        char *big = mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, extra, 1));
        CHECK(strlen(big) > 16384);
        CHECK(verify(v, keys, big, now) == EACCES);
        free(big);
        padding[9000] = 0;
        sprintf(extra, ",\"pad\":\"%s\"", padding);
        big = mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, extra, 1));
        CHECK(strlen(big) < 16384);
        CHECK(verify(v, keys, big, now) == 0);
        free(big);
        free(extra);
        free(padding);
    }

    /* Mutations of a valid token: none may verify, crash or leak. */
    char *valid = pass_for(&EC1, t);
    size_t length = strlen(valid);
    char *mutant = malloc(length + 64);
    static const char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.=+/ \x00\xff";
    for (int round = 0; round < 20000; ++round) {
        memcpy(mutant, valid, length + 1);
        size_t n = length;
        int edits = 1 + (int)(next_random() % 4);
        for (int e = 0; e < edits && n; ++e) {
            size_t at = (size_t)(next_random() % n);
            char c = alphabet[next_random() % (sizeof(alphabet) - 1)];
            switch (next_random() % 4) {
                case 0:
                    mutant[at] = c ? c : 'A';
                    break;
                case 1:
                    memmove(mutant + at, mutant + at + 1, n - at);
                    --n;
                    break;
                case 2:
                    memmove(mutant + at + 1, mutant + at, n - at + 1);
                    mutant[at] = c ? c : 'A';
                    ++n;
                    break;
                default:
                    mutant[at] = 0;
                    n = at;
                    break;
            }
        }
        mutant[n] = 0;
        int rc = verify(v, keys, mutant, now);
        /* A mutation can only verify if it left the signed bytes unchanged. */
        if (rc == 0)
            CHECK(!strcmp(mutant, valid));
        else
            CHECK(rc == EACCES || rc == ESTALE || rc == -1);
    }
    /* Random bytes and random well-formed-looking segments. */
    for (int round = 0; round < 5000; ++round) {
        size_t n = (size_t)(next_random() % 300);
        for (size_t i = 0; i < n; ++i) mutant[i % (length + 63)] = (char)(1 + next_random() % 255);
        mutant[n < length + 63 ? n : length + 63] = 0;
        CHECK(verify(v, keys, mutant, now) == EACCES);
        unsigned char raw[3][96];
        char *parts[3];
        for (int p = 0; p < 3; ++p) {
            size_t size = 1 + (size_t)(next_random() % 95);
            for (size_t i = 0; i < size; ++i) raw[p][i] = (unsigned char)next_random();
            if (p < 2 && next_random() % 2) raw[p][0] = '{';
            parts[p] = b64(raw[p], size);
        }
        char token[1024];
        snprintf(token, sizeof(token), "%s.%s.%s", parts[0], parts[1], parts[2]);
        CHECK(verify(v, keys, token, now) == EACCES);
        for (int p = 0; p < 3; ++p) free(parts[p]);
    }
    free(mutant);
    free(valid);
    free(good_claims);
    free(good_header);
    VgiJwtKeySetFree(keys);
    free(set_json);
    VgiJwtVerifierFree(v);

    /* JWKS documents: hostile ones are refused or yield no usable keys. */
    static const char *bad_sets[] = {
        "", "[]", "{}", "{\"keys\":{}}", "{\"keys\":1}", "{\"keys\":[", "nope"};
    for (size_t i = 0; i < sizeof(bad_sets) / sizeof(*bad_sets); ++i)
        CHECK(VgiJwtKeySetParse(bad_sets[i], strlen(bad_sets[i]), &keys) == EINVAL);
    static const char *unusable =
        "{\"keys\":[1,\"x\",{},{\"kty\":\"oct\",\"k\":\"AAAA\"},"
        "{\"kty\":\"EC\",\"crv\":\"P-256\",\"x\":\"AAAA\",\"y\":\"AAAA\"},"
        "{\"kty\":\"EC\",\"crv\":\"P-256\",\"x\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\","
        "\"y\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\"},"
        "{\"kty\":\"RSA\",\"n\":\"AQAB\",\"e\":\"AQAB\"},"
        "{\"kty\":\"OKP\",\"crv\":\"Ed25519\",\"x\":\"AAAA\"}]}";
    CHECK(VgiJwtKeySetParse(unusable, strlen(unusable), &keys) == 0);
    CHECK(VgiJwtKeySetCount(keys) == 0);
    VgiJwtKeySetFree(keys);
    {
        /* use=enc keys are skipped; use=sig kept. */
        char one[4096], doc[9000];
        jwk(one, sizeof(one), &EC1);
        one[strlen(one) - 1] = 0;
        snprintf(doc, sizeof(doc), "{\"keys\":[%s,\"use\":\"enc\"},%s,\"use\":\"sig\"}]}", one,
                 one);
        CHECK(VgiJwtKeySetParse(doc, strlen(doc), &keys) == 0);
        CHECK(VgiJwtKeySetCount(keys) == 1);
        VgiJwtKeySetFree(keys);
    }
    /* Mutated JWKS documents never crash. */
    set_json = jwks(published, 1);
    length = strlen(set_json);
    char *copy = malloc(length + 1);
    for (int round = 0; round < 3000; ++round) {
        memcpy(copy, set_json, length + 1);
        for (int e = 0; e < 3; ++e) copy[next_random() % length] = (char)(1 + next_random() % 127);
        if (!VgiJwtKeySetParse(copy, length, &keys)) {
            CHECK(VgiJwtKeySetCount(keys) <= 1);
            VgiJwtKeySetFree(keys);
        }
    }
    free(copy);
    free(set_json);
}

/* ---------------------------------------------------------------------- */
/* A loopback JWKS server                                                  */
/* ---------------------------------------------------------------------- */

struct Server {
    int fd;
    unsigned port;
    pthread_mutex_t lock;
    int status;
    char *body;
    char cache_control[128];
    int requests;
    int stop;
    pthread_t thread;
};
static void *serve(void *opaque) {
    struct Server *s = opaque;
    for (;;) {
        int client = accept(s->fd, NULL, NULL);
        if (client < 0) break;
        pthread_mutex_lock(&s->lock);
        int stop = s->stop;
        pthread_mutex_unlock(&s->lock);
        if (stop) {
            close(client);
            break;
        }
        char request[4096];
        size_t used = 0;
        while (used < sizeof(request) - 1) {
            ssize_t n = recv(client, request + used, sizeof(request) - 1 - used, 0);
            if (n <= 0) break;
            used += (size_t)n;
            request[used] = 0;
            if (strstr(request, "\r\n\r\n")) break;
        }
        pthread_mutex_lock(&s->lock);
        ++s->requests;
        char head[512];
        size_t body_size = s->body ? strlen(s->body) : 0;
        int n =
            snprintf(head, sizeof(head),
                     "HTTP/1.1 %d X\r\nContent-Type: application/json\r\nContent-Length: %zu\r\n"
                     "%s%s%sConnection: close\r\n\r\n",
                     s->status, body_size, *s->cache_control ? "Cache-Control: " : "",
                     s->cache_control, *s->cache_control ? "\r\n" : "");
        ssize_t ignored = send(client, head, (size_t)n, 0);
        size_t sent = 0;
        while (sent < body_size) {
            ignored = send(client, s->body + sent, body_size - sent, 0);
            if (ignored <= 0) break;
            sent += (size_t)ignored;
        }
        pthread_mutex_unlock(&s->lock);
        (void)ignored;
        close(client);
    }
    return NULL;
}
static void server_start(struct Server *s) {
    memset(s, 0, sizeof(*s));
    pthread_mutex_init(&s->lock, NULL);
    s->fd = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(s->fd >= 0);
    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(bind(s->fd, (struct sockaddr *)&address, sizeof(address)) == 0);
    socklen_t size = sizeof(address);
    CHECK(getsockname(s->fd, (struct sockaddr *)&address, &size) == 0);
    s->port = ntohs(address.sin_port);
    CHECK(listen(s->fd, 64) == 0);
    s->status = 200;
    CHECK(pthread_create(&s->thread, NULL, serve, s) == 0);
}
static void server_set(struct Server *s, int status, const char *body, const char *cache_control) {
    pthread_mutex_lock(&s->lock);
    s->status = status;
    free(s->body);
    s->body = body ? strdup(body) : NULL;
    snprintf(s->cache_control, sizeof(s->cache_control), "%s", cache_control ? cache_control : "");
    pthread_mutex_unlock(&s->lock);
}
static int server_requests(struct Server *s) {
    pthread_mutex_lock(&s->lock);
    int n = s->requests;
    pthread_mutex_unlock(&s->lock);
    return n;
}
static void server_stop(struct Server *s) {
    pthread_mutex_lock(&s->lock);
    s->stop = 1;
    pthread_mutex_unlock(&s->lock);
    int poke = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons((uint16_t)s->port);
    if (connect(poke, (struct sockaddr *)&address, sizeof(address)) == 0) close(poke);
    pthread_join(s->thread, NULL);
    close(s->fd);
    free(s->body);
}

static double fake_now = 1000;
static double fake_clock(void *state) {
    (void)state;
    return fake_now;
}
static int authenticate(struct VgiJwtVerifier *v, const char *header, const char **reason) {
    struct VgiHttpIdentity identity = {0};
    struct ArrowError error = {{0}};
    int rc = VgiJwtAuthenticate(v, header, &identity, &error);
    *reason = identity.failure_reason;
    if (!rc) {
        CHECK(!strcmp(identity.principal, "user_01M7KNJYC18T"));
        CHECK(!strcmp(identity.domain, "orchard"));
        CHECK(strstr(identity.claims_json, "\"email_verified\":true"));
        CHECK(!identity.failure_reason);
    } else
        CHECK(!identity.principal && !identity.claims_json && identity.failure_reason);
    VgiJwtReleaseIdentity(v, &identity);
    return rc;
}
static char *bearer(const char *token) {
    char *out = malloc(strlen(token) + 8);
    sprintf(out, "Bearer %s", token);
    return out;
}

static void cache_tests(void) {
    struct Server s;
    server_start(&s);
    struct TestKey one[] = {EC1}, both[] = {EC1, EC2};
    char *jwks_one = jwks(one, 1), *jwks_both = jwks(both, 2);
    server_set(&s, 200, jwks_one, NULL);
    char uri[128];
    snprintf(uri, sizeof(uri), "http://127.0.0.1:%u/jwks", s.port);
    struct VgiJwtVerifier *v = hosted_verifier(uri, NULL);
    VgiJwtSetClock(v, fake_clock, NULL, NULL);
    long long t = (long long)time(NULL);
    char *good = pass_for(&EC1, t), *good_header = bearer(good);
    char *other = pass_for(&EC2, t), *other_header = bearer(other);
    const char *reason = NULL;

    /* Missing and non-Bearer credentials never touch the network. */
    CHECK(authenticate(v, "", &reason) == EACCES && !strcmp(reason, "missing_credential"));
    CHECK(authenticate(v, NULL, &reason) == EACCES && !strcmp(reason, "missing_credential"));
    CHECK(authenticate(v, "Basic dXNlcjpwYXNz", &reason) == EACCES &&
          !strcmp(reason, "invalid_credential"));
    CHECK(authenticate(v, "Bearer ", &reason) == EACCES);
    CHECK(server_requests(&s) == 0);
    /* Nothing cached: the early check cannot decide. */
    CHECK(VgiJwtPrecheck(v, good_header) == 0);
    CHECK(VgiJwtPrecheck(v, "") == EACCES);
    CHECK(VgiJwtPrecheck(v, "Basic x") == EACCES);
    CHECK(server_requests(&s) == 0);

    /* First use fetches; the keys then serve until 600 s, not after. */
    CHECK(authenticate(v, good_header, &reason) == 0);
    CHECK(server_requests(&s) == 1);
    CHECK(authenticate(v, good_header, &reason) == 0);
    CHECK(server_requests(&s) == 1);
    fake_now += 599;
    CHECK(authenticate(v, good_header, &reason) == 0);
    CHECK(server_requests(&s) == 1);
    fake_now += 2;
    CHECK(authenticate(v, good_header, &reason) == 0);
    CHECK(server_requests(&s) == 2);

    /* The early check with cached keys. */
    CHECK(VgiJwtPrecheck(v, good_header) == 0);
    CHECK(VgiJwtPrecheck(v, "Bearer not-a-jwt") == EACCES);
    CHECK(VgiJwtPrecheck(v, other_header) == 0); /* unknown kid: maybe after a refresh */
    {
        char aud[256], extra[64];
        snprintf(aud, sizeof(aud), "\"%s\"", AUDIENCE);
        snprintf(extra, sizeof(extra), ",\"exp\":%lld", t - 120);
        char *expired = mint(&EC1, "ES256", "at+jwt", claims_json(t, ISSUER, aud, extra, 0));
        char *header = bearer(expired);
        CHECK(VgiJwtPrecheck(v, header) == ESTALE);
        CHECK(authenticate(v, header, &reason) == ESTALE && !strcmp(reason, "expired_credential"));
        free(header);
        free(expired);
    }
    CHECK(server_requests(&s) == 2);

    /* A shorter Cache-Control max-age wins over the 600 s cap. */
    server_set(&s, 200, jwks_one, "public, max-age=30");
    fake_now += 601;
    CHECK(authenticate(v, good_header, &reason) == 0);
    CHECK(server_requests(&s) == 3);
    fake_now += 29;
    CHECK(authenticate(v, good_header, &reason) == 0);
    CHECK(server_requests(&s) == 3);
    fake_now += 2;
    CHECK(authenticate(v, good_header, &reason) == 0);
    CHECK(server_requests(&s) == 4);
    /* A longer one does not stretch it. */
    server_set(&s, 200, jwks_one, "max-age=86400");
    fake_now += 31;
    CHECK(authenticate(v, good_header, &reason) == 0);
    CHECK(server_requests(&s) == 5);
    fake_now += 601;
    CHECK(authenticate(v, good_header, &reason) == 0);
    CHECK(server_requests(&s) == 6);

    /* JWKS down: unexpired keys keep working; expired keys are never used. */
    server_set(&s, 503, "{}", NULL);
    fake_now += 300;
    CHECK(authenticate(v, good_header, &reason) == 0);
    CHECK(server_requests(&s) == 6);
    fake_now += 301;
    CHECK(authenticate(v, good_header, &reason) == EAGAIN &&
          !strcmp(reason, "identity_unavailable"));
    CHECK(server_requests(&s) == 7);
    CHECK(VgiJwtPrecheck(v, good_header) == 0);
    /* Garbage is also unavailable, not refused, while there are no keys. */
    CHECK(authenticate(v, "Bearer not-a-jwt", &reason) == EAGAIN);
    CHECK(authenticate(v, good_header, &reason) == EAGAIN);
    CHECK(server_requests(&s) == 9);
    /* Every kind of bad response is a failed fetch. */
    server_set(&s, 200, "not json", NULL);
    CHECK(authenticate(v, good_header, &reason) == EAGAIN);
    server_set(&s, 302, "", NULL);
    CHECK(authenticate(v, good_header, &reason) == EAGAIN);
    server_set(&s, 200, "{\"keys\":7}", NULL);
    CHECK(authenticate(v, good_header, &reason) == EAGAIN);
    {
        char *huge = malloc(1024 * 1024 + 64);
        memset(huge, ' ', 1024 * 1024 + 32);
        memcpy(huge, "{\"keys\":[]", 10);
        strcpy(huge + 1024 * 1024 + 32, "}");
        server_set(&s, 200, huge, NULL);
        free(huge);
        CHECK(authenticate(v, good_header, &reason) == EAGAIN);
    }
    /* Recovery. */
    server_set(&s, 200, jwks_one, NULL);
    CHECK(authenticate(v, good_header, &reason) == 0);
    int base = server_requests(&s);

    /* Unknown kid: one re-fetch, then refused without fetching for 60 s. */
    fake_now += 61;
    CHECK(authenticate(v, other_header, &reason) == EACCES &&
          !strcmp(reason, "invalid_credential"));
    CHECK(server_requests(&s) == base + 1);
    CHECK(authenticate(v, other_header, &reason) == EACCES);
    CHECK(server_requests(&s) == base + 1);
    /* The key is published, but the limit still holds... */
    server_set(&s, 200, jwks_both, NULL);
    fake_now += 59;
    CHECK(authenticate(v, other_header, &reason) == EACCES);
    CHECK(server_requests(&s) == base + 1);
    /* ...until 60 s after the last fetch. */
    fake_now += 2;
    CHECK(authenticate(v, other_header, &reason) == 0);
    CHECK(server_requests(&s) == base + 2);
    CHECK(authenticate(v, good_header, &reason) == 0);
    CHECK(server_requests(&s) == base + 2);
    /* Unknown kid while the JWKS is down: unavailable, not refused. */
    {
        struct TestKey third = ec_key("ec3", "P-256", 256);
        char *token = pass_for(&third, t), *header = bearer(token);
        server_set(&s, 500, "", NULL);
        fake_now += 61;
        CHECK(authenticate(v, header, &reason) == EAGAIN);
        /* The cached keys are still in date, so known kids keep working. */
        CHECK(authenticate(v, good_header, &reason) == 0);
        free(header);
        free(token);
        EVP_PKEY_free(third.pkey);
    }

    VgiJwtVerifierFree(v);
    free(good);
    free(good_header);
    free(other);
    free(other_header);
    free(jwks_one);
    free(jwks_both);
    server_stop(&s);
}

/* Many threads through an expiring cache: no races, no stale keys. */
struct Racer {
    struct VgiJwtVerifier *v;
    const char *header;
    int failures;
};
static void *race(void *opaque) {
    struct Racer *r = opaque;
    for (int i = 0; i < 200; ++i) {
        const char *reason = NULL;
        if (authenticate(r->v, r->header, &reason)) ++r->failures;
    }
    return NULL;
}
static double real_offset_clock(void *state) {
    (void)state;
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    /* Ten "seconds" per real millisecond: the 1 s max-age expires constantly. */
    return ((double)now.tv_sec + (double)now.tv_nsec / 1e9) * 10000;
}
static void concurrency_tests(void) {
    struct Server s;
    server_start(&s);
    struct TestKey one[] = {EC1};
    char *set_json = jwks(one, 1);
    server_set(&s, 200, set_json, "max-age=1");
    char uri[128];
    snprintf(uri, sizeof(uri), "http://127.0.0.1:%u/jwks", s.port);
    struct VgiJwtVerifier *v = hosted_verifier(uri, NULL);
    VgiJwtSetClock(v, real_offset_clock, NULL, NULL);
    char *token = pass_for(&EC1, (long long)time(NULL)), *header = bearer(token);
    struct Racer racers[8];
    pthread_t threads[8];
    for (int i = 0; i < 8; ++i) {
        racers[i] = (struct Racer){v, header, 0};
        CHECK(pthread_create(&threads[i], NULL, race, &racers[i]) == 0);
    }
    for (int i = 0; i < 8; ++i) {
        pthread_join(threads[i], NULL);
        CHECK(racers[i].failures == 0);
    }
    CHECK(server_requests(&s) > 1);
    VgiJwtVerifierFree(v);
    free(token);
    free(header);
    free(set_json);
    server_stop(&s);
}

int main(void) {
    CHECK(sodium_init() >= 0);
    EC1 = ec_key("ec1", "P-256", 256);
    EC2 = ec_key("ec2", "P-256", 256);
    EC384 = ec_key("ec384", "P-384", 384);
    RSA1 = rsa_key("rsa1");
    option_tests();
    token_tests();
    malformed_tests();
    cache_tests();
    concurrency_tests();
    EVP_PKEY_free(EC1.pkey);
    EVP_PKEY_free(EC2.pkey);
    EVP_PKEY_free(EC384.pkey);
    EVP_PKEY_free(RSA1.pkey);
    puts("jwt: ok");
    return 0;
}
