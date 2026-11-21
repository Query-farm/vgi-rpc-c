#define _POSIX_C_SOURCE 200809L
#include "vgi_rpc/jwt.h"

#include <ctype.h>
#include <curl/curl.h>
#include <errno.h>
#include <json-c/json.h>
#include <limits.h>
#include <math.h>
#include <openssl/bn.h>
#include <openssl/core_names.h>
#include <openssl/ec.h>
#include <openssl/evp.h>
#include <openssl/param_build.h>
#include <openssl/rsa.h>
#include <pthread.h>
#include <sodium.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#include "jwt_internal.h"

#define MAX_TOKEN_BYTES 16384
#define MAX_JSON_DEPTH 16
#define MAX_JWKS_BYTES (1024 * 1024)
#define MAX_KEYS 64
#define MAX_KID_BYTES 256
#define MIN_RSA_BITS 2048
#define MAX_RSA_BYTES 1024
#define UNKNOWN_KID (-1)

enum Family { FAMILY_EC, FAMILY_RSA, FAMILY_PSS };
struct Algorithm {
    const char *name;
    enum Family family;
    int ec_bits; /* EC curve size; zero for RSA keys. */
    const char *digest;
};
static const struct Algorithm algorithms[] = {
    {"ES256", FAMILY_EC, 256, "SHA256"}, {"ES384", FAMILY_EC, 384, "SHA384"},
    {"ES512", FAMILY_EC, 521, "SHA512"}, {"RS256", FAMILY_RSA, 0, "SHA256"},
    {"RS384", FAMILY_RSA, 0, "SHA384"},  {"RS512", FAMILY_RSA, 0, "SHA512"},
    {"PS256", FAMILY_PSS, 0, "SHA256"},  {"PS384", FAMILY_PSS, 0, "SHA384"},
    {"PS512", FAMILY_PSS, 0, "SHA512"},
};
#define ALGORITHM_COUNT (sizeof(algorithms) / sizeof(*algorithms))

struct Key {
    char *kid; /* NULL when the JWK has none. */
    char *alg; /* The JWK's own "alg", or NULL. */
    int ec_bits;
    EVP_PKEY *pkey;
};
struct KeySet {
    struct Key *keys;
    size_t count;
    double expires_at; /* Monotonic seconds; meaningful when expires is set. */
    int expires;
    unsigned refs;
};
struct VgiJwtVerifier {
    char *issuer, *audience, *jwks_uri, *typ, *principal_claim, *domain;
    unsigned allowed; /* Bit i set: algorithms[i] is allowed. */
    int require_exp, single_audience;
    long long leeway;
    double max_age, refresh_interval, timeout;
    pthread_mutex_t state; /* Guards cache and last_fetch. */
    pthread_mutex_t fetch; /* Serializes fetches; never held while waiting on state. */
    struct KeySet *cache;
    double last_fetch;
    int has_last_fetch;
    VgiJwtClock monotonic, wall;
    void *clock_state;
};

static pthread_once_t curl_once = PTHREAD_ONCE_INIT;
static void curl_start(void) { curl_global_init(CURL_GLOBAL_DEFAULT); }

static double default_monotonic(void *state) {
    (void)state;
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (double)now.tv_sec + (double)now.tv_nsec / 1e9;
}
static double default_wall(void *state) {
    (void)state;
    return (double)time(NULL);
}
static char *dup_text(const char *text) {
    if (!text) return NULL;
    size_t n = strlen(text) + 1;
    char *out = malloc(n);
    if (out) memcpy(out, text, n);
    return out;
}

/* ---------------------------------------------------------------------- */
/* Decoding                                                                */
/* ---------------------------------------------------------------------- */

/* Strict base64url without padding: libsodium refuses foreign characters,
 * padding, impossible lengths and non-zero trailing bits. */
static int b64url(const char *in, size_t n, unsigned char **out, size_t *size) {
    *out = NULL;
    *size = 0;
    if (!n || n > MAX_JWKS_BYTES) return EINVAL;
    size_t capacity = n / 4 * 3 + 3;
    unsigned char *buffer = malloc(capacity + 1);
    if (!buffer) return ENOMEM;
    const char *end = NULL;
    size_t decoded = 0;
    if (sodium_base642bin(buffer, capacity, in, n, NULL, &decoded, &end,
                          sodium_base64_VARIANT_URLSAFE_NO_PADDING) ||
        end != in + n) {
        free(buffer);
        return EINVAL;
    }
    buffer[decoded] = 0;
    *out = buffer;
    *size = decoded;
    return 0;
}
static int b64url_json(struct json_object *object, const char *name, unsigned char **out,
                       size_t *size) {
    struct json_object *value = NULL;
    if (!json_object_object_get_ex(object, name, &value) ||
        !json_object_is_type(value, json_type_string))
        return EINVAL;
    return b64url(json_object_get_string(value), (size_t)json_object_get_string_len(value), out,
                  size);
}

/* One JSON object, the whole input, strict grammar (no NaN, no trailing
 * garbage), bounded depth and no embedded NUL. */
static struct json_object *parse_object(const char *data, size_t size) {
    if (!size || size > (size_t)INT_MAX || memchr(data, 0, size)) return NULL;
    struct json_tokener *tokener = json_tokener_new_ex(MAX_JSON_DEPTH);
    if (!tokener) return NULL;
    int flags = JSON_TOKENER_STRICT;
#ifdef JSON_TOKENER_VALIDATE_UTF8
    flags |= JSON_TOKENER_VALIDATE_UTF8;
#endif
    json_tokener_set_flags(tokener, flags);
    struct json_object *object = json_tokener_parse_ex(tokener, data, (int)size);
    size_t used;
#if defined(JSON_C_VERSION_NUM) && JSON_C_VERSION_NUM >= ((0 << 16) | (15 << 8))
    used = json_tokener_get_parse_end(tokener);
#else
    used = (size_t)tokener->char_offset;
#endif
    int ok = object && json_tokener_get_error(tokener) == json_tokener_success &&
             json_object_is_type(object, json_type_object);
    for (size_t i = used; ok && i < size; ++i)
        ok = data[i] == ' ' || data[i] == '\t' || data[i] == '\n' || data[i] == '\r';
    json_tokener_free(tokener);
    if (!ok && object) {
        json_object_put(object);
        object = NULL;
    }
    return object;
}
static const char *string_member(struct json_object *object, const char *name, int *present) {
    struct json_object *value = NULL;
    *present = json_object_object_get_ex(object, name, &value);
    return *present && json_object_is_type(value, json_type_string) ? json_object_get_string(value)
                                                                    : NULL;
}
/* A JSON number (not a boolean) that is finite, as a double. */
static int number_member(struct json_object *object, const char *name, int *present, double *out) {
    struct json_object *value = NULL;
    *present = json_object_object_get_ex(object, name, &value);
    if (!*present) return 0;
    if (json_object_is_type(value, json_type_int)) {
        *out = (double)json_object_get_int64(value);
        return 1;
    }
    if (json_object_is_type(value, json_type_double)) {
        *out = json_object_get_double(value);
        return isfinite(*out);
    }
    return 0;
}

/* ---------------------------------------------------------------------- */
/* Keys                                                                    */
/* ---------------------------------------------------------------------- */

static int public_check(EVP_PKEY *pkey) {
    EVP_PKEY_CTX *ctx = EVP_PKEY_CTX_new_from_pkey(NULL, pkey, NULL);
    int ok = ctx && EVP_PKEY_public_check(ctx) == 1;
    EVP_PKEY_CTX_free(ctx);
    return ok;
}
static EVP_PKEY *from_params(const char *type, OSSL_PARAM *params) {
    EVP_PKEY *pkey = NULL;
    EVP_PKEY_CTX *ctx = EVP_PKEY_CTX_new_from_name(NULL, type, NULL);
    if (!ctx || EVP_PKEY_fromdata_init(ctx) != 1 ||
        EVP_PKEY_fromdata(ctx, &pkey, EVP_PKEY_PUBLIC_KEY, params) != 1)
        pkey = NULL;
    EVP_PKEY_CTX_free(ctx);
    if (pkey && !public_check(pkey)) {
        EVP_PKEY_free(pkey);
        pkey = NULL;
    }
    return pkey;
}
static EVP_PKEY *import_ec(struct json_object *jwk, int *bits) {
    int present = 0;
    const char *crv = string_member(jwk, "crv", &present);
    const char *group = NULL;
    size_t coordinate = 0;
    if (crv && !strcmp(crv, "P-256")) {
        group = "prime256v1";
        coordinate = 32;
        *bits = 256;
    } else if (crv && !strcmp(crv, "P-384")) {
        group = "secp384r1";
        coordinate = 48;
        *bits = 384;
    } else if (crv && !strcmp(crv, "P-521")) {
        group = "secp521r1";
        coordinate = 66;
        *bits = 521;
    } else
        return NULL;
    unsigned char *x = NULL, *y = NULL;
    size_t x_size = 0, y_size = 0;
    EVP_PKEY *pkey = NULL;
    if (!b64url_json(jwk, "x", &x, &x_size) && !b64url_json(jwk, "y", &y, &y_size) &&
        x_size == coordinate && y_size == coordinate) {
        unsigned char point[1 + 2 * 66];
        point[0] = 0x04;
        memcpy(point + 1, x, coordinate);
        memcpy(point + 1 + coordinate, y, coordinate);
        OSSL_PARAM params[] = {
            OSSL_PARAM_construct_utf8_string(OSSL_PKEY_PARAM_GROUP_NAME, (char *)group, 0),
            OSSL_PARAM_construct_octet_string(OSSL_PKEY_PARAM_PUB_KEY, point, 1 + 2 * coordinate),
            OSSL_PARAM_construct_end()};
        pkey = from_params("EC", params);
    }
    free(x);
    free(y);
    return pkey;
}
static EVP_PKEY *import_rsa(struct json_object *jwk) {
    unsigned char *n = NULL, *e = NULL;
    size_t n_size = 0, e_size = 0;
    EVP_PKEY *pkey = NULL;
    BIGNUM *bn_n = NULL, *bn_e = NULL;
    OSSL_PARAM_BLD *builder = NULL;
    OSSL_PARAM *params = NULL;
    if (b64url_json(jwk, "n", &n, &n_size) || b64url_json(jwk, "e", &e, &e_size) ||
        n_size > MAX_RSA_BYTES || e_size > 8)
        goto done;
    bn_n = BN_bin2bn(n, (int)n_size, NULL);
    bn_e = BN_bin2bn(e, (int)e_size, NULL);
    if (!bn_n || !bn_e || BN_num_bits(bn_n) < MIN_RSA_BITS || !BN_is_odd(bn_e) || BN_is_one(bn_e))
        goto done;
    builder = OSSL_PARAM_BLD_new();
    if (!builder || !OSSL_PARAM_BLD_push_BN(builder, OSSL_PKEY_PARAM_RSA_N, bn_n) ||
        !OSSL_PARAM_BLD_push_BN(builder, OSSL_PKEY_PARAM_RSA_E, bn_e))
        goto done;
    params = OSSL_PARAM_BLD_to_param(builder);
    if (params) pkey = from_params("RSA", params);
done:
    OSSL_PARAM_free(params);
    OSSL_PARAM_BLD_free(builder);
    BN_free(bn_n);
    BN_free(bn_e);
    free(n);
    free(e);
    return pkey;
}
static void key_set_free(struct KeySet *set) {
    if (!set) return;
    for (size_t i = 0; i < set->count; ++i) {
        free(set->keys[i].kid);
        free(set->keys[i].alg);
        EVP_PKEY_free(set->keys[i].pkey);
    }
    free(set->keys);
    free(set);
}
/* Keys that cannot be used to verify a signature (symmetric, unsupported,
 * malformed, encryption-only) are skipped: they can never admit a token. */
static int parse_jwks(const char *body, size_t size, struct KeySet **out) {
    *out = NULL;
    struct json_object *root = parse_object(body, size), *keys = NULL;
    if (!root) return EINVAL;
    if (!json_object_object_get_ex(root, "keys", &keys) ||
        !json_object_is_type(keys, json_type_array)) {
        json_object_put(root);
        return EINVAL;
    }
    struct KeySet *set = calloc(1, sizeof(*set));
    if (set) set->keys = calloc(MAX_KEYS, sizeof(*set->keys));
    if (!set || !set->keys) {
        free(set);
        json_object_put(root);
        return ENOMEM;
    }
    size_t length = json_object_array_length(keys);
    for (size_t i = 0; i < length && set->count < MAX_KEYS; ++i) {
        struct json_object *jwk = json_object_array_get_idx(keys, i);
        if (!json_object_is_type(jwk, json_type_object)) continue;
        int present = 0;
        const char *kty = string_member(jwk, "kty", &present);
        const char *use = string_member(jwk, "use", &present);
        if (present && (!use || strcmp(use, "sig"))) continue;
        const char *kid = string_member(jwk, "kid", &present);
        if (present && (!kid || strlen(kid) > MAX_KID_BYTES)) continue;
        const char *alg = string_member(jwk, "alg", &present);
        if (present && !alg) continue;
        int bits = 0;
        EVP_PKEY *pkey = !kty                  ? NULL
                         : !strcmp(kty, "EC")  ? import_ec(jwk, &bits)
                         : !strcmp(kty, "RSA") ? import_rsa(jwk)
                                               : NULL;
        if (!pkey) continue;
        struct Key *key = &set->keys[set->count];
        key->pkey = pkey;
        key->ec_bits = bits;
        key->kid = kid ? dup_text(kid) : NULL;
        key->alg = alg ? dup_text(alg) : NULL;
        ++set->count;
        if ((kid && !key->kid) || (alg && !key->alg)) {
            key_set_free(set);
            json_object_put(root);
            return ENOMEM;
        }
    }
    json_object_put(root);
    *out = set;
    return 0;
}

/* ---------------------------------------------------------------------- */
/* Signatures                                                              */
/* ---------------------------------------------------------------------- */

static int key_fits(const struct Key *key, const struct Algorithm *alg) {
    if (key->alg && strcmp(key->alg, alg->name)) return 0;
    return alg->family == FAMILY_EC ? key->ec_bits == alg->ec_bits : key->ec_bits == 0;
}
static int verify_signature(const struct Key *key, const struct Algorithm *alg,
                            const unsigned char *message, size_t message_size,
                            const unsigned char *signature, size_t signature_size) {
    const EVP_MD *md = EVP_get_digestbyname(alg->digest);
    if (!md) return 0;
    unsigned char *der = NULL;
    const unsigned char *checked = signature;
    size_t checked_size = signature_size;
    if (alg->family == FAMILY_EC) {
        /* JWS carries r || s, each the curve's width; OpenSSL wants DER. */
        size_t coordinate = ((size_t)alg->ec_bits + 7) / 8;
        if (signature_size != 2 * coordinate) return 0;
        ECDSA_SIG *sig = ECDSA_SIG_new();
        BIGNUM *r = BN_bin2bn(signature, (int)coordinate, NULL);
        BIGNUM *s = BN_bin2bn(signature + coordinate, (int)coordinate, NULL);
        if (!sig || !r || !s || !ECDSA_SIG_set0(sig, r, s)) {
            ECDSA_SIG_free(sig);
            BN_free(r);
            BN_free(s);
            return 0;
        }
        int length = i2d_ECDSA_SIG(sig, &der);
        ECDSA_SIG_free(sig);
        if (length <= 0) return 0;
        checked = der;
        checked_size = (size_t)length;
    } else if (signature_size != (size_t)EVP_PKEY_get_size(key->pkey))
        return 0;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_PKEY_CTX *pkey_ctx = NULL;
    int ok = ctx && EVP_DigestVerifyInit(ctx, &pkey_ctx, md, NULL, key->pkey) == 1;
    if (ok && alg->family == FAMILY_PSS)
        ok = EVP_PKEY_CTX_set_rsa_padding(pkey_ctx, RSA_PKCS1_PSS_PADDING) == 1 &&
             EVP_PKEY_CTX_set_rsa_pss_saltlen(pkey_ctx, RSA_PSS_SALTLEN_DIGEST) == 1 &&
             EVP_PKEY_CTX_set_rsa_mgf1_md(pkey_ctx, md) == 1;
    else if (ok && alg->family == FAMILY_RSA)
        ok = EVP_PKEY_CTX_set_rsa_padding(pkey_ctx, RSA_PKCS1_PADDING) == 1;
    ok = ok && EVP_DigestVerify(ctx, checked, checked_size, message, message_size) == 1;
    EVP_MD_CTX_free(ctx);
    OPENSSL_free(der);
    return ok;
}

/* ---------------------------------------------------------------------- */
/* Tokens                                                                  */
/* ---------------------------------------------------------------------- */

static const char *const REASON_MISSING = "missing_credential";
static const char *const REASON_INVALID = "invalid_credential";
static const char *const REASON_EXPIRED = "expired_credential";

static int typ_matches(const char *actual, const char *expected) {
    while (*actual == ' ' || *actual == '\t') ++actual;
    size_t n = strlen(actual);
    while (n && (actual[n - 1] == ' ' || actual[n - 1] == '\t')) --n;
    if (n >= 12 && !strncasecmp(actual, "application/", 12)) {
        actual += 12;
        n -= 12;
    }
    return n == strlen(expected) && !strncasecmp(actual, expected, n);
}
static int audience_matches(const struct VgiJwtVerifier *v, struct json_object *claims) {
    struct json_object *aud = NULL;
    if (!json_object_object_get_ex(claims, "aud", &aud)) return 0;
    if (json_object_is_type(aud, json_type_string))
        return !strcmp(json_object_get_string(aud), v->audience);
    if (!json_object_is_type(aud, json_type_array)) return 0;
    size_t n = json_object_array_length(aud);
    if (!n || (v->single_audience && n != 1)) return 0;
    int matched = 0;
    for (size_t i = 0; i < n; ++i) {
        struct json_object *item = json_object_array_get_idx(aud, i);
        if (!json_object_is_type(item, json_type_string)) return 0;
        matched |= !strcmp(json_object_get_string(item), v->audience);
    }
    return matched;
}
static int check_claims(const struct VgiJwtVerifier *v, struct json_object *claims, double now,
                        const char **reason) {
    *reason = REASON_INVALID;
    int present = 0;
    const char *iss = string_member(claims, "iss", &present);
    if (!iss || strcmp(iss, v->issuer)) return EACCES;
    if (!audience_matches(v, claims)) return EACCES;
    double value = 0;
    int ok = number_member(claims, "exp", &present, &value);
    if (present && !ok) return EACCES;
    if (!present && v->require_exp) return EACCES;
    if (present && value <= now - (double)v->leeway) {
        *reason = REASON_EXPIRED;
        return ESTALE;
    }
    ok = number_member(claims, "nbf", &present, &value);
    if (present && (!ok || value > now + (double)v->leeway)) return EACCES;
    ok = number_member(claims, "iat", &present, &value);
    if (present && (!ok || value > now + (double)v->leeway)) return EACCES;
    const char *principal = string_member(claims, v->principal_claim, &present);
    if (!principal || !*principal) return EACCES;
    *reason = NULL;
    return 0;
}

int VgiJwtVerifyWithKeys(const struct VgiJwtVerifier *v, const struct KeySet *keys,
                         const char *token, double now, char **claims_out, char **principal_out,
                         const char **reason) {
    *claims_out = NULL;
    *principal_out = NULL;
    *reason = REASON_INVALID;
    size_t length = strnlen(token, MAX_TOKEN_BYTES + 1);
    if (!length || length > MAX_TOKEN_BYTES) return EACCES;
    const char *dot1 = memchr(token, '.', length);
    const char *dot2 = dot1 ? memchr(dot1 + 1, '.', length - (size_t)(dot1 + 1 - token)) : NULL;
    if (!dot1 || !dot2 || memchr(dot2 + 1, '.', length - (size_t)(dot2 + 1 - token))) return EACCES;
    size_t header_size = (size_t)(dot1 - token), payload_size = (size_t)(dot2 - dot1 - 1),
           signature_size = length - (size_t)(dot2 + 1 - token);
    unsigned char *header_json = NULL, *payload_json = NULL, *signature = NULL;
    size_t header_length = 0, payload_length = 0, signature_length = 0;
    struct json_object *header = NULL, *claims = NULL;
    int rc = b64url(token, header_size, &header_json, &header_length);
    if (!rc) rc = b64url(dot1 + 1, payload_size, &payload_json, &payload_length);
    if (!rc) rc = b64url(dot2 + 1, signature_size, &signature, &signature_length);
    if (rc) {
        rc = rc == ENOMEM ? ENOMEM : EACCES;
        goto done;
    }
    header = parse_object((const char *)header_json, header_length);
    claims = parse_object((const char *)payload_json, payload_length);
    rc = EACCES;
    if (!header || !claims) goto done;
    int present = 0;
    if (json_object_object_get_ex(header, "crit", NULL)) goto done;
    const char *alg_name = string_member(header, "alg", &present);
    const struct Algorithm *alg = NULL;
    for (size_t i = 0; alg_name && i < ALGORITHM_COUNT; ++i)
        if ((v->allowed & (1u << i)) && !strcmp(alg_name, algorithms[i].name)) alg = &algorithms[i];
    if (!alg) goto done;
    if (v->typ) {
        const char *typ = string_member(header, "typ", &present);
        if (!typ || !typ_matches(typ, v->typ)) goto done;
    }
    const char *kid = string_member(header, "kid", &present);
    if (present && (!kid || strlen(kid) > MAX_KID_BYTES)) goto done;
    int kid_known = 0, verified = 0;
    for (size_t i = 0; keys && i < keys->count && !verified; ++i) {
        const struct Key *key = &keys->keys[i];
        if (kid && (!key->kid || strcmp(kid, key->kid))) continue;
        kid_known = 1;
        if (key_fits(key, alg))
            verified = verify_signature(key, alg, (const unsigned char *)token,
                                        (size_t)(dot2 - token), signature, signature_length);
    }
    if (!verified) {
        /* Only a kid missing from the key set is worth a re-fetch. */
        if (kid && !kid_known) rc = UNKNOWN_KID;
        goto done;
    }
    rc = check_claims(v, claims, now, reason);
    if (rc) goto done;
    *principal_out = dup_text(string_member(claims, v->principal_claim, &present));
    *claims_out = malloc(payload_length + 1);
    if (!*principal_out || !*claims_out) {
        free(*principal_out);
        free(*claims_out);
        *principal_out = *claims_out = NULL;
        rc = ENOMEM;
        goto done;
    }
    memcpy(*claims_out, payload_json, payload_length + 1);
done:
    if (header) json_object_put(header);
    if (claims) json_object_put(claims);
    free(header_json);
    free(payload_json);
    free(signature);
    return rc;
}

/* ---------------------------------------------------------------------- */
/* JWKS fetch and cache                                                    */
/* ---------------------------------------------------------------------- */

struct Fetch {
    char *body;
    size_t size;
    int overflow;
    char cache_control[512];
};
static size_t on_body(char *data, size_t size, size_t count, void *opaque) {
    struct Fetch *f = opaque;
    size_t n = size * count;
    if (n > MAX_JWKS_BYTES - f->size) {
        f->overflow = 1;
        return 0;
    }
    char *grown = realloc(f->body, f->size + n + 1);
    if (!grown) return 0;
    f->body = grown;
    memcpy(f->body + f->size, data, n);
    f->size += n;
    f->body[f->size] = 0;
    return n;
}
static size_t on_header(char *data, size_t size, size_t count, void *opaque) {
    struct Fetch *f = opaque;
    size_t n = size * count;
    if (n >= 5 && !memcmp(data, "HTTP/", 5)) {
        f->cache_control[0] = 0; /* A new (possibly interim) response. */
        return n;
    }
    if (n > 14 && !strncasecmp(data, "cache-control:", 14)) {
        const char *value = data + 14;
        size_t length = n - 14;
        while (length && (*value == ' ' || *value == '\t')) {
            ++value;
            --length;
        }
        while (length && (value[length - 1] == '\r' || value[length - 1] == '\n' ||
                          value[length - 1] == ' ' || value[length - 1] == '\t'))
            --length;
        size_t used = strlen(f->cache_control);
        /* Repeated headers combine as one comma-separated list. */
        if (used && used + 2 < sizeof(f->cache_control)) {
            memcpy(f->cache_control + used, ", ", 2);
            used += 2;
        }
        if (length > sizeof(f->cache_control) - 1 - used)
            length = sizeof(f->cache_control) - 1 - used;
        memcpy(f->cache_control + used, value, length);
        f->cache_control[used + length] = 0;
    }
    return n;
}
int VgiJwtCacheControlMaxAge(const char *value, double *out) {
    const char *p = value;
    while (p && *p) {
        const char *end = strchr(p, ',');
        if (!end) end = p + strlen(p);
        const char *q = p;
        while (q < end && isspace((unsigned char)*q)) ++q;
        if ((size_t)(end - q) >= 7 && !strncasecmp(q, "max-age", 7)) {
            q += 7;
            while (q < end && isspace((unsigned char)*q)) ++q;
            if (q < end && *q == '=') {
                ++q;
                while (q < end && isspace((unsigned char)*q)) ++q;
                int quoted = q < end && *q == '"';
                if (quoted) ++q;
                const char *digits = q;
                double seconds = 0;
                while (q < end && isdigit((unsigned char)*q)) {
                    if (seconds < 1e15) seconds = seconds * 10 + (*q - '0');
                    ++q;
                }
                int ok = q > digits;
                if (ok && quoted) ok = q < end && *q++ == '"';
                while (ok && q < end && isspace((unsigned char)*q)) ++q;
                if (ok && q == end) {
                    *out = seconds;
                    return 1;
                }
            }
        }
        p = *end ? end + 1 : end;
    }
    return 0;
}
static int fetch_keys(const struct VgiJwtVerifier *v, struct KeySet **out, double *advertised,
                      int *has_advertised) {
    *out = NULL;
    *has_advertised = 0;
    pthread_once(&curl_once, curl_start);
    CURL *curl = curl_easy_init();
    if (!curl) return ENOMEM;
    struct Fetch f = {0};
    long timeout = (long)(v->timeout * 1000);
    if (timeout < 1) timeout = 1;
    curl_easy_setopt(curl, CURLOPT_URL, v->jwks_uri);
    curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, "http,https");
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, timeout);
    curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, "");
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "vgi-rpc-c/" VGI_RPC_VERSION);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, on_body);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &f);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, on_header);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &f);
    CURLcode result = curl_easy_perform(curl);
    long status = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
    curl_easy_cleanup(curl);
    int rc = 0;
    if (result != CURLE_OK) {
        fprintf(stderr, "vgi-rpc-c: JWKS fetch failed (%s)\n",
                f.overflow ? "response too large" : curl_easy_strerror(result));
        rc = EIO;
    } else if (status < 200 || status > 299) {
        fprintf(stderr, "vgi-rpc-c: JWKS fetch failed (HTTP %ld)\n", status);
        rc = EIO;
    } else {
        rc = f.body ? parse_jwks(f.body, f.size, out) : EINVAL;
        if (rc == EINVAL) fprintf(stderr, "vgi-rpc-c: JWKS fetch failed (not a JWK Set)\n");
        if (!rc) *has_advertised = VgiJwtCacheControlMaxAge(f.cache_control, advertised);
    }
    free(f.body);
    return rc;
}
static int usable(const struct KeySet *set, double now) {
    return set && (!set->expires || now < set->expires_at);
}
/* Callers hold v->state. */
static void unref_locked(struct KeySet *set) {
    if (set && !--set->refs) key_set_free(set);
}
static void unref(struct VgiJwtVerifier *v, struct KeySet *set) {
    if (!set) return;
    pthread_mutex_lock(&v->state);
    unref_locked(set);
    pthread_mutex_unlock(&v->state);
}
static struct KeySet *cached(struct VgiJwtVerifier *v) {
    pthread_mutex_lock(&v->state);
    struct KeySet *set = usable(v->cache, v->monotonic(v->clock_state)) ? v->cache : NULL;
    if (set) ++set->refs;
    pthread_mutex_unlock(&v->state);
    return set;
}
/* The caller holds v->fetch. On failure the cache keeps only keys still within
 * their maximum age; expired keys are dropped so nothing can read them. */
static int fetch_locked(struct VgiJwtVerifier *v, struct KeySet **out) {
    *out = NULL;
    double started = v->monotonic(v->clock_state);
    pthread_mutex_lock(&v->state);
    v->last_fetch = started;
    v->has_last_fetch = 1;
    pthread_mutex_unlock(&v->state);
    struct KeySet *fresh = NULL;
    double advertised = 0;
    int has_advertised = 0;
    int rc = fetch_keys(v, &fresh, &advertised, &has_advertised);
    pthread_mutex_lock(&v->state);
    if (rc) {
        if (!usable(v->cache, v->monotonic(v->clock_state))) {
            unref_locked(v->cache);
            v->cache = NULL;
        }
        pthread_mutex_unlock(&v->state);
        return rc == ENOMEM ? ENOMEM : EAGAIN;
    }
    if (v->max_age > 0) {
        double ttl = v->max_age;
        if (has_advertised && advertised < ttl) ttl = advertised;
        /* From when the fetch started, so a slow response cannot stretch it. */
        fresh->expires = 1;
        fresh->expires_at = started + ttl;
    }
    fresh->refs = 2; /* The cache's reference and the caller's. */
    unref_locked(v->cache);
    v->cache = fresh;
    pthread_mutex_unlock(&v->state);
    *out = fresh;
    return 0;
}
static int current_keys(struct VgiJwtVerifier *v, struct KeySet **out) {
    *out = cached(v);
    if (*out) return 0;
    pthread_mutex_lock(&v->fetch);
    *out = cached(v);
    int rc = *out ? 0 : fetch_locked(v, out);
    pthread_mutex_unlock(&v->fetch);
    return rc;
}
/* Re-fetch after an unknown kid. *out is NULL when rate limited. If another
 * thread refreshed while this one waited, its result is reused. */
static int refresh_for_unknown_kid(struct VgiJwtVerifier *v, const struct KeySet *seen,
                                   struct KeySet **out) {
    *out = NULL;
    pthread_mutex_lock(&v->fetch);
    pthread_mutex_lock(&v->state);
    double now = v->monotonic(v->clock_state);
    if (v->cache && v->cache != seen && usable(v->cache, now)) {
        *out = v->cache;
        ++v->cache->refs;
        pthread_mutex_unlock(&v->state);
        pthread_mutex_unlock(&v->fetch);
        return 0;
    }
    int limited =
        v->refresh_interval > 0 && v->has_last_fetch && now - v->last_fetch < v->refresh_interval;
    pthread_mutex_unlock(&v->state);
    int rc = limited ? 0 : fetch_locked(v, out);
    pthread_mutex_unlock(&v->fetch);
    return rc;
}

/* ---------------------------------------------------------------------- */
/* Public API                                                              */
/* ---------------------------------------------------------------------- */

void VgiJwtOptionsInit(struct VgiJwtOptions *options) {
    memset(options, 0, sizeof(*options));
    options->jwks_fetch_timeout_seconds = 10;
}
int VgiJwtParseAlgorithms(const char *list, unsigned *out, struct ArrowError *error) {
    *out = 0;
    if (!list || !*list) list = "ES256";
    const char *p = list;
    while (*p) {
        const char *end = strchr(p, ',');
        if (!end) end = p + strlen(p);
        const char *start = p;
        while (start < end && isspace((unsigned char)*start)) ++start;
        const char *stop = end;
        while (stop > start && isspace((unsigned char)stop[-1])) --stop;
        size_t n = (size_t)(stop - start);
        if (n) {
            if ((n == 4 && !strncasecmp(start, "none", 4)) ||
                (n >= 2 && !strncasecmp(start, "hs", 2))) {
                ArrowErrorSet(error,
                              "JWT algorithm %.*s is not allowed: tokens verified against a JWKS "
                              "are asymmetric, never 'none' or HS*",
                              (int)n, start);
                return EINVAL;
            }
            size_t i = 0;
            while (i < ALGORITHM_COUNT &&
                   (strlen(algorithms[i].name) != n || strncmp(algorithms[i].name, start, n)))
                ++i;
            if (i == ALGORITHM_COUNT) {
                ArrowErrorSet(error, "Unsupported JWT algorithm %.*s", (int)n, start);
                return EINVAL;
            }
            *out |= 1u << i;
        }
        p = *end ? end + 1 : end;
    }
    if (!*out) {
        ArrowErrorSet(error, "The JWT algorithm list is empty");
        return EINVAL;
    }
    return 0;
}
int VgiJwtVerifierNew(const struct VgiJwtOptions *options, struct VgiJwtVerifier **out,
                      struct ArrowError *error) {
    if (!out) return EINVAL;
    *out = NULL;
    if (!options || !options->issuer || !*options->issuer || !options->audience ||
        !*options->audience || !options->jwks_uri || !*options->jwks_uri) {
        ArrowErrorSet(error, "JWT verification needs an issuer, an audience and a JWKS URI");
        return EINVAL;
    }
    if (strncasecmp(options->jwks_uri, "https://", 8) &&
        strncasecmp(options->jwks_uri, "http://", 7)) {
        ArrowErrorSet(error, "The JWKS URI must be an http or https URL");
        return EINVAL;
    }
    if (options->required_typ && !*options->required_typ) {
        ArrowErrorSet(error, "The required JWT typ must not be empty");
        return EINVAL;
    }
    unsigned allowed = 0;
    int rc = VgiJwtParseAlgorithms(options->algorithms, &allowed, error);
    if (rc) return rc;
    struct VgiJwtVerifier *v = calloc(1, sizeof(*v));
    if (!v) return ENOMEM;
    v->allowed = allowed;
    v->issuer = dup_text(options->issuer);
    v->audience = dup_text(options->audience);
    v->jwks_uri = dup_text(options->jwks_uri);
    v->typ = dup_text(options->required_typ);
    v->principal_claim = dup_text(
        options->principal_claim && *options->principal_claim ? options->principal_claim : "sub");
    v->domain = dup_text(options->domain && *options->domain ? options->domain : "jwt");
    v->require_exp = options->require_exp;
    v->single_audience = options->single_audience;
    v->leeway = options->leeway_seconds;
    v->max_age = options->jwks_max_age_seconds;
    v->refresh_interval = options->jwks_refresh_min_interval_seconds;
    v->timeout = options->jwks_fetch_timeout_seconds > 0 ? options->jwks_fetch_timeout_seconds : 10;
    v->monotonic = default_monotonic;
    v->wall = default_wall;
    pthread_mutex_init(&v->state, NULL);
    pthread_mutex_init(&v->fetch, NULL);
    if (!v->issuer || !v->audience || !v->jwks_uri || (options->required_typ && !v->typ) ||
        !v->principal_claim || !v->domain) {
        VgiJwtVerifierFree(v);
        return ENOMEM;
    }
    *out = v;
    return 0;
}
void VgiJwtVerifierFree(struct VgiJwtVerifier *v) {
    if (!v) return;
    key_set_free(v->cache);
    pthread_mutex_destroy(&v->state);
    pthread_mutex_destroy(&v->fetch);
    free(v->issuer);
    free(v->audience);
    free(v->jwks_uri);
    free(v->typ);
    free(v->principal_claim);
    free(v->domain);
    free(v);
}
/* The scheme is case-insensitive (RFC 7235); the token follows one space. */
static int bearer(const char *authorization, const char **token, const char **reason) {
    if (!authorization || !*authorization) {
        *reason = REASON_MISSING;
        return EACCES;
    }
    if (strncasecmp(authorization, "Bearer ", 7) || !authorization[7]) {
        *reason = REASON_INVALID;
        return EACCES;
    }
    *token = authorization + 7;
    return 0;
}
int VgiJwtAuthenticate(void *state, const char *authorization, struct VgiHttpIdentity *out,
                       struct ArrowError *error) {
    (void)error;
    struct VgiJwtVerifier *v = state;
    const char *token = NULL, *reason = NULL;
    out->principal = out->domain = out->claims_json = NULL;
    out->has_auth_time = 0;
    int rc = bearer(authorization, &token, &reason);
    if (rc) {
        out->failure_reason = reason;
        return rc;
    }
    struct KeySet *keys = NULL;
    rc = current_keys(v, &keys);
    if (rc) {
        out->failure_reason = "identity_unavailable";
        return rc == ENOMEM ? ENOMEM : EAGAIN;
    }
    char *claims = NULL, *principal = NULL;
    double now = v->wall(v->clock_state);
    rc = VgiJwtVerifyWithKeys(v, keys, token, now, &claims, &principal, &reason);
    if (rc == UNKNOWN_KID) {
        struct KeySet *fresh = NULL;
        int refreshed = refresh_for_unknown_kid(v, keys, &fresh);
        if (refreshed) {
            unref(v, keys);
            out->failure_reason = "identity_unavailable";
            return refreshed == ENOMEM ? ENOMEM : EAGAIN;
        }
        rc = fresh ? VgiJwtVerifyWithKeys(v, fresh, token, now, &claims, &principal, &reason)
                   : EACCES;
        if (rc == UNKNOWN_KID) rc = EACCES;
        unref(v, fresh);
    }
    unref(v, keys);
    if (rc) {
        out->failure_reason = rc == ESTALE ? REASON_EXPIRED : REASON_INVALID;
        return rc == ENOMEM ? ENOMEM : rc == ESTALE ? ESTALE : EACCES;
    }
    out->principal = principal;
    out->claims_json = claims;
    out->domain = v->domain;
    out->failure_reason = NULL;
    return 0;
}
void VgiJwtReleaseIdentity(void *state, struct VgiHttpIdentity *identity) {
    (void)state;
    if (!identity) return;
    free((char *)identity->principal);
    free((char *)identity->claims_json);
    identity->principal = identity->claims_json = NULL;
}
int VgiJwtPrecheck(void *state, const char *authorization) {
    struct VgiJwtVerifier *v = state;
    const char *token = NULL, *reason = NULL;
    int rc = bearer(authorization, &token, &reason);
    if (rc) return rc;
    struct KeySet *keys = cached(v);
    if (!keys) return 0;
    char *claims = NULL, *principal = NULL;
    rc =
        VgiJwtVerifyWithKeys(v, keys, token, v->wall(v->clock_state), &claims, &principal, &reason);
    unref(v, keys);
    free(claims);
    free(principal);
    return rc == EACCES || rc == ESTALE ? rc : 0;
}
void VgiJwtInstall(struct VgiJwtVerifier *v, struct VgiHttpOptions *options) {
    options->authenticate = VgiJwtAuthenticate;
    options->authenticate_request = NULL;
    options->authenticate_early = VgiJwtPrecheck;
    options->release_identity = VgiJwtReleaseIdentity;
    options->auth_state = v;
}

/* Test hooks (jwt_internal.h). */
void VgiJwtSetClock(struct VgiJwtVerifier *v, VgiJwtClock monotonic, VgiJwtClock wall,
                    void *state) {
    v->monotonic = monotonic ? monotonic : default_monotonic;
    v->wall = wall ? wall : default_wall;
    v->clock_state = state;
}
int VgiJwtKeySetParse(const char *json, size_t size, struct KeySet **out) {
    int rc = parse_jwks(json, size, out);
    if (!rc) (*out)->refs = 1;
    return rc;
}
void VgiJwtKeySetFree(struct KeySet *set) { key_set_free(set); }
size_t VgiJwtKeySetCount(const struct KeySet *set) { return set ? set->count : 0; }
