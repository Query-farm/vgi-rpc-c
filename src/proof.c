#define _POSIX_C_SOURCE 200809L
#include "vgi_rpc/proof.h"

#include <errno.h>
#include <pthread.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>

struct ProofKey {
    char kid[65];
    unsigned char secret[32];
};
struct NonceEntry {
    char nonce[23];
    uint64_t expires;
    size_t next;
    size_t bucket;
};
struct VgiProxyProof {
    enum VgiProofMode mode;
    char origin[256];
    struct ProofKey *keys;
    size_t key_count;
    unsigned skew;
    pthread_mutex_t mutex;
    struct NonceEntry *nonces;
    size_t *buckets;
    size_t capacity, bucket_count, next_entry;
};

static int alphabet(const char *s, size_t min, size_t max, const char *extra) {
    size_t n = strlen(s);
    if (n < min || n > max) return 0;
    for (size_t i = 0; i < n; ++i) {
        unsigned char c = (unsigned char)s[i];
        if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
              strchr(extra, c)))
            return 0;
    }
    return 1;
}
void VgiProofOptionsInit(struct VgiProofOptions *options) {
    memset(options, 0, sizeof(*options));
    options->skew_seconds = 30;
    options->replay_capacity = 100000;
}
void VgiProxyProofFree(struct VgiProxyProof *proof) {
    if (!proof) return;
    if (proof->keys) {
        sodium_memzero(proof->keys, proof->key_count * sizeof(*proof->keys));
        free(proof->keys);
    }
    free(proof->nonces);
    free(proof->buckets);
    pthread_mutex_destroy(&proof->mutex);
    free(proof);
}
int VgiProxyProofNew(const struct VgiProofOptions *options, struct VgiProxyProof **out,
                     struct ArrowError *error) {
    if (!options || !out || *out) return EINVAL;
    if (options->mode < VGI_PROOF_OFF || options->mode > VGI_PROOF_REQUIRE) return EINVAL;
    if (options->mode == VGI_PROOF_OFF) return 0;
    if (!options->origin_id || !alphabet(options->origin_id, 1, 255, "._:/-") ||
        !options->secrets || !*options->secrets) {
        ArrowErrorSet(error, "Proxy proof requires a valid origin_id and at least one secret");
        return EINVAL;
    }
    struct VgiProxyProof *p = calloc(1, sizeof(*p));
    if (!p) return ENOMEM;
    int rc = pthread_mutex_init(&p->mutex, NULL);
    if (rc) {
        free(p);
        return rc;
    }
    p->mode = options->mode;
    p->skew = options->skew_seconds;
    strcpy(p->origin, options->origin_id);
    const char *start = options->secrets;
    do {
        const char *end = strchr(start, ',');
        if (!end) end = start + strlen(start);
        const char *colon = memchr(start, ':', (size_t)(end - start));
        struct ProofKey key = {0};
        if (!colon || colon == start || colon - start > 64 || end - colon - 1 != 64) {
            rc = EINVAL;
            break;
        }
        memcpy(key.kid, start, (size_t)(colon - start));
        size_t decoded = 0;
        if (!alphabet(key.kid, 1, 64, "_-") ||
            sodium_hex2bin(key.secret, sizeof(key.secret), colon + 1, 64, NULL, &decoded, NULL) ||
            decoded != 32)
            rc = EINVAL;
        for (size_t i = 0; !rc && i < p->key_count; ++i)
            if (!strcmp(p->keys[i].kid, key.kid)) rc = EINVAL;
        if (!rc) {
            struct ProofKey *keys = realloc(p->keys, (p->key_count + 1) * sizeof(*keys));
            if (!keys)
                rc = ENOMEM;
            else {
                p->keys = keys;
                p->keys[p->key_count++] = key;
            }
        }
        sodium_memzero(&key, sizeof(key));
        if (!*end) break;
        start = end + 1;
    } while (!rc);
    p->capacity = options->replay_capacity;
    if (!rc && p->capacity) {
        if (p->capacity > SIZE_MAX / 2 / sizeof(size_t))
            rc = EOVERFLOW;
        else {
            p->bucket_count = p->capacity * 2;
            p->nonces = calloc(p->capacity, sizeof(*p->nonces));
            p->buckets = calloc(p->bucket_count, sizeof(*p->buckets));
            if (!p->nonces || !p->buckets) rc = ENOMEM;
        }
    }
    if (rc) {
        ArrowErrorSet(error, "Invalid proxy proof configuration (error %d)", rc);
        VgiProxyProofFree(p);
        return rc;
    }
    *out = p;
    return 0;
}
static int nonce_insert(struct VgiProxyProof *p, const char *nonce, uint64_t now,
                        uint64_t expires) {
    if (!p->capacity) return 0;
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < 22; ++i) hash = (hash ^ (unsigned char)nonce[i]) * 1099511628211ULL;
    size_t bucket = (size_t)(hash % p->bucket_count);
    pthread_mutex_lock(&p->mutex);
    for (size_t i = p->buckets[bucket]; i; i = p->nonces[i - 1].next) {
        struct NonceEntry *entry = &p->nonces[i - 1];
        if (entry->expires >= now && !strcmp(entry->nonce, nonce)) {
            pthread_mutex_unlock(&p->mutex);
            return EACCES;
        }
    }
    size_t slot = p->next_entry++ % p->capacity;
    struct NonceEntry *entry = &p->nonces[slot];
    if (*entry->nonce) {
        size_t *link = &p->buckets[entry->bucket];
        while (*link && *link != slot + 1) link = &p->nonces[*link - 1].next;
        if (*link) *link = entry->next;
    }
    strcpy(entry->nonce, nonce);
    entry->expires = expires;
    entry->bucket = bucket;
    entry->next = p->buckets[bucket];
    p->buckets[bucket] = slot + 1;
    pthread_mutex_unlock(&p->mutex);
    return 0;
}
static const char *verify(struct VgiProxyProof *p, const char *token, size_t count, uint64_t now,
                          struct VgiProofResult *result) {
    if (!count) return "no_proof";
    if (count != 1 || !token || !*token || strlen(token) > 512) return "malformed";
    char text[513];
    strcpy(text, token);
    char *fields[5] = {text};
    for (size_t i = 1; i < 5; ++i) {
        char *dot = strchr(fields[i - 1], '.');
        if (!dot) return "malformed";
        *dot = 0;
        fields[i] = dot + 1;
    }
    if (strcmp(fields[0], "v1") || !alphabet(fields[1], 1, 64, "_-") || strlen(fields[2]) < 1 ||
        strlen(fields[2]) > 20 || !alphabet(fields[3], 22, 22, "_-") ||
        !alphabet(fields[4], 43, 43, "_-"))
        return "malformed";
    uint64_t timestamp = 0;
    int overflow = 0;
    for (const char *s = fields[2]; *s; ++s) {
        if (*s < '0' || *s > '9') return "malformed";
        unsigned digit = (unsigned)(*s - '0');
        if (timestamp > (UINT64_MAX - digit) / 10) overflow = 1;
        if (!overflow) timestamp = timestamp * 10 + digit;
    }
    strcpy(result->kid, fields[1]);
    const struct ProofKey *key = NULL;
    for (size_t i = 0; i < p->key_count; ++i)
        if (!strcmp(p->keys[i].kid, fields[1])) key = &p->keys[i];
    if (!key) return "unknown_kid";
    if (overflow || (timestamp > now && timestamp - now > p->skew)) return "not_yet_valid";
    if (now > timestamp && now - timestamp > p->skew) return "expired";
    unsigned char supplied[32], expected[32];
    size_t decoded = 0;
    if (sodium_base642bin(supplied, sizeof(supplied), fields[4], 43, NULL, &decoded, NULL,
                          sodium_base64_VARIANT_URLSAFE_NO_PADDING) ||
        decoded != 32)
        return "malformed";
    crypto_auth_hmacsha256_state mac;
    crypto_auth_hmacsha256_init(&mac, key->secret, sizeof(key->secret));
    const char *parts[] = {"vgi.proxy.proof.v1", fields[1], fields[2], fields[3], p->origin};
    for (size_t i = 0; i < 5; ++i)
        crypto_auth_hmacsha256_update(&mac, (const unsigned char *)parts[i],
                                      strlen(parts[i]) + (i < 4));
    crypto_auth_hmacsha256_final(&mac, expected);
    if (sodium_memcmp(supplied, expected, sizeof(expected))) return "bad_mac";
    uint64_t expires = timestamp > UINT64_MAX - p->skew ? UINT64_MAX : timestamp + p->skew;
    if (nonce_insert(p, fields[3], now, expires)) return "replayed";
    result->verified = 1;
    return "ok";
}
int VgiProxyProofCheck(struct VgiProxyProof *proof, const char *token, size_t count, uint64_t now,
                       struct VgiProofResult *result) {
    if (!result) return EINVAL;
    memset(result, 0, sizeof(*result));
    result->reason = proof ? verify(proof, token, count, now, result) : "off";
    return proof && proof->mode == VGI_PROOF_REQUIRE && !result->verified ? EACCES : 0;
}
