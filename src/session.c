#define _POSIX_C_SOURCE 200809L
#include "session.h"

#include <errno.h>
#include <pthread.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct VgiSessionEntry {
    unsigned char id[12];
    uint64_t created, expires;
    void *state;
    void (*release)(void *);
    pthread_mutex_t mutex;
    size_t references;
    int closed;
    struct VgiSessionEntry *next;
};
struct VgiSessions {
    pthread_mutex_t mutex;
    unsigned char key[32];
    char server_id[256];
    struct VgiSessionEntry *entries;
    unsigned ttl;
    size_t count, capacity;
    int draining;
};
static void put64(unsigned char *p, uint64_t value) {
    for (int i = 0; i < 8; ++i) p[i] = (unsigned char)(value >> (8 * i));
}
static uint64_t get64(const unsigned char *p) {
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i) value |= (uint64_t)p[i] << (8 * i);
    return value;
}
static void entry_free(struct VgiSessionEntry *entry) {
    if (entry->release) entry->release(entry->state);
    pthread_mutex_destroy(&entry->mutex);
    free(entry);
}
int VgiSessionsNew(const unsigned char *key, const char *id, unsigned ttl, size_t capacity,
                   struct VgiSessions **out) {
    if (!key || !id || strlen(id) > 255 || !ttl || !capacity || !out || *out) return EINVAL;
    struct VgiSessions *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    int rc = pthread_mutex_init(&s->mutex, NULL);
    if (rc) {
        free(s);
        return rc;
    }
    memcpy(s->key, key, 32);
    strcpy(s->server_id, id);
    s->ttl = ttl;
    s->capacity = capacity;
    *out = s;
    return 0;
}
void VgiSessionsFree(struct VgiSessions *s) {
    if (!s) return;
    struct VgiSessionEntry *entry = s->entries;
    while (entry) {
        struct VgiSessionEntry *next = entry->next;
        entry_free(entry);
        entry = next;
    }
    pthread_mutex_destroy(&s->mutex);
    sodium_memzero(s->key, 32);
    free(s);
}
void VgiSessionsDrain(struct VgiSessions *s, int draining) {
    if (!s) return;
    pthread_mutex_lock(&s->mutex);
    s->draining = draining;
    pthread_mutex_unlock(&s->mutex);
}
void VgiSessionsSweep(struct VgiSessions *s) {
    if (!s) return;
    uint64_t now = (uint64_t)time(NULL);
    struct VgiSessionEntry *garbage = NULL;
    pthread_mutex_lock(&s->mutex);
    struct VgiSessionEntry **link = &s->entries;
    while (*link) {
        struct VgiSessionEntry *entry = *link;
        if (!entry->references && (entry->closed || entry->expires <= now)) {
            *link = entry->next;
            entry->next = garbage;
            garbage = entry;
            --s->count;
        } else
            link = &entry->next;
    }
    pthread_mutex_unlock(&s->mutex);
    while (garbage) {
        struct VgiSessionEntry *next = garbage->next;
        entry_free(garbage);
        garbage = next;
    }
}
static int aad(struct VgiRpcCall *call, struct ArrowBuffer *out) {
    static const char prefix[] = "vgi_rpc.state.v4";
    int rc = ArrowBufferAppend(out, prefix, sizeof(prefix));
    if (!rc && call->principal && *call->principal) {
        rc = ArrowBufferAppend(out, "\1", 1);
        const char *domain = call->auth_domain ? call->auth_domain : "";
        if (!rc) rc = ArrowBufferAppend(out, domain, (int64_t)strlen(domain) + 1);
        if (!rc) rc = ArrowBufferAppend(out, call->principal, (int64_t)strlen(call->principal));
    } else if (!rc)
        rc = ArrowBufferAppend(out, "\0anonymous", 10);
    return rc;
}
static int seal(struct VgiSessions *s, struct VgiRpcCall *call, const struct VgiSessionEntry *entry,
                char *out, size_t capacity) {
    unsigned char raw[284], envelope[325];
    size_t n = strlen(s->server_id);
    put64(raw, entry->created);
    raw[8] = (unsigned char)n;
    memcpy(raw + 9, s->server_id, n);
    memcpy(raw + 9 + n, entry->id, 12);
    put64(raw + 21 + n, entry->expires);
    envelope[0] = 1;
    randombytes_buf(envelope + 1, 24);
    struct ArrowBuffer binding;
    ArrowBufferInit(&binding);
    int rc = aad(call, &binding);
    unsigned long long length = 0;
    if (!rc)
        rc = crypto_aead_xchacha20poly1305_ietf_encrypt(
            envelope + 25, &length, raw, 29 + n, binding.data,
            (unsigned long long)binding.size_bytes, NULL, envelope + 1, s->key);
    if (!rc && !sodium_bin2base64(out, capacity, envelope, (size_t)length + 25,
                                  sodium_base64_VARIANT_URLSAFE_NO_PADDING))
        rc = EOVERFLOW;
    ArrowBufferReset(&binding);
    return rc;
}
static int open_token(struct VgiSessions *s, struct VgiRpcCall *call, const char *token,
                      unsigned char *id, uint64_t now) {
    unsigned char raw[284], envelope[325];
    size_t n = 0;
    if (strlen(token) > 511 ||
        sodium_base642bin(envelope, sizeof(envelope), token, strlen(token), NULL, &n, NULL,
                          sodium_base64_VARIANT_URLSAFE_NO_PADDING) ||
        n < 70 || envelope[0] != 1)
        return EINVAL;
    struct ArrowBuffer binding;
    ArrowBufferInit(&binding);
    int rc = aad(call, &binding);
    unsigned long long length = 0;
    if (!rc)
        rc = crypto_aead_xchacha20poly1305_ietf_decrypt(
            raw, &length, NULL, envelope + 25, n - 25, binding.data,
            (unsigned long long)binding.size_bytes, envelope + 1, s->key);
    ArrowBufferReset(&binding);
    if (rc || length < 29 || length != (unsigned long long)(29 + raw[8]) ||
        raw[8] != strlen(s->server_id) || memcmp(raw + 9, s->server_id, raw[8]) ||
        get64(raw + 21 + raw[8]) <= now)
        return EINVAL;
    memcpy(id, raw + 9 + raw[8], 12);
    return 0;
}
static int lost(struct VgiRpcCall *call) {
    return VgiRpcFail(call, "SessionLostError", "FAILED_PRECONDITION", "session_lost",
                      "Sticky session is unavailable");
}
int VgiSessionBind(struct VgiSessions *s, struct VgiRpcCall *call, const char *token, int accepted,
                   struct VgiSessionBinding *binding) {
    memset(binding, 0, sizeof(*binding));
    binding->registry = s;
    binding->accepted = accepted;
    binding->action = "none";
    call->session_binding = binding;
    if (!s || !token || !*token) return 0;
    unsigned char id[12];
    uint64_t now = (uint64_t)time(NULL);
    if (open_token(s, call, token, id, now)) return lost(call);
    pthread_mutex_lock(&s->mutex);
    struct VgiSessionEntry *entry = s->entries;
    while (entry && memcmp(entry->id, id, 12)) entry = entry->next;
    if (entry && !entry->closed && entry->expires > now)
        ++entry->references;
    else
        entry = NULL;
    pthread_mutex_unlock(&s->mutex);
    if (!entry) return lost(call);
    pthread_mutex_lock(&entry->mutex);
    binding->entry = entry;
    pthread_mutex_lock(&s->mutex);
    int invalid = entry->closed || entry->expires <= (uint64_t)time(NULL);
    pthread_mutex_unlock(&s->mutex);
    if (invalid) {
        VgiSessionUnbind(binding);
        return lost(call);
    }
    sodium_bin2hex(binding->id, sizeof(binding->id), entry->id, 12);
    binding->action = "resume";
    return 0;
}
void VgiSessionUnbind(struct VgiSessionBinding *binding) {
    struct VgiSessionEntry *entry = binding->entry;
    if (!entry) return;
    binding->entry = NULL;
    pthread_mutex_unlock(&entry->mutex);
    pthread_mutex_lock(&binding->registry->mutex);
    --entry->references;
    pthread_mutex_unlock(&binding->registry->mutex);
    VgiSessionsSweep(binding->registry);
}
int VgiRpcSessionOpen(struct VgiRpcCall *call, void *state, void (*release)(void *), unsigned ttl) {
    struct VgiSessionBinding *b = call->session_binding;
    if (!b || !b->registry)
        return VgiRpcFail(call, "RuntimeError", "UNKNOWN", "",
                          "sticky sessions not available on this transport");
    if (!b->accepted)
        return VgiRpcFail(call, "RuntimeError", "UNKNOWN", "",
                          "Opening a session requires VGI-Session-Accept: true");
    if (b->entry)
        return VgiRpcFail(call, "RuntimeError", "UNKNOWN", "", "A sticky session is already bound");
    struct VgiSessions *s = b->registry;
    VgiSessionsSweep(s);
    struct VgiSessionEntry *entry = calloc(1, sizeof(*entry));
    if (!entry) return ENOMEM;
    int rc = pthread_mutex_init(&entry->mutex, NULL);
    if (rc) {
        free(entry);
        return rc;
    }
    randombytes_buf(entry->id, sizeof(entry->id));
    entry->created = (uint64_t)time(NULL);
    entry->expires = entry->created + (ttl ? ttl : s->ttl);
    rc = seal(s, call, entry, b->token, sizeof(b->token));
    pthread_mutex_lock(&entry->mutex);
    pthread_mutex_lock(&s->mutex);
    if (!rc && s->draining)
        rc = VgiRpcFail(call, "ServerDrainingError", "UNAVAILABLE", "server_draining",
                        "Server is draining");
    if (!rc && s->count >= s->capacity)
        rc = VgiRpcFail(call, "ResourceExhaustedError", "RESOURCE_EXHAUSTED", "",
                        "Session capacity reached");
    if (!rc) {
        entry->state = state;
        entry->release = release;
        entry->references = 1;
        entry->next = s->entries;
        s->entries = entry;
        ++s->count;
    }
    pthread_mutex_unlock(&s->mutex);
    if (rc) {
        b->token[0] = 0;
        pthread_mutex_unlock(&entry->mutex);
        entry_free(entry);
        return rc;
    }
    b->entry = entry;
    b->closed = 0;
    b->action = "open";
    sodium_bin2hex(b->id, sizeof(b->id), entry->id, 12);
    return 0;
}
void *VgiRpcSessionGet(struct VgiRpcCall *call) {
    struct VgiSessionBinding *b = call->session_binding;
    return b && b->entry && !b->closed ? b->entry->state : NULL;
}
int VgiRpcSessionClose(struct VgiRpcCall *call) {
    struct VgiSessionBinding *b = call->session_binding;
    if (!b || !b->registry)
        return VgiRpcFail(call, "RuntimeError", "UNKNOWN", "",
                          "sticky sessions not available on this transport");
    if (b->entry && !b->closed) {
        b->closed = 1;
        pthread_mutex_lock(&b->registry->mutex);
        b->entry->closed = 1;
        pthread_mutex_unlock(&b->registry->mutex);
        void (*release)(void *) = b->entry->release;
        void *state = b->entry->state;
        b->entry->state = NULL;
        b->entry->release = NULL;
        if (release) release(state);
    }
    b->closed = 1;
    b->token[0] = 0;
    b->action = "close";
    return 0;
}
int VgiRpcSessionsSetDraining(struct VgiRpcCall *call, int draining) {
    if (!call->session_binding || !call->session_binding->registry) return ENOTSUP;
    VgiSessionsDrain(call->session_binding->registry, draining);
    return 0;
}
