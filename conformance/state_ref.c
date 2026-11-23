/* Copyright 2026 Query Farm LLC.
 * Query Farm Source-Available License, Version 1.0; see LICENSE. */
/* StateRef, object cache and argument-reference conformance methods
 * (vgi-rpc STATE_REF_V1.md §15). */
#include <errno.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>

#include "protocol.h"
#include "vgi_rpc/rpc.h"

int ConformanceStateRefInit(struct VgiRpcCall *, struct VgiRpcReply *);
int ConformanceObjectCacheStats(struct VgiRpcCall *, struct VgiRpcReply *);
int ConformanceObjectCacheAge(struct VgiRpcCall *, struct VgiRpcReply *);
int ConformanceResolveArgumentRef(struct VgiRpcCall *, struct VgiRpcReply *);

/* state_ref_payload(size, compressible, seed), byte for byte. */
static int payload(int64_t size, int compressible, struct ArrowStringView seed,
                   struct ArrowBuffer *out) {
    if (size <= 0) return 0;
    int rc = ArrowBufferReserve(out, size);
    if (compressible) {
        static const char head[] = "vgi-rpc StateRef conformance ", tail[] = " ";
        while (!rc && out->size_bytes < size) {
            const struct ArrowBufferView parts[] = {
                {{head}, (int64_t)sizeof(head) - 1}, {{seed.data}, seed.size_bytes}, {{tail}, 1}};
            for (int i = 0; !rc && i < 3 && out->size_bytes < size; ++i) {
                int64_t n = parts[i].size_bytes;
                if (n > size - out->size_bytes) n = size - out->size_bytes;
                rc = ArrowBufferAppend(out, parts[i].data.data, n);
            }
        }
        return rc;
    }
    for (uint64_t i = 0; !rc && out->size_bytes < size; ++i) {
        crypto_hash_sha256_state state;
        uint8_t counter[8], digest[32];
        for (int b = 0; b < 8; ++b) counter[b] = (uint8_t)(i >> (b * 8));
        crypto_hash_sha256_init(&state);
        crypto_hash_sha256_update(&state, (const uint8_t *)seed.data,
                                  (unsigned long long)seed.size_bytes);
        crypto_hash_sha256_update(&state, counter, sizeof(counter));
        crypto_hash_sha256_final(&state, digest);
        int64_t n = size - out->size_bytes < 32 ? size - out->size_bytes : 32;
        rc = ArrowBufferAppend(out, digest, n);
    }
    return rc;
}
static void hex_digest(const void *data, int64_t size, char out[65]) {
    uint8_t digest[32];
    crypto_hash_sha256(digest, data, (unsigned long long)size);
    sodium_bin2hex(out, 65, digest, sizeof(digest));
}

struct StateRefStream {
    struct VgiStateRef *value;
    struct ArrowBuffer padding; /* held only to size a serialized cursor */
    int misuse;
    struct ArrowSchema schema;
};
static void state_ref_release(void *data) {
    struct StateRefStream *s = data;
    VgiStateRefRelease(s->value);
    ArrowBufferReset(&s->padding);
    if (s->schema.release) ArrowSchemaRelease(&s->schema);
    free(s);
}
static int state_ref_next(void *data, struct VgiRpcCall *c, const struct VgiBatch *input,
                          const char *metadata, struct VgiBatch *out, struct ArrowBuffer *md,
                          int *finished) {
    (void)metadata;
    (void)md;
    (void)finished;
    struct StateRefStream *s = data;
    if (s->misuse) {
        static const char text[] = "a continuation may not create a StateRef";
        struct VgiStateRef *refused = NULL;
        int rc = VgiRpcStateRef(c, (struct ArrowBufferView){{text}, sizeof(text) - 1}, &refused);
        VgiStateRefRelease(refused);
        if (rc) return rc;
        return VgiRpcFail(c, "RuntimeError", "INTERNAL", "",
                          "state_ref() on a continuation was "
                          "accepted");
    }
    struct VgiStateRef *presented = NULL;
    const struct ArrowArrayView *override = VgiBatchGet(input, "override");
    int rc = 0;
    if (override && input->array.length && !ArrowArrayViewIsNull(override, 0))
        rc = VgiRpcStateRefDecode(c, ArrowArrayViewGetBytesUnsafe(override, 0), &presented);
    struct ArrowBufferView value = {{NULL}, 0};
    if (!rc) rc = VgiRpcStateRefResolve(c, presented ? presented : s->value, &value);
    char sha256[65];
    if (!rc) hex_digest(value.data.data, value.size_bytes, sha256);
    if (!rc) rc = VgiBatchInit(out, &s->schema, &c->error);
    if (!rc) rc = VgiBatchSetInt(out, "size", value.size_bytes);
    if (!rc) rc = VgiBatchSetString(out, "sha256", sha256);
    if (!rc) rc = VgiBatchSetString(out, "form", VgiStateRefForm(s->value));
    /* Held in memory, never encoded (STATE_REF_V1.md §5.4). */
    if (!rc) rc = VgiBatchSetNull(out, "ref");
    if (!rc) rc = VgiBatchFinishRow(out, &c->error);
    VgiStateRefRelease(presented);
    return rc;
}
int ConformanceStateRefInit(struct VgiRpcCall *c, struct VgiRpcReply *reply) {
    struct StateRefStream *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    ArrowBufferInit(&s->padding);
    struct ArrowBuffer value;
    ArrowBufferInit(&value);
    struct ArrowStringView seed = VgiBatchString(&c->request, "seed");
    int rc = payload(VgiBatchInt(&c->request, "size", 0),
                     (int)VgiBatchInt(&c->request, "compressible", 0), seed, &value);
    /* The init handler is the one place a StateRef may be created. */
    if (!rc)
        rc = VgiRpcStateRef(c, (struct ArrowBufferView){{value.data}, value.size_bytes}, &s->value);
    if (!rc) {
        struct ArrowBuffer padded_seed;
        ArrowBufferInit(&padded_seed);
        rc = ArrowBufferAppend(&padded_seed, seed.data, seed.size_bytes);
        if (!rc) rc = ArrowBufferAppend(&padded_seed, ":padding", 8);
        if (!rc)
            rc = payload(
                VgiBatchInt(&c->request, "cursor_padding", 0), 0,
                (struct ArrowStringView){(const char *)padded_seed.data, padded_seed.size_bytes},
                &s->padding);
        ArrowBufferReset(&padded_seed);
    }
    s->misuse = (int)VgiBatchInt(&c->request, "misuse", 0);
    if (!rc) rc = ConformanceSchemaInit("STATE_REF_OUTPUT_SCHEMA", &s->schema, &c->error);
    if (!rc) rc = ArrowSchemaDeepCopy(&s->schema, &reply->stream.output_schema);
    ArrowBufferReset(&value);
    if (rc) {
        state_ref_release(s);
        return rc;
    }
    reply->stream.is_producer = 0;
    reply->stream.next = state_ref_next;
    reply->stream.release = state_ref_release;
    reply->stream.state = s;
    return 0;
}
static int unary_result(struct VgiRpcCall *c, struct VgiRpcReply *reply) {
    struct ArrowSchema schema = {0};
    int rc = c->method->result(&schema, &c->error);
    if (!rc) rc = VgiBatchInit(&reply->value, &schema, &c->error);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
int ConformanceObjectCacheStats(struct VgiRpcCall *c, struct VgiRpcReply *reply) {
    struct VgiObjectCacheStats stats;
    VgiObjectCacheGetStats(&stats);
    struct ArrowSchema schema = {0};
    struct VgiBatch result = {0};
    int rc = c->method->payload ? c->method->payload(&schema, &c->error) : EINVAL;
    if (!rc) rc = VgiBatchInit(&result, &schema, &c->error);
    const char *names[] = {"resident_bytes", "entries",   "max_bytes", "hits",
                           "misses",         "evictions", "fetches"};
    const int64_t values[] = {stats.resident_bytes, stats.entries,   stats.max_bytes, stats.hits,
                              stats.misses,         stats.evictions, stats.fetches};
    for (int i = 0; !rc && i < 7; ++i) rc = VgiBatchSetInt(&result, names[i], values[i]);
    if (!rc) rc = VgiBatchFinishRow(&result, &c->error);
    if (!rc) rc = VgiRpcReplyPayload(c, reply, &result);
    VgiBatchReset(&result);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
int ConformanceObjectCacheAge(struct VgiRpcCall *c, struct VgiRpcReply *reply) {
    int64_t moved = VgiObjectCacheAge((double)VgiBatchInt(&c->request, "seconds", 0));
    int rc = unary_result(c, reply);
    if (!rc) rc = VgiBatchSetInt(&reply->value, "result", moved);
    return rc ? rc : VgiBatchFinishRow(&reply->value, &c->error);
}
int ConformanceResolveArgumentRef(struct VgiRpcCall *c, struct VgiRpcReply *reply) {
    struct ArrowStringView claimed = VgiBatchString(&c->request, "sha256");
    char sha256[65] = {0};
    if (claimed.size_bytes == 64) memcpy(sha256, claimed.data, 64);
    struct VgiResolvedBytes bytes = {0};
    int rc = VgiRpcResolveArgumentRef(c, VgiBatchBytes(&c->request, "ref"),
                                      VgiBatchInt(&c->request, "size", 0), sha256, &bytes);
    char digest[65];
    if (!rc) hex_digest(bytes.view.data.data, bytes.view.size_bytes, digest);
    VgiResolvedBytesRelease(&bytes);
    if (!rc) rc = unary_result(c, reply);
    if (!rc) rc = VgiBatchSetString(&reply->value, "result", digest);
    return rc ? rc : VgiBatchFinishRow(&reply->value, &c->error);
}
