/* Language-local checks for protocol 3.4 large values (vgi-rpc
 * STATE_REF_V1.md): the token codec rule, in-memory StateRefs, argument
 * references, the object cache and the HTTP defaults. */
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <pthread.h>
#include <sodium.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "rpc_internal.h"
#include "state_ref_internal.h"
#include "token.h"
#include "vgi_rpc/http.h"

#define CHECK(condition)                                                    \
    do {                                                                    \
        if (!(condition)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition); \
            abort();                                                        \
        }                                                                   \
    } while (0)

static int kind_is(const struct VgiRpcCall *call, const char *kind) {
    return call->error_kind && !strcmp(call->error_kind, kind);
}
static void reset_error(struct VgiRpcCall *call) {
    free(call->error_storage);
    call->error_storage = NULL;
    call->error_type = call->error_code = call->error_kind = NULL;
    call->error.message[0] = 0;
}

/* The standard codec: raw when compression does not help, zstd otherwise,
 * and every tag this writer never emits is refused (STATE_REF_V1.md §7). */
static void codec_tests(void) {
    struct ArrowBuffer encoded, decoded;
    ArrowBufferInit(&encoded);
    ArrowBufferInit(&decoded);
    uint8_t tiny[3] = {1, 2, 3};
    CHECK(VgiTokenCodecEncode((struct ArrowBufferView){{tiny}, 3}, &encoded) == 0);
    CHECK(encoded.size_bytes == 4 && encoded.data[0] == VGI_TOKEN_CODEC_RAW);
    CHECK(VgiTokenCodecDecode((struct ArrowBufferView){{encoded.data}, encoded.size_bytes}, 64,
                              &decoded) == 0);
    CHECK(decoded.size_bytes == 3 && !memcmp(decoded.data, tiny, 3));
    ArrowBufferReset(&encoded);
    ArrowBufferReset(&decoded);

    uint8_t large[8192];
    memset(large, 'v', sizeof(large));
    CHECK(VgiTokenCodecEncode((struct ArrowBufferView){{large}, sizeof(large)}, &encoded) == 0);
    CHECK(encoded.data[0] == VGI_TOKEN_CODEC_ZSTD && encoded.size_bytes < 1024);
    CHECK(VgiTokenCodecDecode((struct ArrowBufferView){{encoded.data}, encoded.size_bytes},
                              sizeof(large), &decoded) == 0);
    CHECK(decoded.size_bytes == (int64_t)sizeof(large) && !memcmp(decoded.data, large, 8192));
    ArrowBufferReset(&decoded);
    /* The decode bound holds. */
    CHECK(VgiTokenCodecDecode((struct ArrowBufferView){{encoded.data}, encoded.size_bytes}, 100,
                              &decoded) == EFBIG &&
          !decoded.size_bytes);

    /* DEFLATE (reserved, never written here) and unknown tags are refused,
     * even when the body would decode under another codec. */
    uint8_t tags[] = {VGI_TOKEN_CODEC_DEFLATE, 0x03, 0x7f, 0xff};
    for (size_t i = 0; i < sizeof(tags); ++i) {
        encoded.data[0] = tags[i];
        CHECK(VgiTokenCodecDecode((struct ArrowBufferView){{encoded.data}, encoded.size_bytes},
                                  sizeof(large), &decoded) == EINVAL &&
              !decoded.size_bytes);
    }
    CHECK(VgiTokenCodecDecode((struct ArrowBufferView){{encoded.data}, 0}, 64, &decoded) == EINVAL);
    ArrowBufferReset(&encoded);

    /* The same rule on the token open path: a correctly sealed token whose
     * codec byte is unknown is the uniform token failure. */
    unsigned char key[32] = {7};
    struct VgiTokenContext context = {key, "cursor", "Protocol", "method", "d", "alice"};
    for (int tag = 0; tag < 4; ++tag) {
        uint8_t plain[] = {(uint8_t)tag, 'h', 'i'};
        struct ArrowBuffer sealed;
        ArrowBufferInit(&sealed);
        CHECK(VgiTokenSealRaw(&context, (struct ArrowBufferView){{plain}, 3}, &sealed) == 0);
        size_t n = sodium_base64_ENCODED_LEN((size_t)sealed.size_bytes,
                                             sodium_base64_VARIANT_URLSAFE_NO_PADDING);
        char *text = malloc(n);
        CHECK(text);
        sodium_bin2base64(text, n, sealed.data, (size_t)sealed.size_bytes,
                          sodium_base64_VARIANT_URLSAFE_NO_PADDING);
        int rc = VgiTokenOpen(&context, ArrowCharView(text), &decoded);
        if (tag == VGI_TOKEN_CODEC_RAW)
            CHECK(rc == 0 && decoded.size_bytes == 2 && !memcmp(decoded.data, "hi", 2));
        else
            CHECK(rc == EINVAL && !decoded.size_bytes); /* zstd: not a frame; 2, 3: unknown */
        ArrowBufferReset(&decoded);
        ArrowBufferReset(&sealed);
        free(text);
    }
}

static void state_ref_tests(struct VgiRpcServer *server) {
    struct VgiRpcCall call;
    VgiRpcCallInit(&call, server);
    static const struct VgiMethod method = {.name = "state_ref_exchange",
                                            .kind = VGI_METHOD_STREAM};
    call.protocol = "ConformanceService";
    call.method = &method;
    uint8_t value[4096];
    randombytes_buf(value, sizeof(value)); /* incompressible */
    struct ArrowBufferView view = {{value}, sizeof(value)};

    /* Outside an init handler: state_ref_misuse, naming the method. */
    struct VgiStateRef *ref = NULL;
    CHECK(VgiRpcStateRef(&call, view, &ref) == EINVAL && !ref);
    CHECK(kind_is(&call, VGI_RPC_KIND_STATE_REF_MISUSE));
    CHECK(!strcmp(call.error_code, "FAILED_PRECONDITION"));
    CHECK(strstr(call.error.message, "(ConformanceService.state_ref_exchange)"));
    reset_error(&call);

    call.handling_stream_init = 1;
    CHECK(VgiRpcStateRef(&call, view, &ref) == 0 && ref);
    call.handling_stream_init = 0;
    CHECK(!strcmp(VgiStateRefForm(ref), "memory"));
    struct ArrowBufferView resolved;
    CHECK(VgiRpcStateRefResolve(&call, ref, &resolved) == 0);
    CHECK(resolved.size_bytes == (int64_t)sizeof(value) &&
          !memcmp(resolved.data.data, value, sizeof(value)));

    /* Inline form: 0x01 || codec || body, identical for identical values. */
    struct ArrowBuffer a, b;
    ArrowBufferInit(&a);
    ArrowBufferInit(&b);
    CHECK(VgiStateRefEncode(ref, &a) == 0 && VgiStateRefEncode(ref, &b) == 0);
    CHECK(a.size_bytes == b.size_bytes && !memcmp(a.data, b.data, (size_t)a.size_bytes));
    CHECK(a.data[0] == 0x01 && a.data[1] == VGI_TOKEN_CODEC_RAW); /* incompressible stays raw */
    struct VgiStateRef *decoded = NULL;
    CHECK(VgiRpcStateRefDecode(&call, (struct ArrowBufferView){{a.data}, a.size_bytes}, &decoded) ==
          0);
    CHECK(!strcmp(VgiStateRefForm(decoded), "inline"));
    CHECK(VgiRpcStateRefResolve(&call, decoded, &resolved) == 0);
    CHECK(resolved.size_bytes == (int64_t)sizeof(value) &&
          !memcmp(resolved.data.data, value, sizeof(value)));
    VgiStateRefRelease(decoded);
    decoded = NULL;

    /* Stored form (never minted here), unknown forms, bad codecs, empty. */
    uint8_t stored[64] = {0x02, 0x01};
    uint8_t unknown[] = {0x07, 0x00, 'x'};
    uint8_t bad_codec[] = {0x01, 0x7f, 'x'};
    uint8_t bad_zstd[] = {0x01, 0x01, 'n', 'o', 't'};
    struct ArrowBufferView refused[] = {{{stored}, sizeof(stored)},
                                        {{unknown}, sizeof(unknown)},
                                        {{bad_codec}, sizeof(bad_codec)},
                                        {{bad_zstd}, sizeof(bad_zstd)},
                                        {{stored}, 0}};
    for (size_t i = 0; i < sizeof(refused) / sizeof(refused[0]); ++i) {
        CHECK(VgiRpcStateRefDecode(&call, refused[i], &decoded) == EINVAL && !decoded);
        CHECK(kind_is(&call, VGI_RPC_KIND_INVALID_STATE_REF));
        CHECK(!strcmp(call.error_code, "INVALID_ARGUMENT"));
        reset_error(&call);
    }
    ArrowBufferReset(&a);
    ArrowBufferReset(&b);
    VgiStateRefRelease(VgiStateRefRetain(ref));
    VgiStateRefRelease(ref);
    call.protocol = NULL;
    call.method = NULL;
    VgiRpcCallReset(&call);
}

/* An in-process object store standing in for get_object. */
struct Store {
    pthread_mutex_t mutex;
    char ids[8][32];
    struct ArrowBuffer values[8];
    int count;
    atomic_int gets;
    int fail; /* 0: serve; EIO: transient; 1: serve corrupted bytes */
    unsigned delay_ms;
};
static struct Store store = {.mutex = PTHREAD_MUTEX_INITIALIZER};
static const char *store_put(const void *data, int64_t size) {
    pthread_mutex_lock(&store.mutex);
    int i = store.count++;
    snprintf(store.ids[i], sizeof(store.ids[i]), "obj-%d", i);
    ArrowBufferInit(&store.values[i]);
    CHECK(ArrowBufferAppend(&store.values[i], data, size) == 0);
    pthread_mutex_unlock(&store.mutex);
    return store.ids[i];
}
static void store_delete(const char *id) {
    pthread_mutex_lock(&store.mutex);
    for (int i = 0; i < store.count; ++i)
        if (!strcmp(store.ids[i], id)) store.ids[i][0] = 0;
    pthread_mutex_unlock(&store.mutex);
}
static int get_object(void *state, const char *id, int64_t max_bytes, struct ArrowBuffer *out,
                      struct ArrowError *error) {
    (void)state;
    atomic_fetch_add(&store.gets, 1);
    if (store.delay_ms) {
        struct timespec pause = {0, (long)store.delay_ms * 1000000L};
        nanosleep(&pause, NULL);
    }
    if (store.fail == EIO) {
        ArrowErrorSet(error, "storage is down");
        return EIO;
    }
    int rc = ENOENT;
    pthread_mutex_lock(&store.mutex);
    for (int i = 0; i < store.count; ++i)
        if (store.ids[i][0] && !strcmp(store.ids[i], id)) {
            rc = store.values[i].size_bytes > max_bytes
                     ? EFBIG
                     : ArrowBufferAppend(out, store.values[i].data, store.values[i].size_bytes);
            if (!rc && store.fail == 1) out->data[0] ^= 1;
        }
    pthread_mutex_unlock(&store.mutex);
    return rc;
}
static int never_allocate(void *state, int64_t size, const char *sha256,
                          struct VgiExternalUploadUrl *url, struct ArrowError *error) {
    (void)state, (void)size, (void)sha256, (void)url, (void)error;
    return ENOTSUP;
}

static unsigned char server_key[32] = {42};
struct Grant {
    struct ArrowBuffer ref;
    char sha256[65];
    int64_t size;
};
static void grant(const void *data, int64_t size, const char *principal, struct Grant *out) {
    const char *id = store_put(data, size);
    CHECK(VgiExternalSha256Hex(data, (size_t)size, out->sha256) == 0);
    out->size = size;
    ArrowBufferInit(&out->ref);
    CHECK(VgiArgumentRefMint(server_key, "conformance", principal, size, out->sha256, id,
                             &out->ref) == 0);
}
static int resolve(struct VgiRpcServer *server, const char *principal, const struct Grant *g,
                   int64_t size, const char *sha256, const char *expect_kind) {
    struct VgiRpcCall call;
    VgiRpcCallInit(&call, server);
    call.sealing_key = server_key;
    call.auth_domain = "conformance";
    call.principal = principal;
    struct VgiResolvedBytes bytes = {0};
    int rc = VgiRpcResolveArgumentRef(
        &call, (struct ArrowBufferView){{g->ref.data}, g->ref.size_bytes}, size, sha256, &bytes);
    if (expect_kind) {
        CHECK(rc != 0 && !bytes.owner);
        if (!kind_is(&call, expect_kind)) {
            fprintf(stderr, "expected %s, got %s: %s\n", expect_kind,
                    call.error_kind ? call.error_kind : "(none)", call.error.message);
            abort();
        }
    } else {
        CHECK(rc == 0 && bytes.view.size_bytes == g->size);
        char digest[65];
        CHECK(VgiExternalSha256Hex(bytes.view.data.data, (size_t)bytes.view.size_bytes, digest) ==
              0);
        CHECK(!strcmp(digest, g->sha256));
    }
    VgiResolvedBytesRelease(&bytes);
    call.principal = call.auth_domain = NULL;
    VgiRpcCallReset(&call);
    return rc;
}
struct Concurrent {
    struct VgiRpcServer *server;
    const struct Grant *grant;
};
static void *resolve_thread(void *opaque) {
    struct Concurrent *c = opaque;
    resolve(c->server, "alice", c->grant, c->grant->size, c->grant->sha256, NULL);
    return NULL;
}

static void argument_ref_tests(struct VgiRpcServer *server) {
    uint8_t payload[2048];
    for (size_t i = 0; i < sizeof(payload); ++i) payload[i] = (uint8_t)(i ^ 0x5a);
    struct Grant g;
    grant(payload, sizeof(payload), "alice", &g);

    struct VgiObjectCacheStats before, after;
    VgiObjectCacheGetStats(&before);
    resolve(server, "alice", &g, g.size, g.sha256, NULL);
    resolve(server, "alice", &g, g.size, g.sha256, NULL);
    VgiObjectCacheGetStats(&after);
    CHECK(after.fetches - before.fetches == 1 && after.hits - before.hits == 1);

    /* Bound to the principal and to the claimed size/sha256. */
    resolve(server, "bob", &g, g.size, g.sha256, VGI_RPC_KIND_INVALID_ARGUMENTS_REF);
    resolve(server, NULL, &g, g.size, g.sha256, VGI_RPC_KIND_INVALID_ARGUMENTS_REF);
    resolve(server, "alice", &g, g.size + 1, g.sha256, VGI_RPC_KIND_INVALID_ARGUMENTS_REF);
    char zeros[65];
    memset(zeros, '0', 64);
    zeros[64] = 0;
    resolve(server, "alice", &g, g.size, zeros, VGI_RPC_KIND_INVALID_ARGUMENTS_REF);
    resolve(server, "alice", &g, g.size, "not-hex", VGI_RPC_KIND_INVALID_ARGUMENTS_REF);
    g.ref.data[g.ref.size_bytes - 1] ^= 1;
    resolve(server, "alice", &g, g.size, g.sha256, VGI_RPC_KIND_INVALID_ARGUMENTS_REF);
    g.ref.data[g.ref.size_bytes - 1] ^= 1;
    /* A token-shaped envelope of another purpose (a cursor) never opens. */
    struct VgiTokenContext cursor = {server_key, "cursor", "", "", "conformance", "alice"};
    struct Grant forged = g;
    ArrowBufferInit(&forged.ref);
    CHECK(VgiTokenSealRaw(&cursor, (struct ArrowBufferView){{g.ref.data}, 20}, &forged.ref) == 0);
    resolve(server, "alice", &forged, g.size, g.sha256, VGI_RPC_KIND_INVALID_ARGUMENTS_REF);
    ArrowBufferReset(&forged.ref);

    /* Missing, integrity and transient failures map to distinct kinds; none
     * is cached. */
    uint8_t other[100];
    memset(other, 3, sizeof(other));
    struct Grant h;
    grant(other, sizeof(other), "alice", &h);
    store.fail = 1;
    resolve(server, "alice", &h, h.size, h.sha256, VGI_RPC_KIND_ARGUMENTS_REF_INTEGRITY);
    store.fail = EIO;
    resolve(server, "alice", &h, h.size, h.sha256, VGI_RPC_KIND_STORAGE_UNAVAILABLE);
    store.fail = 0;
    resolve(server, "alice", &h, h.size, h.sha256, NULL);
    store_delete(store.ids[store.count - 1]);
    resolve(server, "alice", &h, h.size, h.sha256, NULL); /* still cached */
    CHECK(VgiObjectCacheAge(86401) >= 1);                 /* a day later: a miss */
    resolve(server, "alice", &h, h.size, h.sha256, VGI_RPC_KIND_ARGUMENTS_REF_MISSING);

    /* Single-flight: eight concurrent resolves of a cold object, one get. */
    uint8_t shared[4096];
    memset(shared, 9, sizeof(shared));
    struct Grant s;
    grant(shared, sizeof(shared), "alice", &s);
    int gets = atomic_load(&store.gets);
    store.delay_ms = 50;
    pthread_t threads[8];
    struct Concurrent c = {server, &s};
    for (int i = 0; i < 8; ++i) CHECK(pthread_create(&threads[i], NULL, resolve_thread, &c) == 0);
    for (int i = 0; i < 8; ++i) pthread_join(threads[i], NULL);
    store.delay_ms = 0;
    CHECK(atomic_load(&store.gets) - gets == 1);

    /* The byte bound evicts; an over-bound value is resolved, not retained. */
    VgiObjectCacheSetMaxBytes(3000);
    VgiObjectCacheGetStats(&after);
    CHECK(after.resident_bytes <= 3000);
    struct Grant x, y;
    uint8_t xv[2000], yv[2000], big[5000];
    memset(xv, 1, sizeof(xv));
    memset(yv, 2, sizeof(yv));
    memset(big, 4, sizeof(big));
    grant(xv, sizeof(xv), "alice", &x);
    grant(yv, sizeof(yv), "alice", &y);
    gets = atomic_load(&store.gets);
    resolve(server, "alice", &x, x.size, x.sha256, NULL);
    resolve(server, "alice", &y, y.size, y.sha256, NULL);
    resolve(server, "alice", &x, x.size, x.sha256, NULL);
    CHECK(atomic_load(&store.gets) - gets == 3);
    VgiObjectCacheGetStats(&after);
    CHECK(after.resident_bytes <= 3000 && after.evictions >= 1 && after.max_bytes == 3000);
    struct Grant z;
    grant(big, sizeof(big), "alice", &z);
    resolve(server, "alice", &z, z.size, z.sha256, NULL);
    VgiObjectCacheGetStats(&after);
    CHECK(after.resident_bytes <= 3000);

    ArrowBufferReset(&g.ref);
    ArrowBufferReset(&h.ref);
    ArrowBufferReset(&s.ref);
    ArrowBufferReset(&x.ref);
    ArrowBufferReset(&y.ref);
    ArrowBufferReset(&z.ref);
}

static void defaults_tests(void) {
    struct VgiHttpOptions options;
    VgiHttpOptionsInit(&options);
    CHECK(options.stream_ttl_seconds == 21600); /* 6 h maximum stream duration */
    CHECK(options.stream_idle_seconds == 600);
}

int main(void) {
    CHECK(sodium_init() >= 0);
    codec_tests();
    defaults_tests();
    struct VgiRpcServer *server = VgiRpcServerNew();
    CHECK(server);
    struct VgiExternalOptions external;
    VgiExternalOptionsInit(&external);
    external.allocate = never_allocate;
    external.get_object = get_object;
    external.max_fetch_bytes = 1 << 20;
    CHECK(VgiRpcServerSetExternal(server, &external) == 0);
    state_ref_tests(server);
    argument_ref_tests(server);
    VgiRpcServerFree(server);
    for (int i = 0; i < store.count; ++i) ArrowBufferReset(&store.values[i]);
    return 0;
}
