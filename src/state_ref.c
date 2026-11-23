/* StateRef (in-memory and inline), argument references and their resolution
 * (vgi-rpc STATE_REF_V1.md; single-replica rules of §5.4). */
#define _POSIX_C_SOURCE 200809L
#include "vgi_rpc/state_ref.h"

#include <errno.h>
#include <sodium.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "object_cache_internal.h"
#include "rpc_internal.h"
#include "state_ref_internal.h"
#include "token.h"

#define STATE_REF_FORM_INLINE 0x01
#define STATE_REF_FORM_STORED 0x02
#define STATE_REF_MIN_DECODE_LIMIT (64LL * 1024 * 1024)
#define ARGREF_VERSION 0x01
#define ARGREF_FIXED 43 /* v, size u64, sha256[32], id_len u16 */
#define OBJECT_ID_MAX 1024

struct VgiStateRef {
    _Atomic int64_t refs;
    int decoded; /* 0: created in memory, 1: decoded from the inline form */
    int64_t size;
    uint8_t data[];
};

static int invalid_state_ref(struct VgiRpcCall *call, const char *message) {
    VgiRpcFail(call, "InvalidStateRefError", "INVALID_ARGUMENT", VGI_RPC_KIND_INVALID_STATE_REF,
               message);
    return EINVAL;
}
static struct VgiStateRef *ref_new(struct ArrowBufferView value, int decoded) {
    if (value.size_bytes < 0 || (uint64_t)value.size_bytes > SIZE_MAX - sizeof(struct VgiStateRef))
        return NULL;
    struct VgiStateRef *ref = malloc(sizeof(*ref) + (size_t)value.size_bytes);
    if (!ref) return NULL;
    atomic_init(&ref->refs, 1);
    ref->decoded = decoded;
    ref->size = value.size_bytes;
    if (value.size_bytes) memcpy(ref->data, value.data.data, (size_t)value.size_bytes);
    return ref;
}
int VgiRpcStateRef(struct VgiRpcCall *call, struct ArrowBufferView value,
                   struct VgiStateRef **out) {
    if (!call || !out || *out || value.size_bytes < 0 || (value.size_bytes && !value.data.data))
        return EINVAL;
    if (!call->handling_stream_init) {
        char message[512];
        snprintf(message, sizeof(message),
                 "state_ref() may only be called while handling a stream's init request (%s.%s)",
                 call->protocol ? call->protocol : "",
                 call->method && call->method->name ? call->method->name : "");
        VgiRpcFail(call, "StateRefMisuseError", "FAILED_PRECONDITION",
                   VGI_RPC_KIND_STATE_REF_MISUSE, message);
        return EINVAL;
    }
    /* Single-replica: the value never leaves the process, so it is held as
     * is (§5.2, §5.4). Identical values trivially encode identically, which
     * is all the init-turn memo (§2.2) requires when nothing is stored. */
    *out = ref_new(value, 0);
    return *out ? 0 : ENOMEM;
}
int VgiRpcStateRefResolve(struct VgiRpcCall *call, const struct VgiStateRef *ref,
                          struct ArrowBufferView *out) {
    if (!call || !ref || !out) return EINVAL;
    *out = (struct ArrowBufferView){{ref->data}, ref->size};
    return 0;
}
int VgiStateRefEncode(const struct VgiStateRef *ref, struct ArrowBuffer *out) {
    if (!ref || !out) return EINVAL;
    uint8_t form = STATE_REF_FORM_INLINE;
    int rc = ArrowBufferAppend(out, &form, 1);
    if (!rc) rc = VgiTokenCodecEncode((struct ArrowBufferView){{ref->data}, ref->size}, out);
    return rc;
}
int VgiRpcStateRefDecode(struct VgiRpcCall *call, struct ArrowBufferView encoded,
                         struct VgiStateRef **out) {
    if (!call || !out || *out || encoded.size_bytes < 0) return EINVAL;
    if (encoded.size_bytes < 1) return invalid_state_ref(call, "empty StateRef");
    uint8_t form = encoded.data.as_uint8[0];
    if (form == STATE_REF_FORM_STORED)
        return invalid_state_ref(call,
                                 "stored StateRefs are never minted by this single-replica server");
    if (form != STATE_REF_FORM_INLINE) return invalid_state_ref(call, "unknown StateRef form");
    const struct VgiExternalOptions *external = VgiRpcServerExternal(call->server);
    int64_t limit = STATE_REF_MIN_DECODE_LIMIT;
    if (external && external->max_fetch_bytes > limit) limit = external->max_fetch_bytes;
    struct ArrowBuffer value;
    ArrowBufferInit(&value);
    int rc = VgiTokenCodecDecode(
        (struct ArrowBufferView){{encoded.data.as_uint8 + 1}, encoded.size_bytes - 1}, limit,
        &value);
    if (rc == ENOMEM) return rc;
    if (rc)
        return invalid_state_ref(call, rc == EFBIG ? "StateRef value exceeds the decode bound"
                                                   : "StateRef payload codec is invalid");
    *out = ref_new((struct ArrowBufferView){{value.data}, value.size_bytes}, 1);
    ArrowBufferReset(&value);
    return *out ? 0 : ENOMEM;
}
const char *VgiStateRefForm(const struct VgiStateRef *ref) {
    return ref && ref->decoded ? "inline" : "memory";
}
struct VgiStateRef *VgiStateRefRetain(struct VgiStateRef *ref) {
    if (ref) atomic_fetch_add(&ref->refs, 1);
    return ref;
}
void VgiStateRefRelease(struct VgiStateRef *ref) {
    if (ref && atomic_fetch_sub(&ref->refs, 1) == 1) free(ref);
}

/* ---------------------------------------------------------------------- */
/* Argument references                                                    */
/* ---------------------------------------------------------------------- */

/* ARGREF_AAD: bound to the principal and the server scope, never to a
 * protocol or method (`__upload_url__` lives at the server prefix). The
 * empty protocol is never a routable protocol name. */
static struct VgiTokenContext argref_context(const unsigned char *key, const char *domain,
                                             const char *principal) {
    return (struct VgiTokenContext){key, "vgi_rpc.argref.v1", "", "", domain, principal};
}
static int lower_hex64(const char *text) {
    if (!text || strlen(text) != 64) return 0;
    for (int i = 0; i < 64; ++i)
        if (!((text[i] >= '0' && text[i] <= '9') || (text[i] >= 'a' && text[i] <= 'f'))) return 0;
    return 1;
}
int VgiArgumentRefMint(const unsigned char *key, const char *domain, const char *principal,
                       int64_t size, const char *sha256_hex, const char *object_id,
                       struct ArrowBuffer *out) {
    if (!key || !out || out->size_bytes || size < 1 || !lower_hex64(sha256_hex) || !object_id)
        return EINVAL;
    size_t id_length = strlen(object_id);
    if (!id_length || id_length > OBJECT_ID_MAX) return EINVAL;
    uint8_t plain[ARGREF_FIXED + OBJECT_ID_MAX];
    plain[0] = ARGREF_VERSION;
    for (int i = 0; i < 8; ++i) plain[1 + i] = (uint8_t)((uint64_t)size >> (i * 8));
    if (sodium_hex2bin(plain + 9, 32, sha256_hex, 64, NULL, NULL, NULL)) return EINVAL;
    plain[41] = (uint8_t)(id_length & 0xff);
    plain[42] = (uint8_t)(id_length >> 8);
    memcpy(plain + ARGREF_FIXED, object_id, id_length);
    struct VgiTokenContext context = argref_context(key, domain, principal);
    return VgiTokenSealRaw(
        &context, (struct ArrowBufferView){{plain}, ARGREF_FIXED + (int64_t)id_length}, out);
}

static int arguments_fail(struct VgiRpcCall *call, const char *kind, const char *message) {
    const char *type = "InvalidArgumentsRefError", *code = "INVALID_ARGUMENT";
    if (!strcmp(kind, VGI_RPC_KIND_ARGUMENTS_REF_INTEGRITY))
        type = "ArgumentsRefIntegrityError", code = "DATA_LOSS";
    else if (!strcmp(kind, VGI_RPC_KIND_ARGUMENTS_REF_MISSING))
        type = "ArgumentsRefMissingError", code = "NOT_FOUND";
    else if (!strcmp(kind, VGI_RPC_KIND_STORAGE_UNAVAILABLE))
        type = "StorageUnavailableError", code = "UNAVAILABLE";
    VgiRpcFail(call, type, code, kind, message);
    return EINVAL;
}

struct ArgumentFetch {
    const struct VgiExternalOptions *external;
    const char *object_id;
    int64_t size;
    const uint8_t *sha256;
};
/* Fetch and verify. ENOENT: missing; EBADMSG: integrity; else transient. */
static int fetch_argument(void *opaque, struct VgiObjectBytes **out, struct ArrowError *error) {
    struct ArgumentFetch *f = opaque;
    struct ArrowBuffer body;
    ArrowBufferInit(&body);
    int rc = f->external->get_object(f->external->storage_state, f->object_id,
                                     f->external->max_fetch_bytes, &body, error);
    if (!rc && body.size_bytes != f->size) {
        ArrowErrorSet(error, "argument object size mismatch: expected %lld, got %lld",
                      (long long)f->size, (long long)body.size_bytes);
        rc = EBADMSG;
    }
    if (!rc) {
        uint8_t digest[32];
        crypto_hash_sha256(digest, body.data, (unsigned long long)body.size_bytes);
        if (sodium_memcmp(digest, f->sha256, 32)) {
            ArrowErrorSet(error, "argument object sha256 mismatch");
            rc = EBADMSG;
        }
    }
    if (!rc) {
        *out = VgiObjectBytesNew(body.size_bytes);
        if (!*out)
            rc = ENOMEM;
        else if (body.size_bytes)
            memcpy(VgiObjectBytesData(*out), body.data, (size_t)body.size_bytes);
    }
    ArrowBufferReset(&body);
    return rc;
}
void VgiResolvedBytesRelease(struct VgiResolvedBytes *resolved) {
    if (!resolved) return;
    VgiObjectBytesRelease(resolved->owner);
    memset(resolved, 0, sizeof(*resolved));
}
int VgiRpcResolveArgumentRef(struct VgiRpcCall *call, struct ArrowBufferView ref, int64_t size,
                             const char *sha256_hex, struct VgiResolvedBytes *out) {
    if (!call || !out || out->owner || ref.size_bytes < 0) return EINVAL;
    if (size < 1)
        return arguments_fail(call, VGI_RPC_KIND_INVALID_ARGUMENTS_REF,
                              "argument location size must be an integer >= 1");
    if (!lower_hex64(sha256_hex))
        return arguments_fail(call, VGI_RPC_KIND_INVALID_ARGUMENTS_REF,
                              "argument location sha256 must be 64 lowercase hex characters");
    const struct VgiExternalOptions *external = VgiRpcServerExternal(call->server);
    if (!external || !external->get_object || !call->sealing_key)
        return arguments_fail(
            call, VGI_RPC_KIND_INVALID_ARGUMENTS_REF,
            "this server has no object storage configured to resolve argument references");
    struct VgiTokenContext context =
        argref_context(call->sealing_key, call->auth_domain, call->principal);
    struct ArrowBuffer plain;
    ArrowBufferInit(&plain);
    int rc = VgiTokenOpenRaw(&context, ref, &plain);
    uint64_t id_length = 0;
    if (!rc && (plain.size_bytes < ARGREF_FIXED || plain.data[0] != ARGREF_VERSION)) rc = EINVAL;
    if (!rc) {
        id_length = (uint64_t)plain.data[41] | (uint64_t)plain.data[42] << 8;
        if (!id_length || id_length > OBJECT_ID_MAX ||
            (int64_t)id_length != plain.size_bytes - ARGREF_FIXED ||
            memchr(plain.data + ARGREF_FIXED, 0, (size_t)id_length))
            rc = EINVAL;
    }
    if (rc) {
        ArrowBufferReset(&plain);
        return arguments_fail(call, VGI_RPC_KIND_INVALID_ARGUMENTS_REF,
                              "argument reference is invalid for this caller");
    }
    uint8_t claimed[32];
    sodium_hex2bin(claimed, sizeof(claimed), sha256_hex, 64, NULL, NULL, NULL);
    if (VgiTokenReadU64(plain.data + 1) != (uint64_t)size ||
        sodium_memcmp(plain.data + 9, claimed, 32)) {
        ArrowBufferReset(&plain);
        return arguments_fail(call, VGI_RPC_KIND_INVALID_ARGUMENTS_REF,
                              "argument reference does not match the size and sha256 it was sent "
                              "with");
    }
    if (size > external->max_fetch_bytes) {
        ArrowBufferReset(&plain);
        char message[160];
        snprintf(message, sizeof(message),
                 "argument object is %lld bytes, over this server's max_fetch_bytes (%lld)",
                 (long long)size, (long long)external->max_fetch_bytes);
        return arguments_fail(call, VGI_RPC_KIND_INVALID_ARGUMENTS_REF, message);
    }
    char object_id[OBJECT_ID_MAX + 1];
    memcpy(object_id, plain.data + ARGREF_FIXED, (size_t)id_length);
    object_id[id_length] = 0;
    ArrowBufferReset(&plain);
    struct ArgumentFetch fetch = {external, object_id, size, claimed};
    struct VgiObjectBytes *bytes = NULL;
    struct ArrowError error = {{0}};
    rc = VgiObjectCacheGetOrFetch(object_id, fetch_argument, &fetch, &bytes, NULL, &error);
    if (rc == ENOENT)
        return arguments_fail(call, VGI_RPC_KIND_ARGUMENTS_REF_MISSING,
                              "the uploaded argument object is gone (expired or deleted)");
    if (rc == EBADMSG)
        return arguments_fail(call, VGI_RPC_KIND_ARGUMENTS_REF_INTEGRITY, error.message);
    if (rc == ENOMEM) return rc;
    if (rc) {
        char message[1200];
        snprintf(message, sizeof(message), "object storage get failed: %s",
                 error.message[0] ? error.message : strerror(rc));
        return arguments_fail(call, VGI_RPC_KIND_STORAGE_UNAVAILABLE, message);
    }
    /* A cached value passed these checks for this object id; a different
     * length means the id was reused, which storage never does. */
    if (VgiObjectBytesSize(bytes) != size) {
        VgiObjectBytesRelease(bytes);
        return arguments_fail(call, VGI_RPC_KIND_ARGUMENTS_REF_INTEGRITY,
                              "argument object size mismatch");
    }
    out->owner = bytes;
    out->view = (struct ArrowBufferView){{VgiObjectBytesData(bytes)}, size};
    return 0;
}
int VgiRpcResolveArgumentsLocation(struct VgiRpcCall *call, const struct VgiBatch *batch,
                                   const char *column, struct VgiResolvedBytes *out) {
    if (!call || !batch || !column || !out || out->owner) return EINVAL;
    int64_t index = VgiFieldIndex(&batch->schema, column);
    if (index < 0 || !batch->array.length) return ENOENT;
    const struct ArrowArrayView *location = batch->view.children[index];
    if (ArrowArrayViewIsNull(location, 0)) return ENOENT;
    const struct ArrowSchema *schema = batch->schema.children[index];
    int64_t ref_at = strcmp(schema->format, "+s") ? -1 : VgiFieldIndex(schema, "ref"),
            size_at = ref_at < 0 ? -1 : VgiFieldIndex(schema, "size"),
            sha_at = ref_at < 0 ? -1 : VgiFieldIndex(schema, "sha256");
    if (ref_at < 0 || size_at < 0 || sha_at < 0 ||
        (strcmp(schema->children[ref_at]->format, "z") &&
         strcmp(schema->children[ref_at]->format, "Z")) ||
        strcmp(schema->children[size_at]->format, "l") ||
        (strcmp(schema->children[sha_at]->format, "u") &&
         strcmp(schema->children[sha_at]->format, "U")) ||
        ArrowArrayViewIsNull(location->children[ref_at], 0) ||
        ArrowArrayViewIsNull(location->children[size_at], 0) ||
        ArrowArrayViewIsNull(location->children[sha_at], 0))
        return arguments_fail(call, VGI_RPC_KIND_INVALID_ARGUMENTS_REF,
                              "arguments_location must be struct<ref: binary, size: int64, "
                              "sha256: utf8> with every field set");
    struct ArrowStringView sha = ArrowArrayViewGetStringUnsafe(location->children[sha_at], 0);
    char sha256[65] = {0};
    if (sha.size_bytes == 64) memcpy(sha256, sha.data, 64);
    struct ArrowBufferView ref = ArrowArrayViewGetBytesUnsafe(location->children[ref_at], 0);
    return VgiRpcResolveArgumentRef(
        call, ref, ArrowArrayViewGetIntUnsafe(location->children[size_at], 0), sha256, out);
}
