#ifndef VGI_RPC_STATE_REF_H
#define VGI_RPC_STATE_REF_H
/* Stream-state references, argument references and the object cache
 * (vgi-rpc protocol 3.4, tools/cross-port/specs/STATE_REF_V1.md).
 *
 * This port keeps HTTP stream state in process (STATE_REF_V1.md §5.4): a
 * StateRef is held in memory and never stored, sealed per turn or fetched, on
 * every transport. Worker code is still portable: create refs with
 * VgiRpcStateRef while handling a stream's init call and resolve them on any
 * turn. Argument references (minted by `__upload_url__` grants) and the
 * object cache are fully implemented. */
#include <stdint.h>

#include "vgi_rpc/batch.h"
#ifdef __cplusplus
extern "C" {
#endif

struct VgiRpcCall;

/* Error kinds raised by this API (vgi_rpc.error_kind on the wire). */
#define VGI_RPC_KIND_INVALID_STATE_REF "invalid_state_ref"
#define VGI_RPC_KIND_STATE_REF_MISUSE "state_ref_misuse"
#define VGI_RPC_KIND_STATE_REF_TOO_LARGE "state_ref_too_large"
#define VGI_RPC_KIND_INVALID_ARGUMENTS_REF "invalid_arguments_ref"
#define VGI_RPC_KIND_ARGUMENTS_REF_INTEGRITY "arguments_ref_integrity"
#define VGI_RPC_KIND_ARGUMENTS_REF_MISSING "arguments_ref_missing"
#define VGI_RPC_KIND_STORAGE_UNAVAILABLE "storage_unavailable"

/* ---------------------------------------------------------------------- */
/* StateRef                                                               */
/* ---------------------------------------------------------------------- */

/* An immutable, reference-counted stream-state value. */
struct VgiStateRef;

/* ctx.state_ref(value): wrap bytes the stream will not change. Only valid
 * while the call is handling a stream's init request (the protocol handler
 * that returns the stream); anywhere else, including every next() turn and
 * the first producer turn, it fails with state_ref_misuse
 * (FAILED_PRECONDITION). The bytes are copied. On success *out is owned by
 * the caller (VgiStateRefRelease). */
int VgiRpcStateRef(struct VgiRpcCall *, struct ArrowBufferView value, struct VgiStateRef **out);
/* ctx.resolve(ref): the value's bytes, borrowed for the ref's lifetime. Valid
 * on every turn and every transport. */
int VgiRpcStateRefResolve(struct VgiRpcCall *, const struct VgiStateRef *,
                          struct ArrowBufferView *out);
/* The encoded inline form, 0x01 || codec || body (STATE_REF_V1.md §3.2):
 * zstd when smaller, raw otherwise. Appends to out. */
int VgiStateRefEncode(const struct VgiStateRef *, struct ArrowBuffer *out);
/* Decode an encoded StateRef. The inline form decodes (bounded by 64 MiB or
 * the server's max_fetch_bytes, whichever is larger); a stored-form ref can
 * never have been minted by this server and, like an unknown form byte or a
 * bad codec, fails invalid_state_ref (INVALID_ARGUMENT). */
int VgiRpcStateRefDecode(struct VgiRpcCall *, struct ArrowBufferView encoded,
                         struct VgiStateRef **out);
/* "memory" for a ref created by VgiRpcStateRef, "inline" for a decoded one. */
const char *VgiStateRefForm(const struct VgiStateRef *);
struct VgiStateRef *VgiStateRefRetain(struct VgiStateRef *);
void VgiStateRefRelease(struct VgiStateRef *);

/* ---------------------------------------------------------------------- */
/* Argument references                                                    */
/* ---------------------------------------------------------------------- */

/* Bytes resolved through the object cache. The view stays valid until
 * release, even if the cache evicts the entry meanwhile. */
struct VgiResolvedBytes {
    struct ArrowBufferView view;
    void *owner;
};
void VgiResolvedBytesRelease(struct VgiResolvedBytes *);

/* Resolve an argument reference minted by this server's `__upload_url__`
 * grant: open `ref` under the calling principal (the grant was authenticated
 * exactly like a protocol method), check its sealed size and SHA-256 equal
 * `size` and `sha256_hex`, then the object cache by object id, single-flight
 * get_object on a miss, and verify the bytes. Failures set the call's error:
 * invalid_arguments_ref (INVALID_ARGUMENT), arguments_ref_integrity
 * (DATA_LOSS), arguments_ref_missing (NOT_FOUND) or storage_unavailable
 * (UNAVAILABLE). out must be zeroed. */
int VgiRpcResolveArgumentRef(struct VgiRpcCall *, struct ArrowBufferView ref, int64_t size,
                             const char *sha256_hex, struct VgiResolvedBytes *out);
/* Resolve a `struct<ref: binary, size: int64, sha256: utf8>` column (vgi's
 * BindRequest.arguments_location) at row 0 of `batch`. ENOENT, with no error
 * set, when the column is absent or null, so the caller falls back to its
 * inline value; a malformed struct is invalid_arguments_ref. */
int VgiRpcResolveArgumentsLocation(struct VgiRpcCall *, const struct VgiBatch *batch,
                                   const char *column, struct VgiResolvedBytes *out);

/* ---------------------------------------------------------------------- */
/* The object cache (one per process; nothing about it is on the wire)    */
/* ---------------------------------------------------------------------- */

struct VgiObjectCacheStats {
    int64_t resident_bytes, entries, max_bytes, hits, misses, evictions, fetches;
};
/* Read VGI_RPC_OBJECT_CACHE_BYTES (a non-negative integer; 0 retains
 * nothing), else min(1 GiB, 40% of the cgroup memory limit, else of physical
 * memory). EINVAL for a malformed value. HTTP servers call this at startup;
 * the first use calls it otherwise and falls back to the default. */
int VgiObjectCacheConfigure(struct ArrowError *);
/* Override the byte bound (evicting down to it). */
void VgiObjectCacheSetMaxBytes(int64_t max_bytes);
void VgiObjectCacheGetStats(struct VgiObjectCacheStats *);
/* Test hook: move every entry's fetched_at back by `seconds`; returns how
 * many entries moved. Entries fetched more than one day ago are misses. */
int64_t VgiObjectCacheAge(double seconds);

#ifdef __cplusplus
}
#endif
#endif
