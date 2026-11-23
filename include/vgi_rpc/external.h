#ifndef VGI_RPC_EXTERNAL_H
#define VGI_RPC_EXTERNAL_H
#include "vgi_rpc/batch.h"
#ifdef __cplusplus
extern "C" {
#endif

/* One header the uploader must send on the PUT, verbatim. */
struct VgiExternalHeader {
    char *name;
    char *value;
};
/* A pre-signed URL pair for one upload of exactly `size` bytes whose SHA-256
 * was given when it was minted. `headers` never contains Content-Length; the
 * PUT sends exactly that many bytes with a plain Content-Length. */
struct VgiExternalUploadUrl {
    char *upload_url;
    char *download_url;
    int64_t expires_at_us;
    struct VgiExternalHeader *headers; /* malloc-owned array of malloc-owned strings */
    size_t header_count;
    /* Storage id of the object the PUT creates (S3/GCS: the key; vendor: its
     * object_id; fake storage: the blob id), at most 1024 bytes of UTF-8.
     * Required for `__upload_url__` grants (protocol 3.4), which return it
     * with a sealed argument reference; optional for the runtime's own
     * response externalization. malloc-owned. */
    char *object_id;
};
struct VgiExternalOptions {
    int64_t threshold_bytes;
    int64_t max_fetch_bytes;
    int64_t max_decompressed_bytes;
    unsigned max_redirects;
    unsigned timeout_seconds;
    const char *compression; /* NULL, identity, or zstd. */
    /* Zero accepts. Called before every fetch, including each redirect hop.
     * The default permits HTTPS only. Both HTTP and HTTPS need explicit opt-in
     * through a caller-supplied validator for local/private storage. */
    int (*validate_url)(void *, const char *url);
    void *validate_state;
    /* Mint a URL pair for one upload of exactly `size` bytes (>= 1) whose
     * SHA-256 is `sha256_hex` (64 lowercase hex characters). Backends that can
     * sign the length and checksum must do so and return the signed headers in
     * the URL's `headers`. Return malloc-owned strings, released by
     * VgiExternalUploadUrlReset, including on failure. */
    int (*allocate)(void *, int64_t size, const char *sha256_hex, struct VgiExternalUploadUrl *,
                    struct ArrowError *);
    /* The object store contract (vgi-rpc STATE_REF_V1.md §10.1). Both are
     * optional; with get_object unset, argument references cannot be
     * resolved (invalid_arguments_ref).
     *
     * put_object stores `data` as one rpc-purpose object, sending its size and
     * SHA-256 so storage verifies the body, and returns the backend's
     * malloc-owned object id (at most 1024 bytes). Objects carry no TTL:
     * operators must set a bucket lifecycle rule of at least one day on the
     * rpc prefix.
     *
     * get_object mints the download server-side from the id, applies the same
     * URL validation as every other storage fetch, and stops at `max_bytes`
     * (EFBIG). It returns ENOENT, distinguishable from every transient
     * failure, when storage reports the object absent (404 / not_found).
     * Callers verify size and SHA-256 themselves. out is empty on entry and
     * left empty on failure. */
    int (*put_object)(void *, struct ArrowBufferView data, char **object_id, struct ArrowError *);
    int (*get_object)(void *, const char *object_id, int64_t max_bytes, struct ArrowBuffer *out,
                      struct ArrowError *);
    void *storage_state;
};
struct VgiExternalRef {
    char *location;
    char sha256[65];
};
void VgiExternalOptionsInit(struct VgiExternalOptions *);
void VgiExternalUploadUrlReset(struct VgiExternalUploadUrl *);
/* Append one header (both strings copied) to an upload URL's header list. */
int VgiExternalUploadUrlAddHeader(struct VgiExternalUploadUrl *, const char *name,
                                  const char *value);
/* Lowercase hex SHA-256 of `size` bytes into out (65 bytes including NUL). */
int VgiExternalSha256Hex(const void *data, size_t size, char out[65]);
void VgiExternalRefReset(struct VgiExternalRef *);
/* Fetch under independent encoded/decoded caps; digest covers decoded bytes.
 * out must be empty and initialized. Failed fetches leave it empty. */
int VgiExternalFetch(const struct VgiExternalOptions *, const char *location, const char *sha256,
                     struct ArrowBuffer *out, struct ArrowError *);
/* Publish one IPC stream once, then reuse its ref without repeating uploads.
 * The encoded body's size and SHA-256 are passed to `allocate`; the ref's
 * digest covers the decoded IPC bytes. Schema and bytes are borrowed. out must
 * be zero-initialized. */
int VgiExternalPublish(const struct VgiExternalOptions *, const struct ArrowSchema *,
                       struct ArrowBufferView ipc, int include_sha256, struct VgiExternalRef *,
                       struct ArrowError *);
int VgiExternalPointer(const struct ArrowSchema *, const struct VgiExternalRef *, struct VgiBatch *,
                       struct ArrowBuffer *metadata, struct ArrowError *);
#ifdef __cplusplus
}
#endif
#endif
