#ifndef VGI_RPC_VENDOR_H
#define VGI_RPC_VENDOR_H
/* Blob-vendor client: region-pinned, single-object pre-signed URLs.
 *
 * The blob vendor is a small HTTP service that holds the object-storage
 * credentials so workers never do. A server asks it for a pre-signed URL for
 * one object of a declared size and SHA-256 (POST /v1/blobs/put), uploads
 * straight to storage with exactly the grant's required headers, and hands a
 * download URL or the object id to whoever needs the bytes; POST
 * /v1/blobs/get mints a fresh download URL for an object id. Signed URLs are
 * pinned to the hosts and prefixes /v1/info names.
 *
 * A vendor backs every storage role of struct VgiExternalOptions: result
 * externalization (allocate), client upload grants (allocate_client_upload,
 * advisory origin "client_upload"), and the object store contract
 * (put_object with origin "state_ref", get_object).
 *
 * Every vendor request carries "Authorization: Bearer <token>"; signed-URL
 * requests never do. Neither the token nor a signed query string is ever put
 * in an error message.
 *
 * Mirrors vgi-rpc-go's vgirpc/vendor.go and vgi-rpc-python's vgi_rpc.vendor;
 * the wire format is the vendor's (vgi docs/testing/blob-test-catalog.md). */
#include "vgi_rpc/external.h"
#ifdef __cplusplus
extern "C" {
#endif

#define VGI_BLOB_VENDOR_URL_ENV "VGI_BLOB_VENDOR_URL"
#define VGI_BLOB_VENDOR_TOKEN_ENV "VGI_BLOB_VENDOR_TOKEN"
/* Consulted when VGI_BLOB_VENDOR_TOKEN is unset. */
#define VGI_BLOB_VENDOR_TOKEN_FALLBACK_ENV "VGI_CF_DO_TOKEN"
/* Result batches at or above this many bytes are externalized (default 1 MiB). */
#define VGI_RPC_EXTERNALIZE_THRESHOLD_ENV "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES"
/* "none" (default) or "zstd". */
#define VGI_RPC_EXTERNAL_COMPRESSION_ENV "VGI_RPC_EXTERNAL_COMPRESSION"
/* Largest client upload a grant is minted for (VGI-Max-Upload-Bytes); unset
 * means the inbound fetch cap. */
#define VGI_RPC_MAX_UPLOAD_BYTES_ENV "VGI_RPC_MAX_UPLOAD_BYTES"
/* Largest inbound pointer or object fetched. Unset: 256 MiB, raised to
 * VGI_RPC_MAX_UPLOAD_BYTES when that is larger; an upload cap above an
 * explicit fetch cap is a configuration error. */
#define VGI_RPC_MAX_FETCH_BYTES_ENV "VGI_RPC_MAX_FETCH_BYTES"
/* The inline request cap advertised as VGI-Max-Request-Bytes. */
#define VGI_RPC_MAX_REQUEST_BYTES_ENV "VGI_RPC_MAX_REQUEST_BYTES"

struct VgiVendor;
struct VgiVendorOptions {
    unsigned timeout_seconds;  /* vendor JSON calls, total per request; default 30 */
    unsigned attempts;         /* transport errors, 429 and 5xx retried; default 3 */
    unsigned info_ttl_seconds; /* /v1/info host pin cache; default 600 */
};
void VgiVendorOptionsInit(struct VgiVendorOptions *);
/* url must be https (http only for a loopback host); token must be non-empty.
 * options may be NULL for the defaults. */
int VgiVendorNew(const char *url, const char *token, const struct VgiVendorOptions *,
                 struct VgiVendor **out, struct ArrowError *);
void VgiVendorFree(struct VgiVendor *);
/* Install the vendor as every storage role of `external`: allocate,
 * allocate_client_upload, put_object, get_object, validate_url (pinned to the
 * vendor's /v1/info hosts) and both state pointers. Other fields are kept. The
 * vendor must outlive every server using the options. */
void VgiVendorBindExternal(struct VgiVendor *, struct VgiExternalOptions *);
/* Validate one signed URL against the vendor's pinned hosts and prefixes. */
int VgiVendorValidateUrl(struct VgiVendor *, const char *url, struct ArrowError *);
/* A vendor from VGI_BLOB_VENDOR_URL and the token variables; *vendor is NULL
 * (and 0 returned) when the URL is unset. */
int VgiVendorFromEnv(struct VgiVendor **vendor, struct ArrowError *);

/* Raw blobs, for callers that keep their own references (a worker's
 * value-level storage externalization). Fields are 1-128 characters of
 * [A-Za-z0-9._:-]. */
struct VgiVendorScope {
    const char *shard;
    const char *execution_id;
};
/* Store `data` (at least one byte) as one object of purpose "rpc" or "fs":
 * a sized, checksummed grant and a PUT with exactly its required headers, so
 * storage verifies the body. scope and idempotency_key are optional; a
 * repeated key with the same bytes returns the same object. *object_id is
 * malloc-owned. */
int VgiVendorPutBlob(struct VgiVendor *, struct ArrowBufferView data, const char *purpose,
                     const struct VgiVendorScope *scope, const char *idempotency_key,
                     char **object_id, struct ArrowError *);
/* Fetch an object by id through a fresh, pinned download URL, capped at
 * max_bytes (<= 0: 256 MiB; EFBIG above it). ENOENT when the vendor
 * (not_found, not_uploaded) or storage (404) reports it gone. The caller
 * verifies size and SHA-256 against its own reference. */
int VgiVendorGetBlob(struct VgiVendor *, const char *object_id, int64_t max_bytes,
                     struct ArrowBuffer *out, struct ArrowError *);
/* Best-effort clean-up: /v1/blobs/delete (batched by 1,000) and
 * /v1/blobs/delete_scope. deleted may be NULL. */
int VgiVendorDeleteBlobs(struct VgiVendor *, const char *const *object_ids, size_t count,
                         int64_t *deleted, struct ArrowError *);
int VgiVendorDeleteScope(struct VgiVendor *, const struct VgiVendorScope *, int64_t *deleted,
                         struct ArrowError *);

/* Direct object storage contract v1 §1: VGI_STORAGE_BACKEND selects vendor,
 * s3, gcs or none; unset, VGI_BLOB_VENDOR_URL selects the vendor, else
 * VGI_STORAGE_S3_BUCKET s3, else VGI_STORAGE_GCS_BUCKET gcs, else none, and
 * more than one of the three without VGI_STORAGE_BACKEND is refused. The C SDK
 * is vendor-only in protocol 3.4, so s3 and gcs (explicit or implied) are
 * refused with ENOTSUP and a message saying so; a worker refuses to start. */
#define VGI_STORAGE_BACKEND_ENV "VGI_STORAGE_BACKEND"
#define VGI_STORAGE_S3_BUCKET_ENV "VGI_STORAGE_S3_BUCKET"
#define VGI_STORAGE_GCS_BUCKET_ENV "VGI_STORAGE_GCS_BUCKET"
enum VgiStorageBackend { VGI_STORAGE_BACKEND_NONE, VGI_STORAGE_BACKEND_VENDOR };
int VgiStorageBackendFromEnv(enum VgiStorageBackend *, struct ArrowError *);

/* External options from the environment. With VGI_BLOB_VENDOR_URL unset (or
 * VGI_STORAGE_BACKEND=none), and for VgiVendorFromEnv likewise,
 * *vendor is NULL, *external is untouched and 0 is returned. Otherwise
 * *external is initialized, bound to a new vendor, and given
 * VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES, VGI_RPC_EXTERNAL_COMPRESSION and the
 * fetch/upload caps above. A malformed or contradictory value is EINVAL.
 * The caller frees *vendor after serving. */
int VgiExternalOptionsFromEnv(struct VgiExternalOptions *external, struct VgiVendor **vendor,
                              struct ArrowError *);

struct VgiHttpOptions;
struct VgiRpcServer;
/* Apply the environment to an HTTP server before VgiRpcServerServeHttp, as
 * vgi-rpc-go's ConfigureHTTPServerFromEnv does: VGI_RPC_MAX_REQUEST_BYTES sets
 * options->max_request_bytes when set, and with VGI_BLOB_VENDOR_URL results
 * are externalized through the vendor, client upload URLs and argument
 * references are granted from it, and VGI-Max-Upload-Bytes is advertised
 * (VgiRpcServerSetExternal). *vendor is NULL without a vendor; otherwise the
 * caller frees it after serving. */
int VgiHttpConfigureFromEnv(struct VgiRpcServer *, struct VgiHttpOptions *,
                            struct VgiVendor **vendor, struct ArrowError *);

#ifdef __cplusplus
}
#endif
#endif
