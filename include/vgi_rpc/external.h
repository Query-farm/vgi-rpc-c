#ifndef VGI_RPC_EXTERNAL_H
#define VGI_RPC_EXTERNAL_H
#include "vgi_rpc/batch.h"
#ifdef __cplusplus
extern "C" {
#endif

struct VgiExternalUploadUrl {
    char *upload_url;
    char *download_url;
    int64_t expires_at_us;
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
    /* Allocate a method-bound URL pair. Return malloc-owned strings, released
     * by VgiExternalUploadUrlReset, including on failure. */
    int (*allocate)(void *, const struct ArrowSchema *, struct VgiExternalUploadUrl *,
                    struct ArrowError *);
    void *storage_state;
};
struct VgiExternalRef {
    char *location;
    char sha256[65];
};
void VgiExternalOptionsInit(struct VgiExternalOptions *);
void VgiExternalUploadUrlReset(struct VgiExternalUploadUrl *);
void VgiExternalRefReset(struct VgiExternalRef *);
/* Fetch under independent encoded/decoded caps; digest covers decoded bytes.
 * out must be empty and initialized. Failed fetches leave it empty. */
int VgiExternalFetch(const struct VgiExternalOptions *, const char *location, const char *sha256,
                     struct ArrowBuffer *out, struct ArrowError *);
/* Publish one IPC stream once, then reuse its ref without repeating uploads.
 * Schema and bytes are borrowed. out must be zero-initialized. */
int VgiExternalPublish(const struct VgiExternalOptions *, const struct ArrowSchema *,
                       struct ArrowBufferView ipc, int include_sha256, struct VgiExternalRef *,
                       struct ArrowError *);
int VgiExternalPointer(const struct ArrowSchema *, const struct VgiExternalRef *, struct VgiBatch *,
                       struct ArrowBuffer *metadata, struct ArrowError *);
#ifdef __cplusplus
}
#endif
#endif
