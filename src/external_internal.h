#ifndef VGI_EXTERNAL_INTERNAL_H
#define VGI_EXTERNAL_INTERNAL_H
#include "vgi_rpc/external.h"
/* Internal HTTP primitive used by the conformance storage adapter. No redirects
 * for mutation requests. Responses and error text never expose signed URLs. */
int VgiExternalHttp(const struct VgiExternalOptions *, const char *method, const char *url,
                    struct ArrowBufferView body, const char *encoding,
                    const struct VgiExternalHeader *headers, size_t header_count,
                    struct ArrowBuffer *out, struct ArrowError *error);
struct VgiRpcCall;
int VgiExternalResolve(struct VgiRpcCall *, struct VgiBatch *, struct ArrowBuffer *);
typedef int (*VgiExternalLogFn)(void *, const char *, const char *, const char *);
/* require_sha256: refuse a pointer without vgi_rpc.location.sha256 (request
 * pointers). The digest, when present, is always verified. */
int VgiExternalResolveOptions(const struct VgiExternalOptions *, int require_sha256,
                              struct VgiBatch *, struct ArrowBuffer *, struct ArrowError *,
                              VgiExternalLogFn, void *);
#endif
