#ifndef VGI_EXTERNAL_INTERNAL_H
#define VGI_EXTERNAL_INTERNAL_H
#include "vgi_rpc/external.h"
/* Internal HTTP primitive used by the conformance storage adapter. No redirects
 * for mutation requests. Responses and error text never expose signed URLs. */
int VgiExternalHttp(const struct VgiExternalOptions *, const char *method, const char *url,
                    struct ArrowBufferView body, const char *encoding, struct ArrowBuffer *out,
                    struct ArrowError *error);
struct VgiRpcCall;
int VgiExternalResolve(struct VgiRpcCall *, struct VgiBatch *, struct ArrowBuffer *);
#endif
