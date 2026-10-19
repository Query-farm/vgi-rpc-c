#ifndef VGI_TOKEN_H
#define VGI_TOKEN_H
#include "nanoarrow/nanoarrow.h"
#define VGI_TOKEN_MAX_BYTES (32 * 1024 * 1024)
/* Tokens are opaque, bounded, independently sealed by purpose/route/identity.
 * Returned text is malloc-owned. Open outputs are empty on failure. */
struct VgiTokenContext {
    const unsigned char *key;
    const char *purpose, *protocol, *method, *domain, *principal;
};
int VgiTokenSeal(const struct VgiTokenContext *, struct ArrowBufferView, char **);
int VgiTokenOpen(const struct VgiTokenContext *, struct ArrowStringView, struct ArrowBuffer *);
int VgiTokenAppendU64(struct ArrowBuffer *, uint64_t);
uint64_t VgiTokenReadU64(const uint8_t *);
#endif
