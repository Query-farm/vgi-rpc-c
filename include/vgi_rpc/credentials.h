#ifndef VGI_RPC_CREDENTIALS_H
#define VGI_RPC_CREDENTIALS_H
#include "nanoarrow/nanoarrow.h"
#ifdef __cplusplus
extern "C" {
#endif
/* All outputs own their storage. Initialize claims to zero and ArrowBuffers
 * with ArrowBufferInit; reset them after use. Tokens include a terminal NUL
 * outside size_bytes. Open returns EINVAL for invalid credentials, ESTALE for
 * authentic expired/not-yet-valid credentials, or ENOMEM. */
struct VgiGrant {
    int64_t issued_at, expires_at;
    char *grant_id, *principal, *purpose;
    char **scopes;
    size_t scope_count;
};
struct VgiGrantKeys {
    const unsigned char (*keys)[32];
    size_t key_count;
    const char *audience;
    int64_t max_ttl_seconds, clock_skew_seconds;
};
void VgiSigningKey(const void *raw, size_t size, unsigned char key[32]);
void VgiGrantReset(struct VgiGrant *);
int VgiGrantKeysValidate(const struct VgiGrantKeys *);
/* nonce must be NULL in production; a fixed nonce is for test vectors only. */
int VgiGrantMint(const struct VgiGrantKeys *, const struct VgiGrant *,
                 const unsigned char nonce[24], struct ArrowBuffer *token);
int VgiGrantOpen(const struct VgiGrantKeys *, const char *token, double now, struct VgiGrant *);
#ifdef __cplusplus
}
#endif
#endif
