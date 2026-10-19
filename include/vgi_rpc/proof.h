#ifndef VGI_RPC_PROOF_H
#define VGI_RPC_PROOF_H
#include "nanoarrow/nanoarrow.h"
#ifdef __cplusplus
extern "C" {
#endif

enum VgiProofMode { VGI_PROOF_OFF, VGI_PROOF_ALLOW, VGI_PROOF_REQUIRE };
struct VgiProofOptions {
    enum VgiProofMode mode;
    const char *origin_id;
    /* Comma-separated kid:hex-secret pairs; each secret is exactly 32 bytes. */
    const char *secrets;
    unsigned skew_seconds;
    /* Zero disables replay detection; otherwise a hard bound on retained nonces. */
    size_t replay_capacity;
};
struct VgiProofResult {
    int verified;
    const char *reason;
    char kid[65];
};
struct VgiProxyProof;
void VgiProofOptionsInit(struct VgiProofOptions *);
/* The configuration is copied. The verifier is safe to share between threads. */
int VgiProxyProofNew(const struct VgiProofOptions *, struct VgiProxyProof **, struct ArrowError *);
void VgiProxyProofFree(struct VgiProxyProof *);
/* Returns EACCES on rejected proofs in REQUIRE mode. Results are diagnostic:
 * never expose their reason or kid in an authentication error response. */
int VgiProxyProofCheck(struct VgiProxyProof *, const char *token, size_t header_count, uint64_t now,
                       struct VgiProofResult *);

#ifdef __cplusplus
}
#endif
#endif
