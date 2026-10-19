#ifndef VGI_IDENTITY_H
#define VGI_IDENTITY_H
#include "vgi_rpc/credentials.h"
#include "vgi_rpc/rpc.h"
#ifdef __cplusplus
extern "C" {
#endif
struct VgiTokenIdentity {
    const char *principal, *token_name;
    int64_t ttl_seconds;
};
struct VgiIssuedGrant {
    const char *token, *grant_id;
    double expires_at;
};
/* Hook output strings are borrowed until the hook's release callback runs.
 * Hooks may call VgiRpcFail/SetErrorDetails for structured errors. ENOENT means
 * unresolved/refused; EAGAIN means identity_unavailable with a retry hint. */
struct VgiIdentityOptions {
    int (*resolve_token)(void *, struct VgiRpcCall *, const char *, struct VgiTokenIdentity *);
    int (*mint_grant)(void *, struct VgiRpcCall *, const char *purpose, const char *const *scopes,
                      size_t scope_count, int64_t ttl, struct VgiIssuedGrant *);
    void (*release_result)(void *, void *, int is_grant);
    void *state;
    const char *const *introspect_principals;
    size_t introspect_principal_count;
    double max_auth_age;                   /* Zero uses 900 seconds. */
    const struct VgiGrantKeys *grant_keys; /* Implicit minter if hook absent. */
};
/* Register before serving. Options and everything they reference must outlive
 * the server. Only configured methods appear in reflection and protocol hash. */
int VgiRpcServerSetIdentity(struct VgiRpcServer *, const struct VgiIdentityOptions *,
                            struct ArrowError *);
#ifdef __cplusplus
}
#endif
#endif
