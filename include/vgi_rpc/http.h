#ifndef VGI_HTTP_H
#define VGI_HTTP_H
#include "vgi_rpc/identity.h"
#include "vgi_rpc/proof.h"
#include "vgi_rpc/rpc.h"
#ifdef __cplusplus
extern "C" {
#endif
struct VgiHttpIdentity {
    const char *principal;
    const char *domain;
    const char *claims_json;
    const char *failure_reason; /* Closed unauthorized reason code, used on rejection. */
    double auth_time;
    int has_auth_time;
};
struct VgiHttpRequest {
    const char *authorization;
    const char *(*header)(void *, const char *);
    void *state;
};
struct VgiHttpOptions {
    const char *host;
    unsigned port;
    const char *prefix;
    /* Comma-separated token=principal pairs; optional tokens permit anonymous
     * requests without credentials. Supplied invalid credentials are refused. */
    const char *bearer_tokens;
    const char *optional_bearer_tokens;
    const char *signing_key;
    const char *cors_origin;
    const char *access_log; /* JSON lines, no credentials or argument values. */
    const struct VgiIdentityOptions *identity;
    const struct VgiGrantKeys *grant_keys;
    struct VgiProofOptions proof;
    int sticky_enabled;
    unsigned sticky_ttl_seconds;
    size_t max_sessions;
    const char *token_key; /* 64 hexadecimal digits; default is a random process key. */
    const char *const *sticky_echo_names;
    const char *const *sticky_echo_values;
    size_t sticky_echo_count;
    /* Optional authenticated control route. ENOENT delegates to RPC routing;
     * zero handles the request with HTTP 204. path excludes the mount prefix. */
    int (*control)(void *, const char *path, int is_delete, struct VgiRpcCall *);
    void *control_state;
    int compression;
    int64_t max_request_bytes;
    int64_t max_response_bytes;
    int64_t max_externalized_response_bytes;
    size_t max_streams;
    /* Maximum stream duration, default 21600 (6 h, vgi-rpc STATE_REF_V1.md
     * §12): call tokens and cursors older than this are refused, and an
     * in-process stream is dropped once this old. */
    unsigned stream_ttl_seconds;
    /* In-process streams with no turn for this long are dropped; default 600.
     * Zero uses stream_ttl_seconds. */
    unsigned stream_idle_seconds;
    /* Reject stream initializers without serialization/resume support. Streams
     * that support it are always stateless: no process-local call cache is used. */
    int require_stateless_streams;
    unsigned max_concurrent_requests;
    /* Zero accepts. EACCES rejects, EAGAIN signals temporary unavailability.
     * Identity strings are copied before the next request. */
    int (*authenticate)(void *, const char *authorization, struct VgiHttpIdentity *,
                        struct ArrowError *);
    int (*authenticate_request)(void *, const struct VgiHttpRequest *, struct VgiHttpIdentity *,
                                struct ArrowError *);
    void *auth_state;
    /* Optional; called once the runtime has copied an authenticate or
     * authenticate_request identity, accepted or not. */
    void (*release_identity)(void *, struct VgiHttpIdentity *);
    /* Optional, non-blocking; runs on the event loop as soon as a request's
     * head arrives, before any of its body is read. EACCES or ESTALE predicts
     * that authentication will refuse the request, whose body is then never
     * read; zero lets the request continue. Only used with
     * require_authentication. */
    int (*authenticate_early)(void *, const char *authorization);
    /* Every request except HEAD /health and CORS preflights (OPTIONS with
     * Access-Control-Request-Method) must authenticate, including GET /health,
     * other OPTIONS requests and unknown paths. Authentication then precedes
     * reading, size-checking and decoding the body: a declared body larger
     * than max_request_bytes is answered only after authentication succeeds.
     * Bodies the runtime does not read close the connection. */
    int require_authentication;
    /* WWW-Authenticate on 401 responses; NULL means Bearer realm="vgi". */
    const char *auth_challenge;
};
void VgiHttpOptionsInit(struct VgiHttpOptions *);
/* Serves until SIGINT/SIGTERM. Prints PORT:<port> after binding (port 0 selects
 * an available port). HTTP bodies, including stream turns, use nanoarrow IPC. */
int VgiRpcServerServeHttp(struct VgiRpcServer *, const struct VgiHttpOptions *,
                          struct ArrowError *);
#ifdef __cplusplus
}
#endif
#endif
