#ifndef VGI_RPC_JWT_H
#define VGI_RPC_JWT_H
#include "vgi_rpc/http.h"
#ifdef __cplusplus
extern "C" {
#endif

/* JWT bearer verification against a JWKS (RFC 7519, RFC 7517, RFC 9068).
 *
 * Tokens are JWS compact serializations signed with an asymmetric algorithm
 * from the allow-list: ES256, ES384, ES512, RS256, RS384, RS512, PS256, PS384
 * or PS512. "none" and HS* are always refused. Signing keys come only from
 * jwks_uri, never from a token header (jku, jwk, x5u and x5c are ignored), and
 * a token with a "crit" header is refused.
 *
 * Every limit is enforced before any allocation proportional to the input:
 * tokens longer than 16 KiB, JSON nested more than 16 deep, and JWKS documents
 * larger than 1 MiB or holding more than 64 usable keys are rejected. */
struct VgiJwtOptions {
    const char *issuer;   /* Required; the "iss" claim must equal it exactly. */
    const char *audience; /* Required; one "aud" value must equal it exactly. */
    const char *jwks_uri; /* Required; an http or https URL. */
    /* Comma-separated allow-list. NULL or empty means "ES256". */
    const char *algorithms;
    /* When set (for example "at+jwt"), the JOSE "typ" header must be present
     * and match, case-insensitively and with "application/" optional. */
    const char *required_typ;
    const char *principal_claim; /* NULL means "sub"; it must be a non-empty string. */
    const char *domain;          /* VgiHttpIdentity.domain; NULL means "jwt". */
    int require_exp;             /* A token without "exp" is refused. */
    int single_audience;         /* "aud" must hold exactly one value. */
    unsigned leeway_seconds;     /* Clock skew allowed on exp, nbf and iat. */
    /* Hard bound on how long fetched keys are used; a shorter Cache-Control
     * max-age on the JWKS response wins. When it passes the keys are dropped,
     * and if the re-fetch fails verification is unavailable (EAGAIN) rather
     * than served from stale keys. Zero or less keeps keys for the process
     * lifetime. */
    double jwks_max_age_seconds;
    /* Minimum time between fetches triggered by an unknown "kid". Within it a
     * token with an unknown kid is refused without a fetch. Zero or less
     * re-fetches on every unknown kid. */
    double jwks_refresh_min_interval_seconds;
    double jwks_fetch_timeout_seconds; /* Zero or less means 10 seconds. */
};
struct VgiJwtVerifier;

void VgiJwtOptionsInit(struct VgiJwtOptions *);
/* Validates and copies the options; no network access happens here. Returns
 * EINVAL, with a message, for unusable options. The verifier is safe to share
 * between threads. */
int VgiJwtVerifierNew(const struct VgiJwtOptions *, struct VgiJwtVerifier **, struct ArrowError *);
void VgiJwtVerifierFree(struct VgiJwtVerifier *);
/* A VgiHttpOptions.authenticate callback; state is the verifier. Returns zero
 * with an identity, EACCES for a missing or invalid token, ESTALE for an
 * authentic but expired one (identity->failure_reason is always set), and
 * EAGAIN when no keys within their maximum age can be obtained. Identity
 * strings belong to the verifier until VgiJwtReleaseIdentity. */
int VgiJwtAuthenticate(void *verifier, const char *authorization, struct VgiHttpIdentity *,
                       struct ArrowError *);
void VgiJwtReleaseIdentity(void *verifier, struct VgiHttpIdentity *);
/* A VgiHttpOptions.authenticate_early callback. Never blocks or fetches: it
 * checks against keys already cached. EACCES or ESTALE means VgiJwtAuthenticate
 * would refuse the same header; zero means it might accept it. */
int VgiJwtPrecheck(void *verifier, const char *authorization);
/* Point options' authenticate, authenticate_early, release_identity and
 * auth_state at the verifier. */
void VgiJwtInstall(struct VgiJwtVerifier *, struct VgiHttpOptions *);

#ifdef __cplusplus
}
#endif
#endif
