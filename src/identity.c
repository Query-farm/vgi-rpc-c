#include "vgi_rpc/identity.h"

#include <errno.h>
#include <math.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "rpc_internal.h"
struct Identity {
    struct VgiIdentityOptions options;
    struct VgiMethod methods[2];
};
static char *copy_view(struct ArrowStringView s) {
    if (s.size_bytes < 0 || memchr(s.data, 0, (size_t)s.size_bytes)) return NULL;
    char *p = malloc((size_t)s.size_bytes + 1);
    if (p) {
        memcpy(p, s.data, (size_t)s.size_bytes);
        p[s.size_bytes] = 0;
    }
    return p;
}
static int fail(struct VgiRpcCall *c, const char *type, const char *kind, const char *message) {
    const char *code = !strcmp(kind, "token_unresolved") ? "NOT_FOUND"
                       : !strcmp(kind, "stale_auth")     ? "UNAUTHENTICATED"
                                                         : "PERMISSION_DENIED";
    return VgiRpcFail(c, type, code, kind, message);
}
static int unavailable(struct VgiRpcCall *c) {
    if (c->error_type) return EINVAL;
    int rc = VgiRpcFail(c, "IdentityUnavailableError", "UNAVAILABLE", "identity_unavailable",
                        "identity lookup unavailable");
    VgiRpcSetErrorDetails(c, "[{\"@type\":\"vgi_rpc.RetryInfo\",\"retry_delay_seconds\":5.0}]");
    return rc;
}
static size_t whitespace(const unsigned char *p, size_t n) {
    if (!n) return 0;
    if (*p == ' ' || (*p >= 9 && *p <= 13)) return 1;
    if (n >= 2 && p[0] == 0xc2 && (p[1] == 0x85 || p[1] == 0xa0)) return 2;
    return 0;
}
int VgiIdentitySubjectOk(const char *token) {
    size_t n = strlen(token);
    if (n > 4096) return 0;
    const unsigned char *p = (const unsigned char *)token;
    size_t w;
    while ((w = whitespace(p, n))) {
        p += w;
        n -= w;
    }
    while (n) {
        w = n >= 2 && whitespace(p + n - 2, 2) == 2 ? 2 : whitespace(p + n - 1, 1);
        if (!w) break;
        n -= w;
    }
    if (!n) return 0;
    size_t i = 0;
    for (int part = 0; part < 3; ++part) {
        size_t start = i;
        while (i < n && ((p[i] >= 'a' && p[i] <= 'z') || (p[i] >= 'A' && p[i] <= 'Z') ||
                         (p[i] >= '0' && p[i] <= '9') || p[i] == '_' || p[i] == '-'))
            ++i;
        if (part < 2) {
            if (i == start || i == n || p[i++] != '.') return 1;
        } else
            return i != n;
    }
    return 1;
}
static int introspect(struct Identity *s, struct VgiRpcCall *c, struct VgiRpcReply *reply) {
    int allowed = 0;
    for (size_t i = 0; c->principal && *c->principal && i < s->options.introspect_principal_count;
         ++i)
        if (!strcmp(c->principal, s->options.introspect_principals[i])) allowed = 1;
    if (!allowed)
        return fail(c, "IntrospectionRefusedError", "introspection_refused",
                    "caller is not an introspector");
    char *token = copy_view(VgiBatchString(&c->request, "token"));
    if (!token) return fail(c, "TokenUnresolvedError", "token_unresolved", "unresolved");
    if (!VgiIdentitySubjectOk(token)) {
        free(token);
        return fail(c, "TokenUnresolvedError", "token_unresolved", "unresolved");
    }
    struct VgiTokenIdentity result = {.ttl_seconds = 300};
    int rc = s->options.resolve_token(s->options.state, c, token, &result);
    sodium_memzero(token, strlen(token));
    free(token);
    if (rc == EAGAIN)
        rc = unavailable(c);
    else if (rc && !c->error_type)
        rc = fail(c, "TokenUnresolvedError", "token_unresolved", "unresolved");
    struct VgiBatch payload = {0};
    if (!rc && (!result.principal || !*result.principal))
        rc = fail(c, "TokenUnresolvedError", "token_unresolved", "unresolved");
    if (!rc) rc = VgiRpcBatchInitNamed(&payload, "IdentityIntrospectTokenResult", &c->error);
    if (!rc) rc = VgiBatchSetString(&payload, "principal", result.principal);
    if (!rc)
        rc = VgiBatchSetString(&payload, "token_name", result.token_name ? result.token_name : "");
    if (!rc) rc = VgiBatchSetInt(&payload, "ttl_seconds", result.ttl_seconds);
    if (!rc) rc = VgiBatchFinishRow(&payload, &c->error);
    if (!rc) rc = VgiRpcReplyPayload(c, reply, &payload);
    VgiBatchReset(&payload);
    if (s->options.release_result) s->options.release_result(s->options.state, &result, 0);
    return rc;
}
static int issue(struct Identity *s, struct VgiRpcCall *c, struct VgiRpcReply *reply) {
    double age = s->options.max_auth_age ? s->options.max_auth_age : 900;
    if (!c->principal || !*c->principal || !c->has_auth_time || !isfinite(c->auth_time) ||
        (c->auth_domain && !strcmp(c->auth_domain, "grant")) ||
        difftime(time(NULL), 0) - c->auth_time > age)
        return fail(c, "StaleAuthError", "stale_auth",
                    "a recently authenticated user is required to mint a grant");
    char *purpose = copy_view(VgiBatchString(&c->request, "purpose"));
    if (!purpose) return EINVAL;
    const struct ArrowArrayView *list = VgiBatchGet(&c->request, "scopes");
    int64_t start = ArrowArrayViewListChildOffset(list, 0),
            end = ArrowArrayViewListChildOffset(list, 1);
    size_t count = (size_t)(end - start);
    char **scopes = calloc(count + 1, sizeof(char *));
    int rc = scopes ? 0 : ENOMEM;
    for (size_t i = 0; !rc && i < count; ++i) {
        if (ArrowArrayViewIsNull(list->children[0], start + (int64_t)i)) {
            rc = EINVAL;
            break;
        }
        scopes[i] = copy_view(VgiValueString(list->children[0], start + (int64_t)i));
        if (!scopes[i]) rc = ENOMEM;
    }
    int64_t ttl = VgiBatchInt(&c->request, "ttl_seconds", 0);
    struct VgiIssuedGrant result = {0};
    struct ArrowBuffer token;
    ArrowBufferInit(&token);
    char id[33];
    int hooked = 0;
    if (!rc && s->options.mint_grant) {
        hooked = 1;
        rc = s->options.mint_grant(s->options.state, c, purpose, (const char *const *)scopes, count,
                                   ttl, &result);
    } else if (!rc) {
        const struct VgiGrantKeys *k = s->options.grant_keys;
        if (ttl <= 0)
            rc = EINVAL;
        else {
            if (ttl > k->max_ttl_seconds) ttl = k->max_ttl_seconds;
            int64_t now = (int64_t)time(NULL);
            if (now > INT64_MAX - ttl)
                rc = EINVAL;
            else {
                unsigned char raw[16];
                randombytes_buf(raw, 16);
                sodium_bin2hex(id, sizeof(id), raw, 16);
                struct VgiGrant claims = {.issued_at = now,
                                          .expires_at = now + ttl,
                                          .grant_id = id,
                                          .principal = (char *)c->principal,
                                          .purpose = purpose,
                                          .scopes = scopes,
                                          .scope_count = count};
                rc = VgiGrantMint(k, &claims, NULL, &token);
                result.token = (char *)token.data;
                result.grant_id = id;
                result.expires_at = (double)claims.expires_at;
            }
        }
    }
    if (rc == EAGAIN)
        rc = unavailable(c);
    else if (rc && !c->error_type)
        rc = fail(c, "GrantRefusedError", "grant_refused", "grant request refused");
    struct VgiBatch payload = {0};
    if (!rc && (!result.token || !isfinite(result.expires_at))) rc = EINVAL;
    if (!rc) rc = VgiRpcBatchInitNamed(&payload, "IdentityIssueGrantResult", &c->error);
    if (!rc) rc = VgiBatchSetString(&payload, "token", result.token);
    if (!rc) rc = VgiBatchSetString(&payload, "grant_id", result.grant_id ? result.grant_id : "");
    if (!rc) rc = ArrowArrayAppendDouble(VgiBatchColumn(&payload, "expires_at"), result.expires_at);
    if (!rc) rc = VgiBatchFinishRow(&payload, &c->error);
    if (!rc) rc = VgiRpcReplyPayload(c, reply, &payload);
    VgiBatchReset(&payload);
    if (hooked && s->options.release_result)
        s->options.release_result(s->options.state, &result, 1);
    if (token.data) sodium_memzero(token.data, (size_t)token.size_bytes);
    ArrowBufferReset(&token);
    for (size_t i = 0; i < count; ++i) free(scopes ? scopes[i] : NULL);
    free(scopes);
    free(purpose);
    return rc;
}
static int dispatch(void *state, struct VgiRpcCall *c, struct VgiRpcReply *r) {
    return !strcmp(c->method->name, "introspect_token") ? introspect(state, c, r)
                                                        : issue(state, c, r);
}
int VgiRpcServerSetIdentity(struct VgiRpcServer *server, const struct VgiIdentityOptions *o,
                            struct ArrowError *error) {
    if (!o) return 0;
    int mint = o->mint_grant || o->grant_keys, resolve = o->resolve_token != NULL;
    if (!mint && !resolve) return 0;
    if (!isfinite(o->max_auth_age) || o->max_auth_age < 0) return EINVAL;
    if (o->grant_keys && VgiGrantKeysValidate(o->grant_keys)) return EINVAL;
    int principals = 0;
    if (resolve) {
        for (size_t i = 0; i < o->introspect_principal_count; ++i)
            if (o->introspect_principals && o->introspect_principals[i] &&
                *o->introspect_principals[i])
                principals = 1;
        if (!principals) {
            ArrowErrorSet(error, "Introspection requires an explicit principal allowlist");
            return EINVAL;
        }
    }
    struct Identity *state = calloc(1, sizeof(*state));
    if (!state) return ENOMEM;
    state->options = *o;
    size_t n = 0;
    for (size_t i = 0; i < VGI_IDENTITY_METHOD_COUNT; ++i)
        if ((resolve && !strcmp(VGI_IDENTITY_METHODS[i].name, "introspect_token")) ||
            (mint && !strcmp(VGI_IDENTITY_METHODS[i].name, "issue_grant")))
            state->methods[n++] = VGI_IDENTITY_METHODS[i];
    struct VgiRpcProtocol protocol = {
        VGI_IDENTITY_NAME,
        VGI_IDENTITY_VERSION,
        resolve ? (mint ? VGI_IDENTITY_HASH_INTROSPECT_ISSUE : VGI_IDENTITY_HASH_INTROSPECT)
                : VGI_IDENTITY_HASH_ISSUE,
        state->methods,
        n,
        dispatch,
        state,
        NULL};
    int rc = VgiRpcServerAddManagedProtocol(server, &protocol, free, error);
    if (rc) free(state);
    return rc;
}
