#include <errno.h>
#include <json-c/json.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "vgi_rpc/http.h"
static struct VgiIdentityOptions identity;
static struct VgiGrantKeys keys;
static unsigned char key_bytes[2][32];
static const char *introspectors[] = {"conformance-introspector"};
static int authenticate(void *state, const struct VgiHttpRequest *request,
                        struct VgiHttpIdentity *out, struct ArrowError *error) {
    (void)state;
    (void)error;
    const char *principal = request->header(request->state, "X-Conformance-Principal");
    out->principal = principal && *principal ? principal : NULL;
    out->domain = out->principal ? "conformance" : NULL;
    const char *time = request->header(request->state, "X-Conformance-Auth-Time");
    if (time) {
        char *end = NULL;
        out->auth_time = strtod(time, &end);
        out->has_auth_time = 1;
        if (end == time || *end) out->auth_time = NAN;
    }
    return out->principal || !request->authorization || !*request->authorization ? 0 : EACCES;
}
int ConformanceRejectAuth(void *state, const struct VgiHttpRequest *request,
                          struct VgiHttpIdentity *out, struct ArrowError *error) {
    (void)state;
    (void)error;
    const char *reason = request->header(request->state, "X-Conformance-Auth-Reason");
    const char *allowed[] = {"missing_credential", "invalid_credential", "expired_credential",
                             "insufficient_scope"};
    out->failure_reason = "unauthorized";
    for (size_t i = 0; reason && i < 4; ++i)
        if (!strcmp(reason, allowed[i])) out->failure_reason = allowed[i];
    return EACCES;
}
static int unavailable(struct VgiRpcCall *call, int retry) {
    int rc = VgiRpcFail(call, "IdentityUnavailableError", "UNAVAILABLE", "identity_unavailable",
                        "conformance: authority unavailable");
    char details[128];
    snprintf(details, sizeof(details),
             "[{\"@type\":\"vgi_rpc.RetryInfo\",\"retry_delay_seconds\":%d}]", retry);
    VgiRpcSetErrorDetails(call, details);
    return rc;
}
static int resolve(void *state, struct VgiRpcCall *call, const char *token,
                   struct VgiTokenIdentity *out) {
    (void)state;
    if (!strcmp(token, "conformance-unavailable-token")) return unavailable(call, 5);
    if (!strcmp(token, "conformance-auth-unavailable-token")) return unavailable(call, 7);
    if (!strcmp(token, "conformance-unknown-token")) return ENOENT;
    out->principal = "subject@conformance.example";
    out->token_name = "conformance-subject";
    out->ttl_seconds = 300;
    if (!strcmp(token, "conformance-zero-ttl-token")) out->ttl_seconds = 0;
    if (!strcmp(token, "conformance-minimal-token")) out->token_name = "";
    if (!strcmp(token, "  conformance-padded-probe  ")) out->token_name = "conformance-padded";
    return 0;
}
static int mint(void *state, struct VgiRpcCall *call, const char *purpose,
                const char *const *scopes, size_t count, int64_t ttl, struct VgiIssuedGrant *out) {
    (void)state;
    (void)ttl;
    if (!strcmp(purpose, "conformance-auth-unavailable")) return unavailable(call, 7);
    if (!strcmp(purpose, "conformance-refused")) return ENOENT;
    size_t size = strlen(call->principal) + 32;
    for (size_t i = 0; i < count; ++i) size += strlen(scopes[i]) + 1;
    char *token = malloc(size);
    if (!token) return ENOMEM;
    snprintf(token, size, "conformance-grant-for:%s|", call->principal);
    for (size_t i = 0; i < count; ++i) {
        if (i) strcat(token, ",");
        strcat(token, scopes[i]);
    }
    out->token = token;
    out->grant_id = !strcmp(purpose, "conformance-minimal") ? "" : "conformance-grant-id";
    out->expires_at = 1893456000.0;
    return 0;
}
static void release(void *state, void *result, int is_grant) {
    (void)state;
    if (is_grant) free((void *)((struct VgiIssuedGrant *)result)->token);
}

static int who_args(struct ArrowSchema *s, struct ArrowError *e) {
    (void)e;
    ArrowSchemaInit(s);
    return ArrowSchemaSetTypeStruct(s, 0);
}
static int who_result(struct ArrowSchema *s, struct ArrowError *e) {
    (void)e;
    ArrowSchemaInit(s);
    int rc = ArrowSchemaSetTypeStruct(s, 1);
    if (!rc) rc = ArrowSchemaSetType(s->children[0], NANOARROW_TYPE_STRING);
    if (!rc) rc = ArrowSchemaSetName(s->children[0], "result");
    if (!rc) s->children[0]->flags = 0;
    return rc;
}
static const struct VgiMethod who_methods[] = {
    {"whoami", VGI_METHOD_UNARY, who_args, who_result, NULL, NULL}};
static int who(void *state, struct VgiRpcCall *c, struct VgiRpcReply *reply) {
    (void)state;
    struct json_object *value = json_object_new_object(),
                       *claims =
                           c->auth_claims_json ? json_tokener_parse(c->auth_claims_json) : NULL;
    json_object_object_add(value, "authenticated",
                           json_object_new_boolean(c->principal && *c->principal));
    json_object_object_add(value, "claims", claims ? claims : json_object_new_object());
    json_object_object_add(value, "domain",
                           json_object_new_string(c->auth_domain ? c->auth_domain : ""));
    json_object_object_add(value, "principal",
                           json_object_new_string(c->principal ? c->principal : ""));
    struct ArrowSchema schema = {0};
    int rc = who_result(&schema, &c->error);
    if (!rc) rc = VgiBatchInit(&reply->value, &schema, &c->error);
    if (!rc)
        rc = VgiBatchSetString(&reply->value, "result",
                               json_object_to_json_string_ext(value, JSON_C_TO_STRING_PLAIN));
    if (!rc) rc = VgiBatchFinishRow(&reply->value, &c->error);
    if (schema.release) ArrowSchemaRelease(&schema);
    json_object_put(value);
    return rc;
}
int ConformanceIdentityConfigure(struct VgiRpcServer *server, struct VgiHttpOptions *options,
                                 const char *mode, struct ArrowError *error) {
    memset(&identity, 0, sizeof(identity));
    identity.resolve_token = resolve;
    identity.introspect_principals = introspectors;
    identity.introspect_principal_count = 1;
    identity.release_result = release;
    if (!strcmp(mode, "both"))
        identity.mint_grant = mint;
    else if (!strcmp(mode, "grants")) {
        for (int i = 0; i < 32; ++i) {
            key_bytes[0][i] = (unsigned char)(0x10 + i);
            key_bytes[1][i] = (unsigned char)(0x30 + i);
        }
        keys = (struct VgiGrantKeys){key_bytes, 2, "conformance", 3600, 60};
        identity.grant_keys = &keys;
        options->grant_keys = &keys;
        struct VgiRpcProtocol p = {
            "conformance.Whoami.v1",
            "",
            "a280333ba72432020e162cab388a78355969a30aa74f0665ad9d2932d7a10b8f",
            who_methods,
            1,
            who,
            NULL};
        int rc = VgiRpcServerAddProtocol(server, &p, error);
        if (rc) return rc;
    } else if (strcmp(mode, "introspect-only"))
        return EINVAL;
    options->identity = &identity;
    options->authenticate_request = authenticate;
    return 0;
}
