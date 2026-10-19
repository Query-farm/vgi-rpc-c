#include <errno.h>
#include <json-c/json.h>
#include <stdlib.h>
#include <string.h>

#include "vgi_rpc/rpc.h"
static int fields(struct ArrowSchema *out, int n, const char **names, const char **formats) {
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, n);
    for (int i = 0; !rc && i < n; ++i) {
        rc = ArrowSchemaSetFormat(out->children[i], formats[i]);
        if (!rc) rc = ArrowSchemaSetName(out->children[i], names[i]);
        out->children[i]->flags = 0;
    }
    return rc;
}
static int echo_args(struct ArrowSchema *out, struct ArrowError *error) {
    (void)error;
    const char *names[] = {"value"}, *formats[] = {"u"};
    return fields(out, 1, names, formats);
}
static int echo_result(struct ArrowSchema *out, struct ArrowError *error) {
    (void)error;
    const char *names[] = {"result"}, *formats[] = {"u"};
    return fields(out, 1, names, formats);
}
static int fail_args(struct ArrowSchema *out, struct ArrowError *error) {
    (void)error;
    const char *names[] = {"code", "kind", "retry_delay_seconds"}, *formats[] = {"u", "u", "g"};
    return fields(out, 3, names, formats);
}
static int empty_args(struct ArrowSchema *out, struct ArrowError *error) {
    (void)error;
    return fields(out, 0, NULL, NULL);
}
static const struct VgiMethod methods[] = {
    {"echo_string", VGI_METHOD_UNARY, echo_args, echo_result, NULL, NULL},
    {"fail", VGI_METHOD_VOID, fail_args, NULL, NULL, NULL},
    {"fail_oversized", VGI_METHOD_VOID, empty_args, NULL, NULL, NULL}};
static struct json_object *detail(const char *type) {
    struct json_object *o = json_object_new_object();
    json_object_object_add(o, "@type", json_object_new_string(type));
    return o;
}
static int handler(void *state, struct VgiRpcCall *call, struct VgiRpcReply *reply) {
    (void)state;
    if (!strcmp(call->method->name, "echo_string")) {
        struct ArrowSchema schema = {0};
        struct ArrowBuffer value;
        ArrowBufferInit(&value);
        int rc = echo_result(&schema, &call->error);
        if (!rc) rc = VgiBatchInit(&reply->value, &schema, &call->error);
        struct ArrowStringView text = VgiBatchString(&call->request, "value");
        if (!rc) rc = ArrowBufferAppend(&value, "secondary:", 10);
        if (!rc) rc = ArrowBufferAppend(&value, text.data, text.size_bytes);
        if (!rc) rc = VgiBatchSetBytes(&reply->value, "result", value.data, value.size_bytes);
        if (!rc) rc = VgiBatchFinishRow(&reply->value, &call->error);
        ArrowBufferReset(&value);
        if (schema.release) ArrowSchemaRelease(&schema);
        return rc;
    }
    const char *codes[] = {"CANCELLED",
                           "UNKNOWN",
                           "INVALID_ARGUMENT",
                           "DEADLINE_EXCEEDED",
                           "NOT_FOUND",
                           "ALREADY_EXISTS",
                           "PERMISSION_DENIED",
                           "RESOURCE_EXHAUSTED",
                           "FAILED_PRECONDITION",
                           "ABORTED",
                           "OUT_OF_RANGE",
                           "UNIMPLEMENTED",
                           "INTERNAL",
                           "UNAVAILABLE",
                           "DATA_LOSS",
                           "UNAUTHENTICATED"};
    int oversized = !strcmp(call->method->name, "fail_oversized");
    const char *code = oversized ? "RESOURCE_EXHAUSTED" : NULL;
    struct ArrowStringView supplied = VgiBatchString(&call->request, "code");
    for (size_t i = 0; !code && i < sizeof(codes) / sizeof(*codes); ++i)
        if (supplied.size_bytes == (int64_t)strlen(codes[i]) &&
            !memcmp(supplied.data, codes[i], (size_t)supplied.size_bytes))
            code = codes[i];
    if (!code) {
        int rc = VgiRpcSetErrorDetails(
            call,
            "[{\"@type\":\"vgi_rpc.BadRequest\",\"field_violations\":[{\"field\":\"code\","
            "\"description\":\"must be a canonical code name\"}]}]");
        return rc ? rc
                  : VgiRpcFail(call, "StatusError", "INVALID_ARGUMENT", "invalid_code",
                               "code must be a canonical code name");
    }
    struct ArrowStringView kind =
        oversized ? ArrowCharView("details_oversized") : VgiBatchString(&call->request, "kind");
    char *reason = malloc((size_t)kind.size_bytes + 1);
    if (!reason) return ENOMEM;
    memcpy(reason, kind.data, (size_t)kind.size_bytes);
    reason[kind.size_bytes] = 0;
    struct json_object *details = json_object_new_array(), *info = detail("vgi_rpc.ErrorInfo"),
                       *metadata = json_object_new_object();
    double delay = oversized ? 1
                             : ArrowArrayViewGetDoubleUnsafe(
                                   VgiBatchGet(&call->request, "retry_delay_seconds"), 0);
    if (oversized) {
        char padding[5001];
        memset(padding, 'x', 5000);
        padding[5000] = 0;
        json_object_object_add(metadata, "padding", json_object_new_string(padding));
    } else
        json_object_object_add(metadata, "fixture",
                               json_object_new_string("conformance.Secondary.v1"));
    json_object_object_add(info, "metadata", metadata);
    if (!oversized) json_object_array_add(details, info);
    if (delay > 0) {
        struct json_object *retry = detail("vgi_rpc.RetryInfo");
        json_object_object_add(retry, "retry_delay_seconds", json_object_new_double(delay));
        json_object_array_add(details, retry);
    }
    if (oversized)
        json_object_array_add(details, info);
    else {
        struct json_object *probe = detail("conformance.Secondary.v1.Probe");
        json_object_object_add(
            probe, "note", json_object_new_string("clients ignore detail types they do not know"));
        json_object_array_add(details, probe);
    }
    int rc = VgiRpcSetErrorDetails(call,
                                   json_object_to_json_string_ext(details, JSON_C_TO_STRING_PLAIN));
    json_object_put(details);
    if (!rc) rc = VgiRpcFail(call, "StatusError", code, reason, "conformance.Secondary.v1 failure");
    free(reason);
    return rc;
}
int ConformanceSecondaryRegister(struct VgiRpcServer *worker, struct ArrowError *error) {
    struct VgiRpcProtocol protocol = {
        "conformance.Secondary.v1",
        "",
        "58557cf1611546ad22d1c379bc3ce1b04166082f78375e9fc959f0086347eab6",
        methods,
        3,
        handler,
        NULL};
    return VgiRpcServerAddProtocol(worker, &protocol, error);
}
