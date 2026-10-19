#include <stdlib.h>
#include <string.h>

#include "vgi_rpc/http.h"

static int params(struct ArrowSchema *schema, struct ArrowError *error) {
    (void)error;
    ArrowSchemaInit(schema);
    int rc = ArrowSchemaSetTypeStruct(schema, 2);
    const char *names[] = {"a", "b"};
    for (int i = 0; !rc && i < 2; ++i) {
        rc = ArrowSchemaSetType(schema->children[i], NANOARROW_TYPE_DOUBLE);
        if (!rc) rc = ArrowSchemaSetName(schema->children[i], names[i]);
        schema->children[i]->flags = 0;
    }
    return rc;
}
static int result(struct ArrowSchema *schema, struct ArrowError *error) {
    (void)error;
    ArrowSchemaInit(schema);
    int rc = ArrowSchemaSetTypeStruct(schema, 1);
    if (!rc) rc = ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_DOUBLE);
    if (!rc) rc = ArrowSchemaSetName(schema->children[0], "result");
    if (!rc) schema->children[0]->flags = 0;
    return rc;
}
static int add(void *state, struct VgiRpcCall *call, struct VgiRpcReply *reply) {
    (void)state;
    struct ArrowSchema schema = {0};
    int rc = result(&schema, &call->error);
    if (!rc) rc = VgiBatchInit(&reply->value, &schema, &call->error);
    if (!rc) {
        double a = ArrowArrayViewGetDoubleUnsafe(VgiBatchGet(&call->request, "a"), 0);
        double b = ArrowArrayViewGetDoubleUnsafe(VgiBatchGet(&call->request, "b"), 0);
        rc = ArrowArrayAppendDouble(reply->value.array.children[0], a + b);
    }
    if (!rc) rc = VgiBatchFinishRow(&reply->value, &call->error);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
int main(int argc, char **argv) {
    static const struct VgiMethod methods[] = {
        {"add", VGI_METHOD_UNARY, params, result, NULL, NULL, VGI_STREAM_UNKNOWN}};
    struct VgiRpcProtocol protocol = {"Calculator", "1.0.0", "", methods, 1, add, NULL, NULL};
    struct ArrowError error = {{0}};
    struct VgiRpcServer *server = VgiRpcServerNew();
    if (!server) return 1;
    int rc = VgiRpcServerAddProtocol(server, &protocol, &error);
    if (!rc && argc > 1 && !strcmp(argv[1], "--http")) {
        struct VgiHttpOptions options;
        VgiHttpOptionsInit(&options);
        options.port = argc > 2 ? (unsigned)strtoul(argv[2], NULL, 10) : 0;
        rc = VgiRpcServerServeHttp(server, &options, &error);
    } else if (!rc)
        rc = VgiRpcServerServe(server, stdin, stdout, &error);
    if (rc) fprintf(stderr, "%s\n", error.message);
    VgiRpcServerFree(server);
    return rc ? 1 : 0;
}
