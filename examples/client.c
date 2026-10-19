#include "vgi_rpc/client.h"

#include <string.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s WORKER | --http URL\n", argv[0]);
        return 1;
    }
    struct VgiClientOptions options;
    VgiClientOptionsInit(&options);
    options.protocol = "Calculator";
    options.protocol_version = "1.0.0";
    const char *command[] = {argv[1], NULL};
    options.argv = command;
    if (argc == 3 && !strcmp(argv[1], "--http")) {
        options.transport = VGI_CLIENT_HTTP;
        options.target = argv[2];
    }
    struct VgiClient *client = NULL;
    struct ArrowError error = {{0}};
    int rc = VgiClientConnect(&options, &client, &error);
    struct ArrowSchema schema = {0};
    struct VgiAnnotatedBatch input = {0}, output = {0};
    if (!rc) {
        ArrowSchemaInit(&schema);
        rc = ArrowSchemaSetTypeStruct(&schema, 2);
    }
    const char *names[] = {"a", "b"};
    for (int i = 0; !rc && i < 2; ++i) {
        rc = ArrowSchemaSetType(schema.children[i], NANOARROW_TYPE_DOUBLE);
        if (!rc) rc = ArrowSchemaSetName(schema.children[i], names[i]);
        schema.children[i]->flags = 0;
    }
    if (!rc) rc = VgiBatchInit(&input.batch, &schema, &error);
    if (!rc) rc = ArrowArrayAppendDouble(input.batch.array.children[0], 2);
    if (!rc) rc = ArrowArrayAppendDouble(input.batch.array.children[1], 3);
    if (!rc) rc = VgiBatchFinishRow(&input.batch, &error);
    if (!rc) rc = VgiClientUnary(client, "add", &input, &output);
    if (!rc) rc = VgiBatchView(&output.batch, &error);
    if (!rc) {
        double value = ArrowArrayViewGetDoubleUnsafe(VgiBatchGet(&output.batch, "result"), 0);
        printf("2 + 3 = %.0f\n", value);
        if (value != 5) rc = 1;
    }
    if (rc) {
        const struct VgiClientError *remote = client ? VgiClientLastError(client) : NULL;
        fprintf(stderr, "%s\n", remote && remote->message ? remote->message : error.message);
    }
    VgiAnnotatedBatchReset(&input);
    VgiAnnotatedBatchReset(&output);
    if (schema.release) ArrowSchemaRelease(&schema);
    VgiClientFree(client);
    return rc ? 1 : 0;
}
