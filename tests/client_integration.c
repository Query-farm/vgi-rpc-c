#define _POSIX_C_SOURCE 200809L
#include <stdlib.h>
#include <string.h>

#include "vgi_rpc/client.h"

static struct ArrowError error;
static struct VgiClient *client;
#define CHECK(x)                                                                          \
    do {                                                                                  \
        if (!(x)) {                                                                       \
            const struct VgiClientError *e = client ? VgiClientLastError(client) : NULL;  \
            fprintf(stderr, "%s:%d: %s: %s; %s\n", __FILE__, __LINE__, #x, error.message, \
                    e && e->message ? e->message : "");                                   \
            abort();                                                                      \
        }                                                                                 \
    } while (0)
static void field(struct ArrowSchema *out, const char *name, const char *format) {
    CHECK(!ArrowSchemaSetName(out, name));
    CHECK(!ArrowSchemaSetFormat(out, format));
}
static void empty(struct VgiAnnotatedBatch *out) {
    struct ArrowSchema schema = {0};
    CHECK(!VgiSchemaRpcEmpty(&schema, &error));
    CHECK(!VgiBatchInit(&out->batch, &schema, &error));
    CHECK(!VgiBatchFinish(&out->batch, &error));
    ArrowSchemaRelease(&schema);
}
static void typed_schema(struct ArrowSchema *s) {
    ArrowSchemaInit(s);
    CHECK(!ArrowSchemaSetTypeStruct(s, 6));
    field(s->children[0], "nullable_float", "g");
    CHECK(!ArrowSchemaSetType(s->children[1], NANOARROW_TYPE_LIST));
    field(s->children[1], "tags", "+l");
    field(s->children[1]->children[0], "item", "u");
    field(s->children[2], "category", "s");
    CHECK(!ArrowSchemaAllocateDictionary(s->children[2]));
    ArrowSchemaInit(s->children[2]->dictionary);
    CHECK(!ArrowSchemaSetType(s->children[2]->dictionary, NANOARROW_TYPE_STRING));
    field(s->children[3], "event_time", "tsu:UTC");
    field(s->children[4], "amount", "d:18,4");
    CHECK(!ArrowSchemaSetTypeStruct(s->children[5], 2));
    field(s->children[5], "nested", "+s");
    field(s->children[5]->children[0], "name", "u");
    CHECK(!ArrowSchemaSetType(s->children[5]->children[1], NANOARROW_TYPE_LIST));
    field(s->children[5]->children[1], "scores", "+l");
    field(s->children[5]->children[1]->children[0], "item", "i");
}
static void typed_exchange(void) {
    struct VgiAnnotatedBatch params = {0}, header = {0};
    empty(&params);
    CHECK(!VgiClientStreamOpen(client, "typed_exchange", &params, 1, 0, &header));
    struct ArrowSchema schema = {0};
    typed_schema(&schema);
    for (int sample = 0; sample < 3; ++sample) {
        struct VgiAnnotatedBatch input = {0}, output = {0};
        CHECK(!VgiBatchInit(&input.batch, &schema, &error));
        struct ArrowArray *a = &input.batch.array;
        if (!sample) {
            for (int i = 0; i < 6; ++i) CHECK(!ArrowArrayAppendNull(a->children[i], 1));
            CHECK(!ArrowArrayFinishElement(a));
        } else if (sample == 2) {
            CHECK(!ArrowArrayAppendDouble(a->children[0], 1.25));
            CHECK(!ArrowArrayAppendString(a->children[1]->children[0], ArrowCharView("café")));
            CHECK(!ArrowArrayAppendNull(a->children[1]->children[0], 1));
            CHECK(!ArrowArrayFinishElement(a->children[1]));
            CHECK(!ArrowArrayAppendString(a->children[2]->dictionary, ArrowCharView("blue")));
            CHECK(!ArrowArrayAppendInt(a->children[2], 0));
            CHECK(!ArrowArrayAppendInt(a->children[3], INT64_C(1700000000123456)));
            struct ArrowDecimal decimal;
            ArrowDecimalInit(&decimal, 128, 18, 4);
            ArrowDecimalSetInt(&decimal, 1234567);
            CHECK(!ArrowArrayAppendDecimal(a->children[4], &decimal));
            CHECK(!ArrowArrayAppendString(a->children[5]->children[0], ArrowCharView("native")));
            struct ArrowArray *scores = a->children[5]->children[1];
            CHECK(!ArrowArrayAppendInt(scores->children[0], 7));
            CHECK(!ArrowArrayAppendNull(scores->children[0], 1));
            CHECK(!ArrowArrayFinishElement(scores));
            CHECK(!ArrowArrayFinishElement(a->children[5]));
            CHECK(!ArrowArrayFinishElement(a));
        }
        CHECK(!VgiBatchFinish(&input.batch, &error));
        int done = 0;
        CHECK(!VgiClientStreamExchange(client, &input, &output, &done));
        CHECK(!done);
        CHECK(!VgiBatchView(&input.batch, &error));
        CHECK(!VgiBatchView(&output.batch, &error));
        int equal = 0;
        CHECK(!ArrowArrayViewCompare(&output.batch.view, &input.batch.view,
                                     NANOARROW_COMPARE_IDENTICAL, &equal, &error));
        CHECK(equal);
        struct ArrowBuffer before, after;
        ArrowBufferInit(&before);
        ArrowBufferInit(&after);
        CHECK(!VgiIpcWriteSchema(&schema, &before, &error));
        CHECK(!VgiIpcWriteSchema(&output.batch.schema, &after, &error));
        CHECK(before.size_bytes == after.size_bytes &&
              !memcmp(before.data, after.data, (size_t)before.size_bytes));
        ArrowBufferReset(&before);
        ArrowBufferReset(&after);
        VgiAnnotatedBatchReset(&input);
        VgiAnnotatedBatchReset(&output);
    }
    CHECK(!VgiClientStreamClose(client, 0));
    ArrowSchemaRelease(&schema);
    VgiAnnotatedBatchReset(&params);
    VgiAnnotatedBatchReset(&header);
}
static void producer(const char *method, int count, int payload_bytes, int expected, int zero_first,
                     int first_index) {
    struct VgiAnnotatedBatch params = {0}, header = {0};
    if (!strcmp(method, "producer_sequence")) {
        struct ArrowSchema schema = {0};
        ArrowSchemaInit(&schema);
        CHECK(!ArrowSchemaSetTypeStruct(&schema, 2));
        field(schema.children[0], "count", "l");
        field(schema.children[1], "payload_bytes", "l");
        schema.children[0]->flags = schema.children[1]->flags = 0;
        CHECK(!VgiBatchInit(&params.batch, &schema, &error));
        CHECK(!VgiBatchSetInt(&params.batch, "count", count));
        CHECK(!VgiBatchSetInt(&params.batch, "payload_bytes", payload_bytes));
        CHECK(!VgiBatchFinishRow(&params.batch, &error));
        ArrowSchemaRelease(&schema);
    } else
        empty(&params);
    CHECK(!VgiClientStreamOpen(client, method, &params, 0, 0, &header));
    int seen = 0, done = 0;
    while (!done) {
        struct VgiAnnotatedBatch value = {0};
        CHECK(!VgiClientStreamTick(client, NULL, &value, &done));
        if (!done) {
            CHECK(!VgiBatchView(&value.batch, &error));
            if (!seen && zero_first)
                CHECK(value.batch.array.length == 0);
            else {
                CHECK(value.batch.array.length == 1);
                CHECK(VgiBatchInt(&value.batch, "index", -1) == first_index + seen - zero_first);
            }
            ++seen;
        }
        VgiAnnotatedBatchReset(&value);
    }
    CHECK(seen == expected);
    struct VgiAnnotatedBatch terminal = {0};
    CHECK(!VgiClientStreamTick(client, NULL, &terminal, &done));
    CHECK(done);
    VgiAnnotatedBatchReset(&terminal);
    VgiAnnotatedBatchReset(&params);
    VgiAnnotatedBatchReset(&header);
}
static void echo_bytes(void) {
    struct ArrowSchema schema = {0};
    ArrowSchemaInit(&schema);
    CHECK(!ArrowSchemaSetTypeStruct(&schema, 1));
    field(schema.children[0], "value", "z");
    schema.children[0]->flags = 0;
    unsigned char content[32768];
    for (size_t i = 0; i < sizeof(content); ++i) content[i] = (unsigned char)(i % 251);
    struct VgiAnnotatedBatch params = {0}, output = {0};
    CHECK(!VgiBatchInit(&params.batch, &schema, &error));
    CHECK(!VgiBatchSetBytes(&params.batch, "value", content, sizeof(content)));
    CHECK(!VgiBatchFinishRow(&params.batch, &error));
    CHECK(!VgiClientUnary(client, "echo_bytes", &params, &output));
    CHECK(!VgiBatchView(&output.batch, &error));
    const struct ArrowArrayView *result = VgiBatchGet(&output.batch, "result");
    CHECK(result);
    struct ArrowBufferView value = ArrowArrayViewGetBytesUnsafe(result, 0);
    CHECK(value.size_bytes == sizeof(content) &&
          !memcmp(value.data.data, content, sizeof(content)));
    VgiAnnotatedBatchReset(&params);
    VgiAnnotatedBatchReset(&output);
    ArrowSchemaRelease(&schema);
}
int main(int argc, char **argv) {
    CHECK(argc >= 3);
    struct VgiClientOptions options;
    VgiClientOptionsInit(&options);
    options.protocol = "ClientConformanceService";
    options.target = argv[2];
    const char *command[] = {argv[2], "-m", "vgi_rpc.conformance.client_worker", "--stdio", NULL};
    if (!strcmp(argv[1], "stdio")) {
        options.transport = VGI_CLIENT_STDIO;
        options.argv = command;
    } else if (!strcmp(argv[1], "unix"))
        options.transport = VGI_CLIENT_UNIX;
    else if (!strcmp(argv[1], "tcp"))
        options.transport = VGI_CLIENT_TCP;
    else
        options.transport = VGI_CLIENT_HTTP;
    if (argc > 3) options.tls_ca_file = argv[3];
    struct VgiExternalOptions external;
    VgiExternalOptionsInit(&external);
    external.validate_url = NULL;
    options.external = &external;
    int rc = VgiClientConnect(&options, &client, &error);
    if (!strcmp(argv[1], "untrusted")) {
        CHECK(rc && !client && strstr(error.message, "certificate"));
        return 0;
    }
    CHECK(!rc);
    echo_bytes();
    typed_exchange();
    producer("producer_sequence", 100, 1024, 100, 0, 0);
    producer("producer_zero_row_then_value", 0, 0, 2, 1, 7);
    producer("producer_emit_and_finish", 0, 0, 1, 0, 99);
    producer("producer_empty", 0, 0, 0, 0, 0);
    struct VgiAnnotatedBatch listing = {0}, description = {0};
    CHECK(!VgiClientDescribe(client, &listing, &description));
    CHECK(VgiBatchGet(&listing.batch, "server_id") && VgiBatchGet(&description.batch, "methods"));
    VgiAnnotatedBatchReset(&listing);
    VgiAnnotatedBatchReset(&description);
    VgiClientFree(client);
    puts("Native client integration passed");
    return 0;
}
