/* The conformance_negotiating_client hook (vgi-rpc PROTOCOL_VERSIONS_V1 §9):
 * a vgi-rpc-c HTTP client bound to one protocol, declaring a newest version and
 * a supported window, negotiating per §6. One client lives for the whole
 * process, so its per-endpoint state carries across calls.
 *
 *   conformance_negotiating_client BASE_URL PROTOCOL VERSION RANGE
 *
 * Each "ping" line on stdin makes one no-argument unary `ping` call and prints
 * one JSON line: {"result": "..."} or {"error_kind", "error_code", "message"}. */
#include <json-c/json.h>
#include <stdio.h>
#include <string.h>

#include "rpc_internal.h"
#include "vgi_rpc/client.h"

static void reply(struct json_object *line) {
    puts(json_object_to_json_string_ext(line, JSON_C_TO_STRING_PLAIN));
    fflush(stdout);
    json_object_put(line);
}
static void report_error(struct VgiClient *client, int rc) {
    const struct VgiClientError *e = VgiClientLastError(client);
    struct json_object *line = json_object_new_object();
    json_object_object_add(line, "error_kind", json_object_new_string(e->kind ? e->kind : ""));
    json_object_object_add(line, "error_code", json_object_new_string(e->code ? e->code : ""));
    json_object_object_add(line, "error_type", json_object_new_string(e->type ? e->type : ""));
    json_object_object_add(line, "message",
                           json_object_new_string(e->message ? e->message : strerror(rc)));
    reply(line);
}
int main(int argc, char **argv) {
    if (argc != 5) {
        fprintf(stderr, "usage: %s BASE_URL PROTOCOL VERSION RANGE\n", argv[0]);
        return 2;
    }
    struct VgiClientOptions options;
    VgiClientOptionsInit(&options);
    options.transport = VGI_CLIENT_HTTP;
    options.target = argv[1];
    options.protocol = argv[2];
    options.protocol_version = argv[3];
    options.protocol_version_range = argv[4];
    options.compression_level = 0;
    struct VgiClient *client = NULL;
    struct ArrowError error = {{0}};
    if (VgiClientConnect(&options, &client, &error)) {
        fprintf(stderr, "connect failed: %s\n", error.message);
        return 1;
    }
    char command[64];
    while (fgets(command, sizeof(command), stdin)) {
        if (strncmp(command, "ping", 4)) continue;
        struct VgiAnnotatedBatch in = {0}, out = {0};
        ArrowBufferInit(&in.metadata);
        int rc = VgiRpcBatchInitNamed(&in.batch, "Empty", &error);
        if (!rc) rc = VgiBatchFinishRow(&in.batch, &error);
        if (!rc) rc = VgiClientUnary(client, "ping", &in, &out);
        if (!rc) rc = VgiBatchView(&out.batch, &error);
        if (rc) {
            report_error(client, rc);
        } else {
            struct ArrowStringView value = VgiBatchString(&out.batch, "result");
            struct json_object *line = json_object_new_object();
            json_object_object_add(
                line, "result",
                json_object_new_string_len(value.data ? value.data : "", (int)value.size_bytes));
            reply(line);
        }
        VgiAnnotatedBatchReset(&in);
        VgiAnnotatedBatchReset(&out);
    }
    VgiClientFree(client);
    return 0;
}
