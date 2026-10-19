#define _POSIX_C_SOURCE 200809L
/* JSONL bridge only: all RPC framing, error/log parsing and external transfers
 * belong to the native C client. Contract: CLIENT_DRIVER_PROTOCOL.md. */
#include <errno.h>
#include <json-c/json.h>
#include <signal.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "vgi_rpc/client.h"

struct Driver {
    struct VgiClient *client;
    struct json_object *logs;
};
static const char *text_field(struct json_object *o, const char *key) {
    struct json_object *v = NULL;
    return json_object_object_get_ex(o, key, &v) && json_object_is_type(v, json_type_string)
               ? json_object_get_string(v)
               : NULL;
}
static int boolean(struct json_object *o, const char *key) {
    return json_object_get_boolean(json_object_object_get(o, key));
}
static struct json_object *string(const char *s) { return s ? json_object_new_string(s) : NULL; }
static struct json_object *answer(int ok) {
    struct json_object *o = json_object_new_object();
    json_object_object_add(o, "ok", json_object_new_boolean(ok));
    return o;
}
static struct json_object *driver_error(const char *message) {
    struct json_object *o = answer(0);
    json_object_object_add(o, "error", string(message));
    return o;
}
static void log_received(void *state, const char *level, const char *message, const char *extra) {
    struct Driver *d = state;
    struct json_object *o = json_object_new_object();
    json_object_object_add(o, "level", string(level));
    json_object_object_add(o, "message", string(message));
    json_object_object_add(o, "extra", json_tokener_parse(extra));
    json_object_array_add(d->logs, o);
}
static void call_error(struct Driver *d, struct json_object *out) {
    const struct VgiClientError *e = VgiClientLastError(d->client);
    struct json_object *o = json_object_new_object();
    json_object_object_add(o, "error_type", string(e->type));
    json_object_object_add(o, "error_message", string(e->message));
    json_object_object_add(o, "traceback", string(e->traceback));
    json_object_object_add(o, "error_code", string(e->code));
    json_object_object_add(o, "error_kind", string(e->kind));
    json_object_object_add(
        o, "error_details",
        e->details_json ? json_tokener_parse(e->details_json) : json_object_new_array());
    json_object_object_add(out, "error", o);
}
static int decode(const char *text, struct VgiAnnotatedBatch *out, struct ArrowError *error) {
    if (!text) {
        ArrowErrorSet(error, "Missing IPC base64 field");
        return EINVAL;
    }
    ArrowBufferInit(&out->metadata);
    size_t size = strlen(text), decoded = 0;
    struct ArrowBuffer buffer;
    ArrowBufferInit(&buffer);
    int rc = ArrowBufferResize(&buffer, (int64_t)size, 0);
    if (!rc && sodium_base642bin(buffer.data, size, text, size, NULL, &decoded, NULL,
                                 sodium_base64_VARIANT_ORIGINAL))
        rc = EINVAL;
    if (!rc)
        rc = VgiIpcReadBatch((struct ArrowBufferView){{buffer.data}, (int64_t)decoded},
                             &out->batch.schema, &out->batch.array, &out->metadata, error);
    ArrowBufferReset(&buffer);
    if (rc && !error->message[0]) ArrowErrorSet(error, "Invalid base64 or IPC stream");
    return rc;
}
static struct json_object *encode_bytes(struct ArrowBufferView bytes) {
    size_t size =
        sodium_base64_ENCODED_LEN((size_t)bytes.size_bytes, sodium_base64_VARIANT_ORIGINAL);
    char *out = malloc(size);
    if (!out) return NULL;
    sodium_bin2base64(out, size, bytes.data.as_uint8, (size_t)bytes.size_bytes,
                      sodium_base64_VARIANT_ORIGINAL);
    struct json_object *value = string(out);
    free(out);
    return value;
}
static int encode(struct json_object *out, const char *key, const struct VgiAnnotatedBatch *value,
                  struct ArrowError *error) {
    if (!value->batch.array.release) {
        json_object_object_add(out, key, NULL);
        return 0;
    }
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    int rc = VgiIpcWriteBatch(&value->batch.schema, &value->batch.array,
                              (const char *)value->metadata.data, &bytes, error);
    if (!rc)
        json_object_object_add(
            out, key, encode_bytes((struct ArrowBufferView){{bytes.data}, bytes.size_bytes}));
    ArrowBufferReset(&bytes);
    return rc;
}
static struct json_object *connect_client(struct Driver *d, struct json_object *request) {
    if (d->client) return driver_error("Already connected");
    const char *transport = text_field(request, "transport"),
               *protocol = text_field(request, "protocol");
    if (!transport || !protocol || !*protocol)
        return driver_error("connect requires transport and protocol");
    struct VgiClientOptions options;
    VgiClientOptionsInit(&options);
    options.protocol = protocol;
    options.on_log = log_received;
    options.log_state = d;
    options.target = text_field(request, "target");
    const char **argv = NULL;
    if (!strcmp(transport, "stdio") || !strcmp(transport, "shm")) {
        options.transport = VGI_CLIENT_STDIO;
        if (!strcmp(transport, "shm")) options.shared_memory_size = 64 * 1024 * 1024;
        struct json_object *args = json_object_object_get(request, "target");
        if (!json_object_is_type(args, json_type_array))
            return driver_error("stdio target must be an argv array");
        size_t n = json_object_array_length(args);
        argv = calloc(n + 1, sizeof(char *));
        if (!argv) return driver_error("Out of memory");
        for (size_t i = 0; i < n; ++i)
            argv[i] = json_object_get_string(json_object_array_get_idx(args, i));
        options.argv = argv;
    } else if (!strcmp(transport, "unix"))
        options.transport = VGI_CLIENT_UNIX;
    else if (!strcmp(transport, "tcp"))
        options.transport = VGI_CLIENT_TCP;
    else if (!strcmp(transport, "http"))
        options.transport = VGI_CLIENT_HTTP;
    else
        return driver_error("Unknown transport");
    struct VgiExternalOptions external;
    VgiExternalOptionsInit(&external);
    if (boolean(request, "external")) {
        external.validate_url = NULL;
        options.external = &external;
    }
    struct json_object *level = NULL;
    if (json_object_object_get_ex(request, "compression_level", &level))
        options.compression_level =
            json_object_is_type(level, json_type_null) ? 0 : json_object_get_int(level);
    struct json_object *headers = json_object_object_get(request, "headers");
    size_t header_count = headers && json_object_is_type(headers, json_type_object)
                              ? json_object_object_length(headers)
                              : 0;
    struct VgiClientHeader *header_values = calloc(header_count + 1, sizeof(*header_values));
    if (headers && header_count) {
        json_object_object_foreach(headers, key, value) {
            header_values[options.header_count++] =
                (struct VgiClientHeader){key, json_object_get_string(value)};
        }
    }
    options.headers = header_values;
    struct ArrowError error = {{0}};
    int rc = VgiClientConnect(&options, &d->client, &error);
    free(argv);
    free(header_values);
    return rc ? driver_error(error.message) : answer(1);
}
static struct json_object *view_string(struct ArrowStringView value) {
    return json_object_new_string_len(value.data, (int)value.size_bytes);
}
static int describe(struct Driver *d, struct json_object *out) {
    struct VgiAnnotatedBatch listing = {0}, description = {0};
    int rc = VgiClientDescribe(d->client, &listing, &description);
    if (rc)
        call_error(d, out);
    else {
        struct json_object *value = json_object_new_object(), *methods = json_object_new_array();
        const char *source[] = {"protocol", "protocol_version", "protocol_hash"},
                   *target[] = {"protocol_name", "protocol_version", "protocol_hash"};
        for (int i = 0; i < 3; ++i)
            json_object_object_add(value, target[i],
                                   view_string(VgiBatchString(&description.batch, source[i])));
        json_object_object_add(value, "server_id",
                               view_string(VgiBatchString(&listing.batch, "server_id")));
        json_object_object_add(value, "request_version",
                               view_string(VgiBatchString(&listing.batch, "request_version")));
        json_object_object_add(value, "describe_version", string("5"));
        const struct ArrowArrayView *list = VgiBatchGet(&description.batch, "methods");
        const struct ArrowSchema *schema =
            description.batch.schema.children[VgiFieldIndex(&description.batch.schema, "methods")]
                ->children[0];
        const struct ArrowArrayView *rows = list->children[0];
        for (int64_t row = ArrowArrayViewListChildOffset(list, 0);
             row < ArrowArrayViewListChildOffset(list, 1); ++row) {
            struct json_object *method = json_object_new_object();
            const char *texts[] = {"name", "method_type"}, *bools[] = {"has_return", "has_header"};
            for (int i = 0; i < 2; ++i) {
                int64_t index = VgiFieldIndex(schema, texts[i]);
                json_object_object_add(
                    method, texts[i],
                    view_string(ArrowArrayViewGetStringUnsafe(rows->children[index], row)));
                index = VgiFieldIndex(schema, bools[i]);
                json_object_object_add(method, bools[i],
                                       json_object_new_boolean(
                                           ArrowArrayViewGetIntUnsafe(rows->children[index], row)));
            }
            int64_t index = VgiFieldIndex(schema, "stream_kind");
            struct ArrowStringView kind = ArrowArrayViewGetStringUnsafe(rows->children[index], row);
            json_object_object_add(method, "is_exchange",
                                   kind.size_bytes == 8 && !memcmp(kind.data, "exchange", 8)
                                       ? json_object_new_boolean(1)
                                   : kind.size_bytes == 8 && !memcmp(kind.data, "producer", 8)
                                       ? json_object_new_boolean(0)
                                       : NULL);
            const char *schemas[] = {"params_schema_ipc", "result_schema_ipc", "header_schema_ipc"},
                       *encoded[] = {"params_schema_b64", "result_schema_b64", "header_schema_b64"};
            for (int i = 0; i < 3; ++i) {
                index = VgiFieldIndex(schema, schemas[i]);
                struct ArrowBufferView bytes =
                    ArrowArrayViewGetBytesUnsafe(rows->children[index], row);
                json_object_object_add(method, encoded[i],
                                       bytes.size_bytes ? encode_bytes(bytes) : NULL);
            }
            json_object_array_add(methods, method);
        }
        json_object_object_add(value, "methods", methods);
        json_object_object_add(out, "describe", value);
    }
    VgiAnnotatedBatchReset(&listing);
    VgiAnnotatedBatchReset(&description);
    return rc;
}
static struct json_object *csv(const char *text) {
    struct json_object *out = json_object_new_array();
    char *copy = strdup(text ? text : ""), *save = NULL;
    for (char *s = strtok_r(copy, ",", &save); s; s = strtok_r(NULL, ",", &save)) {
        while (*s == ' ') ++s;
        size_t n = strlen(s);
        while (n && s[n - 1] == ' ') s[--n] = 0;
        if (n) json_object_array_add(out, string(s));
    }
    free(copy);
    return out;
}
static int http_op(struct Driver *d, const char *op, struct json_object *request,
                   struct json_object *out) {
    const struct VgiClientCapabilities *caps = VgiClientCapabilities(d->client);
    if (!caps) return ENOTSUP;
    if (!strcmp(op, "capabilities")) {
        struct json_object *value = json_object_new_object();
        json_object_object_add(value, "sticky_enabled",
                               json_object_new_boolean(caps->sticky_enabled));
        json_object_object_add(value, "upload_url_support",
                               json_object_new_boolean(caps->upload_url_support));
        json_object_object_add(value, "externalization_enabled",
                               json_object_new_boolean(caps->externalization_enabled));
        const char *names[] = {"sticky_default_ttl", "max_request_bytes", "max_response_bytes",
                               "max_externalized_response_bytes", "max_upload_bytes"};
        int64_t values[] = {caps->sticky_default_ttl, caps->max_request_bytes,
                            caps->max_response_bytes, caps->max_externalized_response_bytes,
                            caps->max_upload_bytes};
        for (int i = 0; i < 5; ++i)
            json_object_object_add(value, names[i],
                                   values[i] < 0 ? NULL : json_object_new_int64(values[i]));
        json_object_object_add(value, "supported_encodings", csv(caps->supported_encodings));
        json_object_object_add(value, "sticky_echo_headers", csv(caps->sticky_echo_headers));
        json_object_object_add(out, "caps", value);
        return 0;
    }
    if (!strcmp(op, "request_upload_urls")) {
        struct VgiExternalUploadUrl *urls = NULL;
        size_t count = 0;
        int rc = VgiClientUploadUrls(d->client,
                                     json_object_get_int(json_object_object_get(request, "count")),
                                     &urls, &count);
        if (rc)
            call_error(d, out);
        else {
            struct json_object *values = json_object_new_array();
            for (size_t i = 0; i < count; ++i) {
                struct json_object *value = json_object_new_object();
                json_object_object_add(value, "upload_url", string(urls[i].upload_url));
                json_object_object_add(value, "download_url", string(urls[i].download_url));
                json_object_object_add(value, "expires_at",
                                       json_object_new_int64(urls[i].expires_at_us / 1000000));
                json_object_array_add(values, value);
            }
            json_object_object_add(out, "urls", values);
        }
        VgiClientUploadUrlsFree(urls, count);
        return rc;
    }
    if (!strcmp(op, "session_begin"))
        return VgiClientSessionBegin(d->client, text_field(request, "token"));
    if (!strcmp(op, "session_end")) return VgiClientSessionEnd(d->client);
    if (!strcmp(op, "session_token") || !strcmp(op, "session_detach")) {
        json_object_object_add(
            out, "token",
            string(!strcmp(op, "session_detach") ? VgiClientSessionDetach(d->client)
                                                 : VgiClientSessionToken(d->client)));
        return 0;
    }
    if (!strcmp(op, "session_echo_headers")) {
        const struct VgiClientHeader *headers = NULL;
        size_t count = VgiClientSessionEchoHeaders(d->client, &headers);
        struct json_object *values = json_object_new_object();
        for (size_t i = 0; i < count; ++i)
            json_object_object_add(values, headers[i].name, string(headers[i].value));
        json_object_object_add(out, "headers", values);
        return 0;
    }
    return EINVAL;
}
static struct json_object *dispatch(struct Driver *d, struct json_object *request) {
    const char *op = text_field(request, "op");
    if (!op) return driver_error("Missing op");
    if (!strcmp(op, "connect")) return connect_client(d, request);
    if (!strcmp(op, "shutdown")) {
        VgiClientFree(d->client);
        d->client = NULL;
        return answer(1);
    }
    if (!d->client) return driver_error("Not connected");
    struct VgiAnnotatedBatch input = {0}, result = {0};
    struct ArrowError error = {{0}};
    struct json_object *out = answer(1);
    int rc = 0, done = 0;
    if (!strcmp(op, "describe")) {
        rc = describe(d, out);
    } else if (!strcmp(op, "unary") || !strcmp(op, "stream_open")) {
        rc = decode(text_field(request, "request_b64"), &input, &error);
        struct ArrowStringView name =
            VgiMetadataGet((const char *)input.metadata.data, "vgi_rpc.method");
        if (!rc && (!name.data || !name.size_bytes)) {
            ArrowErrorSet(&error, "Missing vgi_rpc.method");
            rc = EINVAL;
        }
        char *method = !rc ? strndup(name.data, (size_t)name.size_bytes) : NULL;
        if (!rc && !method) rc = ENOMEM;
        if (!rc) {
            rc = !strcmp(op, "unary")
                     ? VgiClientUnary(d->client, method, &input, &result)
                     : VgiClientStreamOpen(d->client, method, &input,
                                           boolean(request, "is_exchange"),
                                           boolean(request, "has_header"), &result);
            if (rc)
                call_error(d, out);
            else
                rc = encode(out, !strcmp(op, "unary") ? "result_b64" : "header_b64", &result,
                            &error);
        }
        free(method);
    } else if (!strcmp(op, "tick") || !strcmp(op, "next_with_token") || !strcmp(op, "exchange")) {
        const char *encoded = text_field(request, "input_b64");
        if (encoded || !strcmp(op, "exchange")) rc = decode(encoded, &input, &error);
        if (!rc) {
            rc = !strcmp(op, "exchange")
                     ? VgiClientStreamExchange(d->client, &input, &result, &done)
                     : VgiClientStreamTick(d->client, (const char *)input.metadata.data, &result,
                                           &done);
            json_object_object_add(out, "done", json_object_new_boolean(done || rc));
            if (rc)
                call_error(d, out);
            else
                rc = encode(out, "batch_b64", &result, &error);
            if (!strcmp(op, "next_with_token"))
                json_object_object_add(out, "token", string(VgiClientStreamToken(d->client)));
        }
    } else if (!strcmp(op, "close") || !strcmp(op, "cancel")) {
        rc = VgiClientStreamClose(d->client, !strcmp(op, "cancel"));
        if (rc) call_error(d, out);
    } else {
        rc = http_op(d, op, request, out);
        if (rc == ENOTSUP)
            ArrowErrorSet(&error, "op requires http transport");
        else if (rc == EINVAL)
            ArrowErrorSet(&error, "Unknown or invalid op: %s", op);
    }
    if (rc && error.message[0]) {
        json_object_put(out);
        out = driver_error(error.message);
    }
    VgiAnnotatedBatchReset(&input);
    VgiAnnotatedBatchReset(&result);
    return out;
}
int main(void) {
    if (sodium_init() < 0) return 1;
    signal(SIGPIPE, SIG_IGN);
    int fd = dup(STDOUT_FILENO);
    if (fd < 0 || dup2(STDERR_FILENO, STDOUT_FILENO) < 0) return 1;
    FILE *control = fdopen(fd, "w");
    if (!control) return 1;
    struct Driver d = {NULL, json_object_new_array()};
    char *line = NULL;
    size_t capacity = 0;
    while (getline(&line, &capacity, stdin) >= 0) {
        struct json_object *request = json_tokener_parse(line);
        struct json_object *response =
            request ? dispatch(&d, request) : driver_error("Invalid JSON");
        json_object_object_add(response, "logs", d.logs);
        d.logs = json_object_new_array();
        fprintf(control, "%s\n", json_object_to_json_string_ext(response, JSON_C_TO_STRING_PLAIN));
        fflush(control);
        int shutdown =
            request && text_field(request, "op") && !strcmp(text_field(request, "op"), "shutdown");
        json_object_put(response);
        json_object_put(request);
        if (shutdown) break;
    }
    free(line);
    VgiClientFree(d.client);
    json_object_put(d.logs);
    fclose(control);
    return 0;
}
