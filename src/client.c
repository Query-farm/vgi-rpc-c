#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <json-c/json.h>
#include <stdlib.h>
#include <string.h>

#include "client_internal.h"
#include "client_transport.h"
#include "external_internal.h"
#include "platform.h"
#include "rpc_internal.h"
#include "shm_internal.h"
#include "telemetry_internal.h"

static char *text_copy(struct ArrowStringView value) {
    if (value.size_bytes < 0 ||
        (value.size_bytes && memchr(value.data, 0, (size_t)value.size_bytes)))
        return NULL;
    char *s = malloc((size_t)value.size_bytes + 1);
    if (s) {
        if (value.size_bytes) memcpy(s, value.data, (size_t)value.size_bytes);
        s[value.size_bytes] = 0;
    }
    return s;
}
static const char *json_text(struct json_object *value, const char *key) {
    struct json_object *field = NULL;
    return value && json_object_object_get_ex(value, key, &field) &&
                   json_object_is_type(field, json_type_string)
               ? json_object_get_string(field)
               : "";
}
void VgiAnnotatedBatchReset(struct VgiAnnotatedBatch *value) {
    VgiBatchReset(&value->batch);
    if (value->metadata.allocator.free)
        ArrowBufferReset(&value->metadata);
    else
        ArrowBufferInit(&value->metadata);
}
void VgiClientOptionsInit(struct VgiClientOptions *options) {
    memset(options, 0, sizeof(*options));
    options->timeout_seconds = 30;
    options->compression_level = 1;
    options->shared_memory_threshold = 128 * 1024;
}
void VgiClientSharedMemoryStats(const struct VgiClient *c, struct VgiShmStats *out) {
    VgiShmGetStats(c ? c->shm : NULL, out);
}
void VgiClientClearError(struct VgiClient *c) {
    free((void *)c->error.type);
    free((void *)c->error.message);
    free((void *)c->error.traceback);
    free((void *)c->error.code);
    free((void *)c->error.kind);
    free((void *)c->error.details_json);
    memset(&c->error, 0, sizeof(c->error));
    c->arrow_error.message[0] = 0;
}
int VgiClientSetError(struct VgiClient *c, const char *type, const char *code,
                      const char *message) {
    VgiClientClearError(c);
    c->error.type = strdup(type);
    c->error.message = strdup(message);
    c->error.code = strdup(code);
    c->error.kind = strdup("");
    c->error.traceback = strdup("");
    c->error.details_json = strdup("[]");
    return EIO;
}
static int failed(struct VgiClient *c, int rc) {
    if (rc) VgiTraceError(c->trace, c->error.type);
    if (rc && rc != ECANCELED) c->broken = 1;
    if (rc && !c->error.type) {
        char message[sizeof(c->arrow_error.message)];
        snprintf(message, sizeof(message), "%s",
                 c->arrow_error.message[0] ? c->arrow_error.message : strerror(rc));
        VgiClientSetError(c, "TransportError", "UNAVAILABLE", message);
    }
    return rc;
}
static void trace_start(struct VgiClient *c, const char *method, const char *type,
                        const char *metadata) {
    const char *transports[] = {"pipe", "unix", "tcp", "http"};
    c->trace = VgiTraceStart(c->options.telemetry, c->protocol, method, type,
                             transports[c->options.transport], 1, metadata,
                             VgiHttpClientDefaultHeader(c, "traceparent"),
                             VgiHttpClientDefaultHeader(c, "tracestate"));
}
static void trace_end(struct VgiClient *c, int rc) {
    if (rc) VgiTraceError(c->trace, c->error.type);
    VgiTraceEnd(c->trace, rc);
    c->trace = NULL;
}
int VgiClientReusable(struct VgiClient *c) { return VgiClientTransportReusable(c); }
const struct VgiClientError *VgiClientLastError(const struct VgiClient *c) { return &c->error; }
int VgiClientReadMetadata(struct VgiClient *c, const char *metadata) {
    struct ArrowStringView level = VgiMetadataGet(metadata, VGI_RPC_LOG_LEVEL_KEY);
    if (!level.data) return 0;
    char *l = text_copy(level),
         *message = text_copy(VgiMetadataGet(metadata, VGI_RPC_LOG_MESSAGE_KEY)),
         *extra_text = text_copy(VgiMetadataGet(metadata, VGI_RPC_LOG_EXTRA_KEY));
    if (!l || !message || !extra_text) {
        free(l);
        free(message);
        free(extra_text);
        return ENOMEM;
    }
    struct json_object *extra = *extra_text ? json_tokener_parse(extra_text) : NULL;
    int rc = 0;
    if (!strcmp(l, "EXCEPTION")) {
        VgiClientClearError(c);
        c->error.type = strdup(json_text(extra, "exception_type"));
        c->error.message = strdup(message);
        c->error.traceback = strdup(json_text(extra, "traceback"));
        c->error.code = text_copy(VgiMetadataGet(metadata, VGI_RPC_ERROR_CODE_KEY));
        c->error.kind = text_copy(VgiMetadataGet(metadata, VGI_RPC_ERROR_KIND_KEY));
        char *details = text_copy(VgiMetadataGet(metadata, VGI_RPC_ERROR_DETAILS_KEY));
        struct json_object *parsed = details && *details ? json_tokener_parse(details) : NULL;
        c->error.details_json =
            strdup(parsed && json_object_is_type(parsed, json_type_array)
                       ? json_object_to_json_string_ext(parsed, JSON_C_TO_STRING_PLAIN)
                       : "[]");
        json_object_put(parsed);
        free(details);
        rc = ECANCELED;
    } else if (c->options.on_log)
        c->options.on_log(c->options.log_state, l, message, *extra_text ? extra_text : "{}");
    json_object_put(extra);
    free(l);
    free(message);
    free(extra_text);
    return rc ? rc : 1; /* a non-data log */
}
static int resolved_external_log(void *state, const char *level, const char *message,
                                 const char *extra) {
    struct VgiClient *c = state;
    if (c->options.on_log)
        c->options.on_log(c->options.log_state, level, message, extra ? extra : "{}");
    return 0;
}
int VgiClientResolve(struct VgiClient *c, struct VgiAnnotatedBatch *batch) {
    int rc = VgiShmResolve(c->shm, &batch->batch, &batch->metadata, &c->arrow_error);
    if (!rc)
        rc = VgiExternalResolveOptions(c->external_enabled ? &c->external : NULL, &batch->batch,
                                       &batch->metadata, &c->arrow_error, resolved_external_log, c);
    if (rc)
        VgiClientSetError(c, "ExternalLocationError", "INVALID_ARGUMENT", c->arrow_error.message);
    return rc;
}
int VgiClientRequestMetadata(struct VgiClient *c, const char *method, const char *metadata,
                             struct ArrowBuffer *out) {
    int rc = ArrowMetadataBuilderInit(out, metadata);
    const char *keys[] = {"vgi_rpc.method", VGI_RPC_PROTOCOL_KEY, VGI_RPC_PROTOCOL_VERSION_KEY,
                          VGI_RPC_REQUEST_VERSION_KEY};
    int transport_options = !strcmp(method, "__transport_options__");
    const char *values[] = {method, transport_options ? NULL : c->protocol,
                            transport_options ? NULL : c->version, "1"};
    for (int i = 0; !rc && i < 4; ++i)
        if (values[i] && *values[i] && !VgiMetadataGet(metadata, keys[i]).data)
            rc = VgiMetadataSet(out, keys[i], values[i]);
    if (!rc) rc = VgiShmAdvertise(c->shm, out);
    if (!rc) rc = VgiTraceInject(c->trace, out);
    return rc;
}
static int negotiate_shm(struct VgiClient *c, struct ArrowError *error) {
    struct VgiAnnotatedBatch in = {0}, out = {0};
    int rc = VgiRpcBatchInitNamed(&in.batch, "Empty", error);
    if (!rc) rc = VgiBatchFinishRow(&in.batch, error);
    if (!rc) rc = VgiMetadataSet(&in.metadata, "vgi_rpc.transport.shm", "true");
    if (!rc) rc = VgiClientUnary(c, "__transport_options__", &in, &out);
    struct ArrowStringView capability =
        VgiMetadataGet((const char *)out.metadata.data, "vgi_rpc.transport.shm");
    int supported = !rc && !out.batch.schema.n_children && capability.size_bytes == 4 &&
                    !memcmp(capability.data, "true", 4);
    /* Older peers may reject the synthetic method; the drained application
     * error is safe to ignore. A broken connection cannot be reused. */
    if (rc == ECANCELED) {
        rc = 0;
        VgiClientClearError(c);
    }
    if (!rc && supported) {
        rc = VgiShmCreate(c->options.shared_memory_size, &c->shm, error);
        if (!rc) VgiShmSetThreshold(c->shm, c->options.shared_memory_threshold);
    }
    VgiAnnotatedBatchReset(&in);
    VgiAnnotatedBatchReset(&out);
    return rc;
}
int VgiClientConnect(const struct VgiClientOptions *options, struct VgiClient **out,
                     struct ArrowError *error) {
    if (!options || !out || *out || !options->protocol || !*options->protocol ||
        options->transport < VGI_CLIENT_STDIO || options->transport > VGI_CLIENT_HTTP)
        return EINVAL;
    if (options->shared_memory_size && options->transport != VGI_CLIENT_STDIO &&
        options->transport != VGI_CLIENT_UNIX)
        return EINVAL;
    struct VgiClient *c = calloc(1, sizeof(*c));
    if (!c) return ENOMEM;
    c->options = *options;
    c->protocol = strdup(options->protocol);
    c->version = strdup(options->protocol_version ? options->protocol_version : "");
    c->input_fd = c->output_fd = -1;
    c->socket_handle = -1;
    c->external_enabled = options->external != NULL;
    if (options->external) c->external = *options->external;
    int rc = c->protocol && c->version ? 0 : ENOMEM;
    if (!rc)
        rc = options->transport == VGI_CLIENT_HTTP ? VgiHttpClientInit(c)
                                                   : VgiClientTransportOpen(c);
    if (!rc) {
        if (options->shared_memory_size) rc = negotiate_shm(c, error);
    }
    if (rc) {
        ArrowErrorSet(error, "Cannot connect: %s",
                      c->error.message ? c->error.message : strerror(rc));
        VgiClientFree(c);
    } else
        *out = c;
    return rc;
}
void VgiClientFree(struct VgiClient *c) {
    if (!c) return;
    trace_end(c, c->streaming);
    VgiHttpClientReset(c);
    VgiIpcWriterReset(&c->writer);
    VgiIpcReaderReset(&c->reader);
    if (c->output_schema.release) ArrowSchemaRelease(&c->output_schema);
    VgiClientTransportClose(c);
    VgiShmFree(c->shm);
    VgiClientClearError(c);
    free(c->protocol);
    free(c->version);
    free(c);
}
static int request(struct VgiClient *c, const char *method, const struct VgiAnnotatedBatch *input) {
    struct ArrowBuffer metadata;
    ArrowBufferInit(&metadata);
    int rc = VgiClientRequestMetadata(c, method, (const char *)input->metadata.data, &metadata);
    struct ArrowIpcOutputStream target = {0};
    struct VgiIpcWriter writer = {0};
    if (!rc) rc = VgiClientOutput(c, &target);
    if (!rc) rc = VgiIpcWriterInit(&writer, &input->batch.schema, &target, &c->arrow_error);
    if (!rc)
        rc = VgiShmWrite(c->shm, &writer, &input->batch.array, (const char *)metadata.data,
                         &c->arrow_error);
    if (!rc) rc = VgiIpcWriterFinish(&writer, &c->arrow_error);
    VgiIpcWriterReset(&writer);
    if (target.release) target.release(&target);
    ArrowBufferReset(&metadata);
    return rc;
}
static int reader_init(struct VgiClient *c, struct VgiIpcReader *reader,
                       struct ArrowSchema *schema) {
    struct ArrowIpcInputStream source = {0};
    int rc = VgiClientInput(c, &source);
    if (!rc) rc = VgiIpcReaderInit(reader, &source);
    if (!rc) rc = VgiIpcReaderSchema(reader, schema, &c->arrow_error);
    if (source.release) source.release(&source);
    return rc;
}
static int read_next(struct VgiClient *c, struct VgiIpcReader *reader,
                     const struct ArrowSchema *schema, struct VgiAnnotatedBatch *out, int *done) {
    int rc = 0;
    *done = 0;
    while (!rc) {
        struct VgiAnnotatedBatch value = {0};
        ArrowBufferInit(&value.metadata);
        rc = VgiIpcReaderNext(reader, &value.batch.array, &value.metadata, &c->arrow_error);
        if (!rc && !value.batch.array.release) {
            VgiAnnotatedBatchReset(&value);
            *done = 1;
            break;
        }
        if (!rc) rc = ArrowSchemaDeepCopy(schema, &value.batch.schema);
        if (!rc) rc = VgiClientResolve(c, &value);
        int kind = !rc ? VgiClientReadMetadata(c, (const char *)value.metadata.data) : 0;
        if (kind > 1) rc = kind;
        if (!rc && !kind) {
            *out = value;
            memset(&value, 0, sizeof(value));
            return 0;
        }
        VgiAnnotatedBatchReset(&value);
    }
    return rc;
}
static int substream(struct VgiClient *c, struct VgiAnnotatedBatch *out) {
    struct VgiIpcReader reader = {0};
    struct ArrowSchema schema = {0};
    int rc = reader_init(c, &reader, &schema), done = 0, pending = 0;
    while (!rc && !done) {
        struct VgiAnnotatedBatch value = {0};
        ArrowBufferInit(&value.metadata);
        rc = read_next(c, &reader, &schema, &value, &done);
        if (rc == ECANCELED) {
            pending = rc;
            rc = 0;
        }
        if (!rc && value.batch.array.release) {
            if (out->batch.array.release) {
                pending = EINVAL;
            } else {
                *out = value;
                memset(&value, 0, sizeof(value));
            }
        }
        VgiAnnotatedBatchReset(&value);
    }
    VgiIpcReaderReset(&reader);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc ? rc : pending;
}
int VgiClientUnary(struct VgiClient *c, const char *method, const struct VgiAnnotatedBatch *input,
                   struct VgiAnnotatedBatch *out) {
    VgiClientClearError(c);
    if (c->streaming)
        return VgiClientSetError(c, "ProtocolError", "FAILED_PRECONDITION",
                                 "Stream is already open");
    trace_start(c, method, "unary", (const char *)input->metadata.data);
    int rc;
    if (c->http)
        rc = VgiHttpClientUnary(c, method, input, out);
    else {
        rc = request(c, method, input);
        if (!rc) rc = substream(c, out);
    }
    if (!rc && !out->batch.array.release) rc = EINVAL;
    if (!rc || rc == ECANCELED) VgiShmReset(c->shm);
    trace_end(c, rc);
    return failed(c, rc);
}
static int writer_init(struct VgiClient *c, const struct ArrowSchema *schema) {
    if (c->writer.writer.private_data) return 0;
    struct ArrowIpcOutputStream target = {0};
    int rc = VgiClientOutput(c, &target);
    if (!rc) rc = VgiIpcWriterInit(&c->writer, schema, &target, &c->arrow_error);
    if (target.release) target.release(&target);
    return rc;
}
int VgiClientStreamOpen(struct VgiClient *c, const char *method,
                        const struct VgiAnnotatedBatch *input, int exchange, int has_header,
                        struct VgiAnnotatedBatch *header) {
    VgiClientClearError(c);
    if (c->streaming)
        return VgiClientSetError(c, "ProtocolError", "FAILED_PRECONDITION",
                                 "Stream is already open");
    trace_start(c, method, exchange ? "exchange" : "producer", (const char *)input->metadata.data);
    if (c->http) {
        int rc = VgiHttpClientOpen(c, method, input, exchange, has_header, header);
        if (rc || !c->streaming) trace_end(c, rc);
        return failed(c, rc);
    }
    int rc = request(c, method, input);
    c->streaming = !rc;
    c->exchange = exchange;
    c->eos = 0;
    if (!rc && has_header) {
        rc = substream(c, header);
        if (!rc && !header->batch.array.release) rc = EINVAL;
        if (rc) {
            /* Initializer errors still require the client's input substream EOS. */
            struct ArrowSchema empty = {0};
            if (!VgiSchemaRpcEmpty(&empty, &c->arrow_error) && !writer_init(c, &empty)) {
                VgiIpcWriterFinish(&c->writer, &c->arrow_error);
            }
            if (empty.release) ArrowSchemaRelease(&empty);
            VgiIpcWriterReset(&c->writer);
            c->streaming = 0;
        }
    }
    if (rc) trace_end(c, rc);
    return failed(c, rc);
}
static int step(struct VgiClient *c, const struct VgiAnnotatedBatch *input,
                struct VgiAnnotatedBatch *out, int *done) {
    if (c->http) {
        int rc = VgiHttpClientStep(c, input, out, done);
        if (rc || *done || !c->streaming) trace_end(c, rc);
        return failed(c, rc);
    }
    int rc = writer_init(c, &input->batch.schema);
    if (!rc)
        rc = VgiShmWrite(c->shm, &c->writer, &input->batch.array,
                         (const char *)input->metadata.data, &c->arrow_error);
    if (!rc && !c->reader.private_data) rc = reader_init(c, &c->reader, &c->output_schema);
    if (!rc) rc = read_next(c, &c->reader, &c->output_schema, out, done);
    if (rc || *done) {
        if (rc) VgiTraceError(c->trace, c->error.type);
        if (*done) c->eos = 1;
        int closed = VgiClientStreamClose(c, 0);
        if (!rc) rc = closed;
    }
    return failed(c, rc);
}
int VgiClientStreamTick(struct VgiClient *c, const char *metadata, struct VgiAnnotatedBatch *out,
                        int *done) {
    VgiClientClearError(c);
    *done = 0;
    if (!c->streaming) {
        *done = 1;
        return 0;
    }
    if (c->exchange)
        return VgiClientSetError(c, "ProtocolError", "INVALID_ARGUMENT",
                                 "Tick requires a producer");
    struct VgiAnnotatedBatch tick = {0};
    ArrowBufferInit(&tick.metadata);
    struct ArrowSchema schema = {0};
    int rc = VgiSchemaRpcEmpty(&schema, &c->arrow_error);
    if (!rc) rc = VgiBatchInit(&tick.batch, &schema, &c->arrow_error);
    if (!rc) rc = VgiBatchFinish(&tick.batch, &c->arrow_error);
    if (!rc) rc = ArrowMetadataBuilderInit(&tick.metadata, metadata);
    if (!rc) rc = step(c, &tick, out, done);
    VgiAnnotatedBatchReset(&tick);
    if (schema.release) ArrowSchemaRelease(&schema);
    return failed(c, rc);
}
int VgiClientStreamExchange(struct VgiClient *c, const struct VgiAnnotatedBatch *input,
                            struct VgiAnnotatedBatch *out, int *done) {
    VgiClientClearError(c);
    *done = 0;
    if (!c->streaming || !c->exchange)
        return VgiClientSetError(c, "ProtocolError", "FAILED_PRECONDITION", "Exchange is not open");
    return step(c, input, out, done);
}
int VgiClientStreamClose(struct VgiClient *c, int cancel) {
    if (!c->streaming) return 0;
    if (cancel) VgiTraceError(c->trace, "Cancelled");
    if (c->http) {
        int rc = VgiHttpClientClose(c, cancel);
        trace_end(c, rc);
        return failed(c, rc);
    }
    struct ArrowSchema empty = {0};
    int rc = 0;
    if (!c->writer.writer.private_data) {
        rc = VgiSchemaRpcEmpty(&empty, &c->arrow_error);
        if (!rc) rc = writer_init(c, &empty);
    }
    if (!rc && cancel && !c->eos) {
        struct VgiAnnotatedBatch value = {0};
        ArrowBufferInit(&value.metadata);
        rc = VgiBatchInit(&value.batch, &c->writer.schema, &c->arrow_error);
        if (!rc) rc = VgiBatchFinish(&value.batch, &c->arrow_error);
        if (!rc) rc = VgiMetadataSet(&value.metadata, VGI_RPC_CANCEL_KEY, "1");
        if (!rc)
            rc = VgiIpcWriterNext(&c->writer, &value.batch.array, (const char *)value.metadata.data,
                                  &c->arrow_error);
        VgiAnnotatedBatchReset(&value);
    }
    if (!rc) rc = VgiIpcWriterFinish(&c->writer, &c->arrow_error);
    if (!rc && !c->eos && !c->reader.private_data)
        rc = reader_init(c, &c->reader, &c->output_schema);
    int pending = 0;
    while (!rc && !c->eos) {
        struct VgiAnnotatedBatch discarded = {0};
        ArrowBufferInit(&discarded.metadata);
        rc = read_next(c, &c->reader, &c->output_schema, &discarded, &c->eos);
        VgiAnnotatedBatchReset(&discarded);
        if (rc == ECANCELED) {
            pending = rc;
            rc = 0;
        }
    }
    if (empty.release) ArrowSchemaRelease(&empty);
    VgiIpcWriterReset(&c->writer);
    VgiIpcReaderReset(&c->reader);
    if (c->output_schema.release) ArrowSchemaRelease(&c->output_schema);
    c->streaming = 0;
    if (!rc) VgiShmReset(c->shm);
    trace_end(c, rc ? rc : pending);
    return failed(c, rc ? rc : pending);
}

static int reflection_call(struct VgiClient *c, const char *method, const char *protocol,
                           struct VgiAnnotatedBatch *out) {
    struct ArrowSchema schema = {0};
    struct VgiAnnotatedBatch input = {0}, reply = {0};
    ArrowBufferInit(&input.metadata);
    int rc = protocol ? VgiSchemaRpcDescribeParams(&schema, &c->arrow_error)
                      : VgiSchemaRpcListProtocolsParams(&schema, &c->arrow_error);
    if (!rc) rc = VgiBatchInit(&input.batch, &schema, &c->arrow_error);
    if (!rc && protocol) rc = VgiBatchSetString(&input.batch, "protocol", protocol);
    if (!rc)
        rc = schema.n_children ? VgiBatchFinishRow(&input.batch, &c->arrow_error)
                               : VgiBatchFinish(&input.batch, &c->arrow_error);
    if (!rc) rc = VgiMetadataSet(&input.metadata, VGI_RPC_PROTOCOL_KEY, VGI_REFLECTION_NAME);
    if (!rc)
        rc = VgiMetadataSet(&input.metadata, VGI_RPC_PROTOCOL_VERSION_KEY, VGI_REFLECTION_VERSION);
    if (!rc) rc = VgiClientUnary(c, method, &input, &reply);
    if (!rc) rc = VgiBatchView(&reply.batch, &c->arrow_error);
    const struct ArrowArrayView *value = !rc ? VgiBatchGet(&reply.batch, "result") : NULL;
    if (!rc && (!value || reply.batch.array.length != 1)) rc = EINVAL;
    if (!rc) {
        ArrowBufferInit(&out->metadata);
        rc = VgiIpcReadBatch(ArrowArrayViewGetBytesUnsafe(value, 0), &out->batch.schema,
                             &out->batch.array, &out->metadata, &c->arrow_error);
    }
    if (!rc) rc = VgiBatchView(&out->batch, &c->arrow_error);
    VgiAnnotatedBatchReset(&input);
    VgiAnnotatedBatchReset(&reply);
    if (schema.release) ArrowSchemaRelease(&schema);
    return failed(c, rc);
}
int VgiClientDescribe(struct VgiClient *c, struct VgiAnnotatedBatch *listing,
                      struct VgiAnnotatedBatch *description) {
    int rc = reflection_call(c, "list_protocols", NULL, listing);
    const struct ArrowArrayView *list = !rc ? VgiBatchGet(&listing->batch, "protocols") : NULL;
    char *protocol = NULL;
    if (!rc && (!list || list->n_children != 1 || listing->batch.array.length != 1)) rc = EINVAL;
    if (!rc) {
        int64_t index = VgiFieldIndex(
            listing->batch.schema.children[VgiFieldIndex(&listing->batch.schema, "protocols")]
                ->children[0],
            "protocol");
        if (index < 0)
            rc = EINVAL;
        else
            for (int64_t i = ArrowArrayViewListChildOffset(list, 0);
                 i < ArrowArrayViewListChildOffset(list, 1); ++i) {
                struct ArrowStringView name =
                    ArrowArrayViewGetStringUnsafe(list->children[0]->children[index], i);
                if (name.size_bytes < 8 || memcmp(name.data, "vgi_rpc.", 8)) {
                    protocol = text_copy(name);
                    break;
                }
            }
        if (!protocol)
            rc = VgiClientSetError(c, "ProtocolError", "NOT_FOUND",
                                   "Server hosts no application protocol");
    }
    if (!rc) rc = reflection_call(c, "describe", protocol, description);
    free(protocol);
    return failed(c, rc);
}
