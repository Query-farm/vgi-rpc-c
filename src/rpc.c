#include <errno.h>
#include <inttypes.h>
#include <json-c/json.h>
#include <pthread.h>
#include <sodium.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "external_internal.h"
#include "rpc_internal.h"
#include "shm_internal.h"
#include "telemetry_internal.h"

struct ManagedState {
    void *state;
    void (*release)(void *);
    struct ManagedState *next;
};
struct VgiRpcServer {
    struct VgiTelemetry *telemetry;
    struct VgiExternalOptions external;
    int external_enabled;
    struct ManagedState *managed;
    _Atomic int serving;
    struct VgiRpcProtocol *protocols;
    size_t count;
    char id[13];
    int include_tracebacks;
    enum VgiTransportKind transport_kind;
    pthread_mutex_t start_mutex;
    int started;
    int (*on_start)(void *, enum VgiTransportKind, struct ArrowError *);
    void *start_state;
};

static char *copy_text(const char *s) {
    if (!s) s = "";
    size_t n = strlen(s) + 1;
    char *out = malloc(n);
    if (out) memcpy(out, s, n);
    return out;
}
static int text_equals(struct ArrowStringView value, const char *text) {
    return value.size_bytes == (int64_t)strlen(text) &&
           !memcmp(value.data, text, (size_t)value.size_bytes);
}
static char *view_text(struct ArrowStringView value) {
    char *out = malloc((size_t)value.size_bytes + 1);
    if (out) {
        memcpy(out, value.data, (size_t)value.size_bytes);
        out[value.size_bytes] = 0;
    }
    return out;
}
int VgiMetadataSet(struct ArrowBuffer *metadata, const char *key, const char *value) {
    if (!metadata->data) NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderInit(metadata, NULL));
    return ArrowMetadataBuilderSet(metadata, ArrowCharView(key), ArrowCharView(value));
}
struct ArrowStringView VgiMetadataGet(const char *metadata, const char *key) {
    struct ArrowStringView value = {NULL, 0};
    if (metadata && ArrowMetadataGetValue(metadata, ArrowCharView(key), &value))
        value = (struct ArrowStringView){NULL, 0};
    return value;
}
struct VgiRpcLogEntry {
    struct ArrowBuffer metadata;
    struct VgiRpcLogEntry *next;
};
static void clear_logs(struct VgiRpcCall *call) {
    while (call->logs) {
        struct VgiRpcLogEntry *entry = call->logs;
        call->logs = entry->next;
        ArrowBufferReset(&entry->metadata);
        free(entry);
    }
    call->logs_tail = NULL;
}
void VgiRpcDrainLogs(struct VgiRpcCall *call, void (*receive)(void *, const char *), void *state) {
    for (struct VgiRpcLogEntry *entry = call->logs; entry; entry = entry->next)
        if (receive) receive(state, (const char *)entry->metadata.data);
    clear_logs(call);
}
int VgiRpcLogExtra(struct VgiRpcCall *call, const char *level, const char *message,
                   const char *extra_json) {
    if (!call || !level || !message) return EINVAL;
    struct VgiRpcLogEntry *entry = calloc(1, sizeof(*entry));
    if (!entry) return ENOMEM;
    ArrowBufferInit(&entry->metadata);
    int rc = VgiMetadataSet(&entry->metadata, VGI_RPC_LOG_LEVEL_KEY, level);
    if (!rc) rc = VgiMetadataSet(&entry->metadata, VGI_RPC_LOG_MESSAGE_KEY, message);
    if (!rc) rc = VgiMetadataSet(&entry->metadata, VGI_RPC_REQUEST_ID_KEY, call->request_id);
    if (!rc) rc = VgiMetadataSet(&entry->metadata, VGI_RPC_SERVER_ID_KEY, call->server->id);
    if (rc) {
        ArrowBufferReset(&entry->metadata);
        free(entry);
        return rc;
    }
    if (extra_json) {
        rc = VgiMetadataSet(&entry->metadata, VGI_RPC_LOG_EXTRA_KEY, extra_json);
        if (rc) {
            ArrowBufferReset(&entry->metadata);
            free(entry);
            return rc;
        }
    }
    struct VgiTraceContext trace;
    VgiTraceContextCurrent(&trace);
    if (trace.traceparent[0]) {
        struct json_object *extra = extra_json ? json_tokener_parse(extra_json) : NULL;
        if (!extra || !json_object_is_type(extra, json_type_object)) {
            json_object_put(extra);
            extra = json_object_new_object();
        }
        json_object_object_add(extra, "trace_id",
                               json_object_new_string_len(trace.traceparent + 3, 32));
        json_object_object_add(extra, "span_id",
                               json_object_new_string_len(trace.traceparent + 36, 16));
        VgiMetadataSet(&entry->metadata, VGI_RPC_LOG_EXTRA_KEY,
                       json_object_to_json_string_ext(extra, JSON_C_TO_STRING_PLAIN));
        json_object_put(extra);
    }
    if (call->logs_tail)
        call->logs_tail->next = entry;
    else
        call->logs = entry;
    call->logs_tail = entry;
    return 0;
}
int VgiRpcWriteLogs(struct VgiRpcCall *call, struct VgiIpcWriter *writer,
                    const struct ArrowSchema *schema, struct ArrowError *error) {
    if (!call || !call->logs) return 0;
    struct VgiBatch empty = {0};
    int rc = VgiBatchInit(&empty, schema, error);
    if (!rc) rc = VgiBatchFinish(&empty, error);
    for (struct VgiRpcLogEntry *entry = call->logs; !rc && entry; entry = entry->next)
        rc = VgiIpcWriterNext(writer, &empty.array, (const char *)entry->metadata.data, error);
    VgiBatchReset(&empty);
    clear_logs(call);
    return rc;
}
int VgiRpcFail(struct VgiRpcCall *call, const char *type, const char *code, const char *kind,
               const char *message) {
    const char *values[] = {type ? type : "RuntimeError", code ? code : "UNKNOWN",
                            kind ? kind : ""};
    size_t lengths[3], total = 0;
    for (int i = 0; i < 3; ++i) {
        lengths[i] = strlen(values[i]) + 1;
        if (lengths[i] > SIZE_MAX - total) return ENOMEM;
        total += lengths[i];
    }
    char *owned = malloc(total);
    if (!owned) return ENOMEM;
    size_t offset = 0;
    for (int i = 0; i < 3; ++i) {
        memcpy(owned + offset, values[i], lengths[i]);
        offset += lengths[i];
    }
    free(call->error_storage);
    call->error_storage = owned;
    call->error_type = owned;
    call->error_code = owned + lengths[0];
    call->error_kind = owned + lengths[0] + lengths[1];
    ArrowErrorSet(&call->error, "%s", message);
    return EINVAL;
}
int VgiRpcSetErrorDetails(struct VgiRpcCall *call, const char *json) {
    if (!call || !json) return EINVAL;
    size_t length = strlen(json);
    if (length > 1024 * 1024) return EOVERFLOW;
    struct json_tokener *parser = json_tokener_new_ex(64);
    if (!parser) return ENOMEM;
    json_tokener_set_flags(parser, JSON_TOKENER_STRICT | JSON_TOKENER_VALIDATE_UTF8);
    struct json_object *details = json_tokener_parse_ex(parser, json, (int)length + 1);
    int valid = json_tokener_get_error(parser) == json_tokener_success &&
                json_object_is_type(details, json_type_array);
    json_tokener_free(parser);
    const char *catalog[] = {"vgi_rpc.ErrorInfo",    "vgi_rpc.RetryInfo",
                             "vgi_rpc.BadRequest",   "vgi_rpc.PreconditionFailure",
                             "vgi_rpc.QuotaFailure", "vgi_rpc.ResourceInfo",
                             "vgi_rpc.Help",         "vgi_rpc.LocalizedMessage"};
    for (size_t i = 0; valid && i < json_object_array_length(details); ++i) {
        struct json_object *item = json_object_array_get_idx(details, i), *type = NULL;
        valid = json_object_is_type(item, json_type_object) &&
                json_object_object_get_ex(item, "@type", &type) &&
                json_object_is_type(type, json_type_string);
        if (!valid) break;
        const char *name = json_object_get_string(type);
        valid =
            strchr(name, '.') != NULL && strlen(name) == (size_t)json_object_get_string_len(type);
        if (!strncmp(name, "vgi_rpc.", 8)) {
            int known = 0;
            for (size_t k = 0; k < sizeof(catalog) / sizeof(*catalog); ++k)
                known |= !strcmp(name, catalog[k]);
            valid &= known;
        }
        for (size_t j = 0; valid && j < i; ++j) {
            struct json_object *other = NULL;
            json_object_object_get_ex(json_object_array_get_idx(details, j), "@type", &other);
            if (!strcmp(name, json_object_get_string(other))) valid = 0;
        }
    }
    int rc = valid ? 0 : EINVAL;
    char *owned = NULL;
    if (valid && json_object_array_length(details)) {
        const char *text = json_object_to_json_string_ext(details, JSON_C_TO_STRING_PLAIN);
        if (strlen(text) <= 4096) {
            owned = copy_text(text);
            if (!owned) rc = ENOMEM;
        }
    }
    if (details) json_object_put(details);
    if (!rc) {
        free(call->error_details);
        call->error_details = owned;
    }
    return rc;
}
static int reflection(void *state, struct VgiRpcCall *call, struct VgiRpcReply *reply);

struct VgiRpcServer *VgiRpcServerNew(void) {
    if (sodium_init() < 0) return NULL;
    struct VgiRpcServer *server = calloc(1, sizeof(*server));
    if (!server) return NULL;
    server->include_tracebacks = 1;
    pthread_mutex_init(&server->start_mutex, NULL);
    unsigned char random[6];
    randombytes_buf(random, sizeof(random));
    sodium_bin2hex(server->id, sizeof(server->id), random, sizeof(random));
    return server;
}
void VgiRpcServerFree(struct VgiRpcServer *server) {
    if (!server) return;
    for (size_t i = 0; i < server->count; ++i) {
        free((void *)server->protocols[i].name);
        free((void *)server->protocols[i].version);
        free((void *)server->protocols[i].hash);
    }
    while (server->managed) {
        struct ManagedState *m = server->managed;
        server->managed = m->next;
        m->release(m->state);
        free(m);
    }
    free(server->protocols);
    pthread_mutex_destroy(&server->start_mutex);
    free(server);
}
void VgiRpcServerSetTracebacks(struct VgiRpcServer *server, int enabled) {
    if (server) server->include_tracebacks = enabled != 0;
}
static const struct VgiRpcProtocol *find_protocol(struct VgiRpcServer *server, const char *name) {
    for (size_t i = 0; i < server->count; ++i)
        if (!strcmp(server->protocols[i].name, name)) return &server->protocols[i];
    return NULL;
}
static struct VgiRpcProtocol reflection_protocol(struct VgiRpcServer *server) {
    return (struct VgiRpcProtocol){VGI_REFLECTION_NAME,
                                   "",
                                   VGI_REFLECTION_HASH,
                                   VGI_REFLECTION_METHODS,
                                   VGI_REFLECTION_METHOD_COUNT,
                                   reflection,
                                   server,
                                   NULL};
}
static int add_protocol(struct VgiRpcServer *server, const struct VgiRpcProtocol *protocol,
                        struct ArrowError *error, int framework) {
    if (!server || !protocol || !protocol->name || !*protocol->name || !protocol->handler ||
        !protocol->methods || !protocol->hash)
        return EINVAL;
    if (server->serving) {
        ArrowErrorSet(error, "Configure protocols before serving");
        return EBUSY;
    }
    if ((!framework && !strncmp(protocol->name, "vgi_rpc.", 8)) ||
        find_protocol(server, protocol->name)) {
        ArrowErrorSet(error, "Duplicate or reserved protocol name: %s", protocol->name);
        return EINVAL;
    }
    for (const char *p = protocol->name; *p; ++p)
        if (!(('A' <= *p && *p <= 'Z') || ('a' <= *p && *p <= 'z') || ('0' <= *p && *p <= '9') ||
              *p == '.' || *p == '_' || *p == '-'))
            return EINVAL;
    struct VgiRpcProtocol owned = *protocol;
    owned.name = copy_text(protocol->name);
    owned.version = copy_text(protocol->version);
    owned.hash = copy_text(protocol->hash);
    struct VgiRpcProtocol *next = realloc(server->protocols, (server->count + 1) * sizeof(*next));
    if (!next || !owned.name || !owned.version || !owned.hash) {
        if (next) server->protocols = next;
        free((void *)owned.name);
        free((void *)owned.version);
        free((void *)owned.hash);
        return ENOMEM;
    }
    server->protocols = next;
    server->protocols[server->count++] = owned;
    return 0;
}

int VgiRpcServerAddProtocol(struct VgiRpcServer *s, const struct VgiRpcProtocol *p,
                            struct ArrowError *e) {
    return add_protocol(s, p, e, 0);
}
int VgiRpcServerAddManagedProtocol(struct VgiRpcServer *s, const struct VgiRpcProtocol *p,
                                   void (*release)(void *), struct ArrowError *e) {
    struct ManagedState *m = calloc(1, sizeof(*m));
    if (!m) return ENOMEM;
    int rc = add_protocol(s, p, e, 1);
    if (rc) {
        free(m);
        return rc;
    }
    m->state = p->state;
    m->release = release;
    m->next = s->managed;
    s->managed = m;
    return 0;
}
void VgiRpcServerFreeze(struct VgiRpcServer *s) { s->serving = 1; }

int VgiRpcReplyPayload(struct VgiRpcCall *call, struct VgiRpcReply *reply,
                       const struct VgiBatch *payload) {
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    struct ArrowSchema schema = {0};
    int rc = call->method->result ? call->method->result(&schema, &call->error) : EINVAL;
    if (!rc) rc = VgiBatchInit(&reply->value, &schema, &call->error);
    if (!rc) rc = VgiIpcWriteBatch(&payload->schema, &payload->array, NULL, &bytes, &call->error);
    if (!rc) rc = VgiBatchSetBytes(&reply->value, "result", bytes.data, bytes.size_bytes);
    if (!rc) rc = VgiBatchFinishRow(&reply->value, &call->error);
    ArrowBufferReset(&bytes);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}

static int append_summary(struct ArrowArray *row, const struct VgiRpcProtocol *protocol) {
    NANOARROW_RETURN_NOT_OK(VgiArrayAppendString(row->children[0], ArrowCharView(protocol->name)));
    NANOARROW_RETURN_NOT_OK(
        VgiArrayAppendString(row->children[1], ArrowCharView(protocol->version)));
    NANOARROW_RETURN_NOT_OK(VgiArrayAppendString(row->children[2], ArrowCharView(protocol->hash)));
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(row->children[3], 0));
    NANOARROW_RETURN_NOT_OK(VgiArrayAppendString(row->children[4], ArrowCharView("")));
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(row->children[5]));
    return ArrowArrayFinishElement(row);
}
static int append_schema(struct ArrowArray *column, VgiSchemaFactory factory,
                         struct ArrowError *error) {
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    struct ArrowSchema schema = {0};
    int rc = factory ? factory(&schema, error) : 0;
    if (!rc && factory) rc = VgiIpcWriteSchema(&schema, &bytes, error);
    if (!rc)
        rc =
            ArrowArrayAppendBytes(column, (struct ArrowBufferView){{bytes.data}, bytes.size_bytes});
    ArrowBufferReset(&bytes);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
static int reflection(void *state, struct VgiRpcCall *call, struct VgiRpcReply *reply) {
    struct VgiRpcServer *server = state;
    struct VgiRpcProtocol self = reflection_protocol(server);
    struct VgiBatch payload = {0};
    int rc;
    if (!strcmp(call->method->name, "list_protocols")) {
        rc = VgiRpcBatchInitNamed(&payload, "RpcProtocolList", &call->error);
        if (!rc) rc = VgiBatchSetString(&payload, "server_id", server->id);
        if (!rc) rc = VgiBatchSetString(&payload, "server_version", "vgi-rpc-c/0.1.0");
        if (!rc) rc = VgiBatchSetString(&payload, "request_version", "1");
        struct ArrowArray *list = rc ? NULL : VgiBatchColumn(&payload, "protocols");
        for (size_t i = 0; !rc && i < server->count; ++i)
            rc = append_summary(list->children[0], &server->protocols[i]);
        if (!rc) rc = append_summary(list->children[0], &self);
        if (!rc) rc = ArrowArrayFinishElement(list);
    } else {
        char *name = view_text(VgiBatchString(&call->request, "protocol"));
        if (!name) return ENOMEM;
        const struct VgiRpcProtocol *protocol =
            !strcmp(name, self.name) ? &self : find_protocol(server, name);
        free(name);
        if (!protocol)
            return VgiRpcFail(call, "ProtocolNotSupportedError", "UNIMPLEMENTED",
                              "protocol_not_supported", "Requested protocol is not hosted");
        rc = VgiRpcBatchInitNamed(&payload, "RpcServiceDescription", &call->error);
        if (!rc) rc = VgiBatchSetString(&payload, "protocol", protocol->name);
        if (!rc) rc = VgiBatchSetString(&payload, "protocol_version", protocol->version);
        if (!rc) rc = VgiBatchSetString(&payload, "protocol_hash", protocol->hash);
        struct ArrowArray *list = rc ? NULL : VgiBatchColumn(&payload, "methods");
        for (size_t i = 0; !rc && i < protocol->method_count; ++i) {
            const struct VgiMethod *method = &protocol->methods[i];
            struct ArrowArray *row = list->children[0];
            rc = VgiArrayAppendString(row->children[0], ArrowCharView(method->name));
            if (!rc)
                rc = VgiArrayAppendString(
                    row->children[1],
                    ArrowCharView(method->kind == VGI_METHOD_STREAM ? "stream" : "unary"));
            if (!rc) rc = ArrowArrayAppendInt(row->children[2], method->kind == VGI_METHOD_UNARY);
            if (!rc) rc = ArrowArrayAppendInt(row->children[3], method->header != NULL);
            if (!rc)
                rc = VgiArrayAppendString(
                    row->children[4],
                    ArrowCharView(method->kind != VGI_METHOD_STREAM            ? ""
                                  : method->stream_kind == VGI_STREAM_PRODUCER ? "producer"
                                  : method->stream_kind == VGI_STREAM_EXCHANGE ? "exchange"
                                                                               : "unknown"));
            if (!rc) rc = append_schema(row->children[5], method->params, &call->error);
            if (!rc) rc = append_schema(row->children[6], method->result, &call->error);
            if (!rc) rc = append_schema(row->children[7], method->header, &call->error);
            if (!rc) rc = VgiArrayAppendString(row->children[8], ArrowCharView("unknown"));
            if (!rc) rc = ArrowArrayAppendInt(row->children[9], 0);
            if (!rc) rc = VgiArrayAppendString(row->children[10], ArrowCharView(""));
            if (!rc) rc = ArrowArrayFinishElement(row);
        }
        if (!rc) rc = ArrowArrayFinishElement(list);
    }
    if (!rc) rc = VgiBatchFinishRow(&payload, &call->error);
    if (!rc) rc = VgiRpcReplyPayload(call, reply, &payload);
    VgiBatchReset(&payload);
    return rc;
}

static int write_batch(FILE *output, const struct VgiBatch *batch, const char *metadata,
                       struct VgiRpcCall *call, struct ArrowError *error, int use_shm) {
    struct ArrowIpcOutputStream target = {0};
    struct VgiIpcWriter writer = {0};
    int rc = ArrowIpcOutputStreamInitFile(&target, output, 0);
    if (!rc) rc = VgiIpcWriterInit(&writer, &batch->schema, &target, error);
    if (!rc) rc = VgiRpcWriteLogs(call, &writer, &batch->schema, error);
    if (!rc) rc = VgiShmWrite(use_shm ? call->shm : NULL, &writer, &batch->array, metadata, error);
    if (!rc) rc = VgiIpcWriterFinish(&writer, error);
    if (!rc && fflush(output)) rc = EIO;
    VgiIpcWriterReset(&writer);
    if (target.release) target.release(&target);
    return rc;
}
int VgiRpcErrorMetadata(struct VgiRpcCall *call, struct ArrowBuffer *metadata) {
    const char *type = call->error_type ? call->error_type : "RuntimeError";
    const char *code = call->error_code ? call->error_code : "INTERNAL";
    const char *kind = call->error_kind ? call->error_kind : "";
    struct json_object *extra = json_object_new_object();
    if (!extra) return ENOMEM;
    json_object_object_add(extra, "exception_type", json_object_new_string(type));
    json_object_object_add(extra, "error_code", json_object_new_string(code));
    json_object_object_add(extra, "error_kind", json_object_new_string(kind));
    char traceback[1536] = {0};
    if (call->server->include_tracebacks)
        snprintf(traceback, sizeof(traceback), "at %s/%s\n%s: %s",
                 call->protocol ? call->protocol : "rpc",
                 call->method ? call->method->name : "dispatch", type, call->error.message);
    json_object_object_add(extra, "traceback", json_object_new_string(traceback));
    if (call->error_details)
        json_object_object_add(extra, "error_details", json_tokener_parse(call->error_details));
    int rc = VgiMetadataSet(metadata, VGI_RPC_LOG_LEVEL_KEY, "EXCEPTION");
    if (!rc && call->error_details)
        rc = VgiMetadataSet(metadata, VGI_RPC_ERROR_DETAILS_KEY, call->error_details);
    if (!rc) rc = VgiMetadataSet(metadata, VGI_RPC_LOG_MESSAGE_KEY, call->error.message);
    if (!rc)
        rc = VgiMetadataSet(metadata, VGI_RPC_LOG_EXTRA_KEY,
                            json_object_to_json_string_ext(extra, JSON_C_TO_STRING_PLAIN));
    if (!rc) rc = VgiMetadataSet(metadata, VGI_RPC_ERROR_CODE_KEY, code);
    if (!rc && *kind) rc = VgiMetadataSet(metadata, VGI_RPC_ERROR_KIND_KEY, kind);
    if (!rc) rc = VgiMetadataSet(metadata, VGI_RPC_SERVER_ID_KEY, call->server->id);
    if (!rc) rc = VgiMetadataSet(metadata, VGI_RPC_REQUEST_ID_KEY, call->request_id);
    json_object_put(extra);
    return rc;
}
static int write_error(FILE *output, struct VgiRpcCall *call, struct ArrowError *error) {
    struct VgiBatch empty = {0};
    struct ArrowBuffer metadata;
    ArrowBufferInit(&metadata);
    int rc = VgiRpcBatchInitNamed(&empty, "Empty", error);
    if (!rc) rc = VgiBatchFinish(&empty, error);
    if (!rc) rc = VgiRpcErrorMetadata(call, &metadata);
    if (!rc) rc = write_batch(output, &empty, (const char *)metadata.data, call, error, 0);
    VgiBatchReset(&empty);
    ArrowBufferReset(&metadata);
    return rc;
}

static int semver(const char *text, uint64_t out[3]) {
    for (int i = 0; i < 3; ++i) {
        out[i] = 0;
        const char *start = text;
        while (*text >= '0' && *text <= '9') {
            if (out[i] > (UINT64_MAX - 9) / 10) return EINVAL;
            out[i] = out[i] * 10 + (unsigned)(*text++ - '0');
        }
        if (text == start || (text - start > 1 && *start == '0')) return EINVAL;
        if (i < 2 && *text++ != '.') return EINVAL;
    }
    return *text ? EINVAL : 0;
}
static int validate_version(struct VgiRpcCall *call, const char *version) {
    if (!version || !*version) return 0;
    char *client =
        view_text(VgiMetadataGet((const char *)call->metadata.data, VGI_RPC_PROTOCOL_VERSION_KEY));
    if (!client) return ENOMEM;
    uint64_t actual[3], expected[3];
    int invalid = semver(client, actual) || semver(version, expected);
    int mismatch = invalid || actual[0] != expected[0] || actual[1] != expected[1];
    if (mismatch) {
        call->error_type = "ProtocolVersionError";
        call->error_code = "FAILED_PRECONDITION";
        call->error_kind = "protocol_version_mismatch";
        ArrowErrorSet(
            &call->error, "protocol_version mismatch for %s. Client: %s. Server: %s. %s",
            call->protocol, *client ? client : "<not declared>", version,
            invalid ? "Client must send canonical MAJOR.MINOR.PATCH."
            : (actual[0] < expected[0] || (actual[0] == expected[0] && actual[1] < expected[1]))
                ? "Client is too old; upgrade the VGI extension/client."
                : "Server is too old; upgrade the VGI worker.");
    }
    if (mismatch) {
        struct json_object *details = json_object_new_array(), *detail = json_object_new_object(),
                           *violations = json_object_new_array(),
                           *violation = json_object_new_object();
        json_object_object_add(detail, "@type",
                               json_object_new_string("vgi_rpc.PreconditionFailure"));
        json_object_object_add(violation, "type", json_object_new_string("protocol_version"));
        json_object_object_add(violation, "subject", json_object_new_string(call->protocol));
        json_object_object_add(violation, "description",
                               json_object_new_string(call->error.message));
        json_object_array_add(violations, violation);
        json_object_object_add(detail, "violations", violations);
        json_object_array_add(details, detail);
        VgiRpcSetErrorDetails(call,
                              json_object_to_json_string_ext(details, JSON_C_TO_STRING_PLAIN));
        json_object_put(details);
    }
    free(client);
    return mismatch ? EINVAL : 0;
}

static int same_field(const struct ArrowSchema *a, const struct ArrowSchema *b) {
    if (strcmp(a->format, b->format) || a->flags != b->flags ||
        strcmp(a->name ? a->name : "", b->name ? b->name : "") || a->n_children != b->n_children ||
        (!!a->dictionary != !!b->dictionary))
        return 0;
    for (int64_t i = 0; i < a->n_children; ++i)
        if (!same_field(a->children[i], b->children[i])) return 0;
    return !a->dictionary || same_field(a->dictionary, b->dictionary);
}
static int validate_params(struct VgiRpcCall *call) {
    struct ArrowSchema expected = {0};
    int rc = call->method->params(&expected, &call->error);
    if (rc) return rc;
    int same = expected.n_children == call->request.schema.n_children;
    for (int64_t i = 0; same && i < expected.n_children; ++i)
        same = same_field(expected.children[i], call->request.schema.children[i]);
    if (!same)
        rc = VgiRpcFail(call, "TypeError", "INVALID_ARGUMENT", "",
                        "Request schema must match the declared parameter names, order, types, and "
                        "nullability");
    if (!rc && call->request.schema.n_children && call->request.array.length != 1) {
        rc = VgiRpcFail(call, "ProtocolError", "INVALID_ARGUMENT", "",
                        "RPC request must contain exactly one row");
    }
    for (int64_t i = 0; !rc && i < call->request.schema.n_children; ++i) {
        const char *name = call->request.schema.children[i]->name;
        if (!name || VgiFieldIndex(&call->request.schema, name) != i) {
            rc = VgiRpcFail(call, "TypeError", "INVALID_ARGUMENT", "",
                            "Duplicate or unnamed RPC parameter");
            break;
        }
        if (VgiFieldIndex(&expected, name) < 0) {
            char message[512];
            snprintf(message, sizeof(message), "Unexpected parameter '%s' for method '%s'", name,
                     call->method->name);
            rc = VgiRpcFail(call, "TypeError", "INVALID_ARGUMENT", "", message);
        }
    }
    for (int64_t i = 0; !rc && i < expected.n_children; ++i) {
        if (!(expected.children[i]->flags & ARROW_FLAG_NULLABLE) &&
            VgiBatchIsNull(&call->request, expected.children[i]->name)) {
            char message[512];
            snprintf(message, sizeof(message), "Required parameter '%s' is absent or null",
                     expected.children[i]->name);
            rc = VgiRpcFail(call, "TypeError", "INVALID_ARGUMENT", "", message);
        }
    }
    if (!rc && call->request.array.length) {
        struct VgiBatch normalized = {0};
        rc = VgiBatchCoerce(&call->request, &expected, &normalized, &call->error);
        if (!rc) {
            VgiBatchReset(&call->request);
            call->request = normalized;
        } else {
            call->error_type = "TypeError";
            call->error_code = "INVALID_ARGUMENT";
            if (!call->error.message[0])
                ArrowErrorSet(&call->error,
                              "RPC parameter types do not match the method signature");
        }
    }
    ArrowSchemaRelease(&expected);
    return rc;
}

void VgiRpcCallInit(struct VgiRpcCall *call, struct VgiRpcServer *server) {
    memset(call, 0, sizeof(*call));
    call->server = server;
    call->transport_kind = server->transport_kind;
    ArrowBufferInit(&call->metadata);
    unsigned char id[16];
    randombytes_buf(id, sizeof(id));
    sodium_bin2hex(call->request_id, sizeof(call->request_id), id, sizeof(id));
}
void VgiRpcCallReset(struct VgiRpcCall *call) {
    VgiShmFree(call->shm);
    VgiBatchReset(&call->request);
    ArrowBufferReset(&call->metadata);
    if (call->release_application_state) call->release_application_state(call->application_state);
    free(call->error_storage);
    free(call->error_details);
    free(call->auth_claims_json);
    clear_logs(call);
    memset(call, 0, sizeof(*call));
}
void VgiRpcReplyReset(struct VgiRpcReply *reply) {
    if (reply->stream.release) reply->stream.release(reply->stream.state);
    VgiBatchReset(&reply->value);
    ArrowBufferReset(&reply->metadata);
    VgiBatchReset(&reply->stream.header);
    if (reply->stream.output_schema.release) ArrowSchemaRelease(&reply->stream.output_schema);
    memset(reply, 0, sizeof(*reply));
}
const char *VgiRpcServerId(struct VgiRpcServer *server) { return server->id; }
int VgiRpcServerSetTelemetry(struct VgiRpcServer *server, struct VgiTelemetry *telemetry) {
    if (!server || server->serving) return EINVAL;
    server->telemetry = telemetry;
    return 0;
}
struct VgiTelemetry *VgiRpcServerTelemetry(struct VgiRpcServer *server) {
    return server ? server->telemetry : NULL;
}
int VgiRpcServerSetExternal(struct VgiRpcServer *server, const struct VgiExternalOptions *options) {
    if (!server || server->serving) return EINVAL;
    if (options && (options->max_fetch_bytes < 1 || options->max_decompressed_bytes < 1 ||
                    !options->timeout_seconds || options->threshold_bytes < 0))
        return EINVAL;
    server->external_enabled = options != NULL;
    if (options) server->external = *options;
    return 0;
}
const struct VgiExternalOptions *VgiRpcServerExternal(struct VgiRpcServer *server) {
    return server->external_enabled ? &server->external : NULL;
}
const char *VgiRpcPrimaryProtocol(struct VgiRpcServer *server) {
    return server->count ? server->protocols[0].name : "";
}
static const struct VgiMethod transport_options = {
    .name = "__transport_options__",
    .kind = VGI_METHOD_UNARY,
    .params = VgiSchemaRpcEmpty,
    .result = VgiSchemaRpcEmpty,
};
int VgiRpcPrepare(struct VgiRpcCall *call, const char *route_protocol, const char *route_method) {
    int rc = 0;
    struct VgiRpcServer *server = call->server;
    struct ArrowStringView id =
        VgiMetadataGet((const char *)call->metadata.data, VGI_RPC_REQUEST_ID_KEY);
    if (id.data && id.size_bytes < (int64_t)sizeof(call->request_id)) {
        memcpy(call->request_id, id.data, (size_t)id.size_bytes);
        call->request_id[id.size_bytes] = 0;
    }
    char *protocol_name =
        view_text(VgiMetadataGet((const char *)call->metadata.data, VGI_RPC_PROTOCOL_KEY));
    if (route_protocol && protocol_name && !*protocol_name) {
        free(protocol_name);
        protocol_name = copy_text(route_protocol);
    }
    char *method_name =
        view_text(VgiMetadataGet((const char *)call->metadata.data, VGI_RPC_RPC_METHOD_KEY));
    if (!rc && route_protocol && protocol_name && strcmp(route_protocol, protocol_name))
        rc = VgiRpcFail(call, "ProtocolNotSupportedError", "INVALID_ARGUMENT",
                        "protocol_not_supported",
                        "Request routing key disagrees with the protocol in its path");
    if (!rc && route_method && method_name && strcmp(route_method, method_name))
        rc = VgiRpcFail(call, "ProtocolError", "INVALID_ARGUMENT", "",
                        "Method name in request does not match the HTTP route");
    struct VgiRpcProtocol self = reflection_protocol(server);
    const struct VgiRpcProtocol *protocol = NULL;
    if (!rc && (!protocol_name || !method_name)) rc = ENOMEM;
    if (!rc &&
        !text_equals(VgiMetadataGet((const char *)call->metadata.data, VGI_RPC_REQUEST_VERSION_KEY),
                     "1"))
        rc = VgiRpcFail(call, "VersionError", "INVALID_ARGUMENT", "",
                        "Missing or unsupported vgi_rpc.request_version; expected 1");
    /* Framework capabilities are independent of application routing/versioning
     * and are deliberately absent from protocol reflection. */
    if (!rc && !strcmp(method_name, transport_options.name)) {
        call->protocol = "";
        call->method = &transport_options;
        free(protocol_name);
        free(method_name);
        return validate_params(call);
    }
    if (!rc)
        protocol = !strcmp(protocol_name, self.name) ? &self : find_protocol(server, protocol_name);
    if (!rc && !*protocol_name)
        rc = VgiRpcFail(call, "ProtocolNotSpecifiedError", "INVALID_ARGUMENT",
                        "protocol_not_specified", "Request must specify a protocol");
    if (!rc && !protocol)
        rc = VgiRpcFail(call, "ProtocolNotSupportedError", "UNIMPLEMENTED",
                        "protocol_not_supported", "Requested protocol is not hosted");
    if (protocol) call->protocol = protocol->name;
    if (!rc) rc = validate_version(call, protocol->version);
    if (!rc) {
        for (size_t i = 0; i < protocol->method_count; ++i)
            if (!strcmp(protocol->methods[i].name, method_name))
                call->method = &protocol->methods[i];
        if (!call->method)
            rc = VgiRpcFail(call, "MethodNotImplementedError", "UNIMPLEMENTED",
                            "method_not_implemented", "Requested method is not implemented");
    }
    if (!rc) rc = validate_params(call);
    free(protocol_name);
    free(method_name);
    return rc;
}
int VgiRpcInvoke(struct VgiRpcCall *call, struct VgiRpcReply *reply) {
    if (call->method == &transport_options) {
        int rc = VgiRpcBatchInitNamed(&reply->value, "Empty", &call->error);
        if (!rc) rc = VgiBatchFinish(&reply->value, &call->error);
        int local = call->transport_kind == VGI_TRANSPORT_PIPE ||
                    call->transport_kind == VGI_TRANSPORT_UNIX;
        if (!rc)
            rc =
                VgiMetadataSet(&reply->metadata, "vgi_rpc.transport.shm", local ? "true" : "false");
        if (!rc) rc = VgiMetadataSet(&reply->metadata, VGI_RPC_REQUEST_VERSION_KEY, "1");
        return rc;
    }
    struct VgiRpcProtocol self = reflection_protocol(call->server);
    const struct VgiRpcProtocol *protocol =
        call->protocol && !strcmp(call->protocol, self.name)
            ? &self
            : find_protocol(call->server, call->protocol ? call->protocol : "");
    if (!protocol || !call->method) return EINVAL;
    return protocol->handler(protocol->state, call, reply);
}
int VgiRpcCanResume(struct VgiRpcCall *call) {
    const struct VgiRpcProtocol *p = find_protocol(call->server, call->protocol);
    return p && p->resume;
}
int VgiRpcResume(struct VgiRpcCall *call, struct ArrowBufferView state,
                 const struct ArrowSchema *schema, struct VgiRpcReply *reply) {
    const struct VgiRpcProtocol *p = find_protocol(call->server, call->protocol);
    if (!p || !p->resume || !call->method || call->method->kind != VGI_METHOD_STREAM) return EINVAL;
    return p->resume(p->state, call, state, schema, reply);
}

static int serve_stream(struct VgiRpcCall *call, struct VgiRpcReply *reply, FILE *input,
                        FILE *output, int shm_response) {
    struct VgiRpcStream *stream = &reply->stream;
    struct ArrowError *error = &call->error;
    if (!stream->next || !stream->output_schema.release) return EINVAL;
    int rc = stream->header.array.release
                 ? write_batch(output, &stream->header, NULL, call, error, 0)
                 : 0;
    struct ArrowIpcInputStream source = {0};
    struct ArrowIpcOutputStream target = {0};
    struct VgiIpcReader reader = {0};
    struct VgiIpcWriter writer = {0};
    struct VgiBatch incoming = {0}, outgoing = {0};
    struct ArrowBuffer metadata, out_metadata;
    ArrowBufferInit(&metadata);
    ArrowBufferInit(&out_metadata);
    if (!rc) rc = ArrowIpcInputStreamInitFile(&source, input, 0);
    if (!rc) rc = VgiIpcReaderInit(&reader, &source);
    if (!rc) rc = VgiIpcReaderSchema(&reader, &incoming.schema, error);
    if (!rc) rc = ArrowIpcOutputStreamInitFile(&target, output, 0);
    if (!rc) rc = VgiIpcWriterInit(&writer, &stream->output_schema, &target, error);
    int finished = 0;
    while (!rc) {
        rc = VgiIpcReaderNext(&reader, &incoming.array, &metadata, error);
        if (rc || !incoming.array.release) break;
        if (!finished) {
            rc = VgiShmResolve(call->shm, &incoming, &metadata, error);
            if (!rc) rc = VgiExternalResolve(call, &incoming, &metadata);
            struct ArrowStringView cancel =
                VgiMetadataGet((const char *)metadata.data, VGI_RPC_CANCEL_KEY);
            if (!rc && cancel.data) {
                finished = 1;
                if (stream->on_cancel) stream->on_cancel(stream->state, call);
            }
            if (!rc && !finished) rc = VgiBatchView(&incoming, error);
            if (!rc && !finished)
                rc = stream->next(stream->state, call, &incoming, (const char *)metadata.data,
                                  &outgoing, &out_metadata, &finished);
            if (!rc) rc = VgiRpcWriteLogs(call, &writer, &stream->output_schema, error);
            if (!rc && outgoing.array.release)
                rc = VgiShmWrite(shm_response ? call->shm : NULL, &writer, &outgoing.array,
                                 (const char *)out_metadata.data, error);
            if (!rc && finished) rc = VgiIpcWriterFinish(&writer, error);
            if (!rc && fflush(output)) rc = EIO;
        }
        ArrowArrayViewReset(&incoming.view);
        ArrowArrayRelease(&incoming.array);
        VgiBatchReset(&outgoing);
        ArrowBufferReset(&out_metadata);
    }
    if (rc && writer.writer.private_data && !writer.finished) {
        struct VgiBatch empty = {0};
        struct ArrowError saved = *error;
        int error_rc = VgiBatchInit(&empty, &stream->output_schema, error);
        if (!error_rc) error_rc = VgiBatchFinish(&empty, error);
        *error = saved;
        ArrowBufferReset(&out_metadata);
        if (!error_rc) error_rc = VgiRpcErrorMetadata(call, &out_metadata);
        if (!error_rc)
            error_rc =
                VgiIpcWriterNext(&writer, &empty.array, (const char *)out_metadata.data, error);
        if (!error_rc) error_rc = VgiIpcWriterFinish(&writer, error);
        if (!error_rc && fflush(output)) error_rc = EIO;
        VgiBatchReset(&empty);
        if (!error_rc) rc = 0;
        /* After an error, consume the remainder so the next call starts at a schema. */
        if (incoming.array.release) ArrowArrayRelease(&incoming.array);
        while (!rc) {
            rc = VgiIpcReaderNext(&reader, &incoming.array, NULL, error);
            if (rc || !incoming.array.release) break;
            ArrowArrayRelease(&incoming.array);
        }
    }
    if (!rc && writer.writer.private_data) rc = VgiIpcWriterFinish(&writer, error);
    if (!rc && fflush(output)) rc = EIO;
    VgiBatchReset(&incoming);
    VgiBatchReset(&outgoing);
    ArrowBufferReset(&metadata);
    ArrowBufferReset(&out_metadata);
    VgiIpcReaderReset(&reader);
    VgiIpcWriterReset(&writer);
    if (source.release) source.release(&source);
    if (target.release) target.release(&target);
    return rc;
}

static int serve_one(struct VgiRpcServer *server, FILE *input, FILE *output,
                     struct ArrowError *error, struct VgiShm **connection_shm) {
    VgiRpcServerFreeze(server);
    int started = VgiRpcServerEnsureStarted(server, error);
    if (started) return started;
    struct VgiRpcCall call = {0};
    VgiRpcCallInit(&call, server);
    if (connection_shm) {
        call.shm = *connection_shm;
        *connection_shm = NULL;
    }
    struct VgiRpcReply reply = {0};
    ArrowBufferInit(&reply.metadata);
    struct ArrowIpcInputStream source = {0};
    struct VgiIpcReader reader = {0};
    struct ArrowArray extra = {0};
    int rc = ArrowIpcInputStreamInitFile(&source, input, 0);
    if (!rc) rc = VgiIpcReaderInit(&reader, &source);
    if (!rc) rc = VgiIpcReaderSchema(&reader, &call.request.schema, &call.error);
    if (!rc) rc = VgiIpcReaderNext(&reader, &call.request.array, &call.metadata, &call.error);
    if (!rc && !call.request.array.release)
        rc = VgiRpcFail(&call, "ProtocolError", "INVALID_ARGUMENT", "", "Request has no batch");
    if (!rc) rc = VgiIpcReaderNext(&reader, &extra, NULL, &call.error);
    if (!rc && extra.release)
        rc = VgiRpcFail(&call, "ProtocolError", "INVALID_ARGUMENT", "",
                        "Request contains multiple batches");
    if (extra.release) ArrowArrayRelease(&extra);
    VgiIpcReaderReset(&reader);
    if (source.release) source.release(&source);
    /* Pooled peers may use this connection for inline-only catalog/bind calls
     * between SHM exchanges. A cached mapping alone does not opt a response in. */
    int shm_response =
        VgiMetadataGet((const char *)call.metadata.data, "vgi_rpc.shm_segment_name").data ||
        VgiMetadataGet((const char *)call.metadata.data, "vgi_rpc.shm_offset").data;
    if (!rc &&
        (call.transport_kind == VGI_TRANSPORT_PIPE || call.transport_kind == VGI_TRANSPORT_UNIX))
        rc = VgiShmFromMetadata((const char *)call.metadata.data, &call.shm, &call.error);
    if (!rc) rc = VgiShmResolve(call.shm, &call.request, &call.metadata, &call.error);
    if (!rc) rc = VgiExternalResolve(&call, &call.request, &call.metadata);
    if (!rc) rc = VgiBatchView(&call.request, &call.error);
    if (!rc) rc = VgiRpcPrepare(&call, NULL, NULL);
    struct VgiTraceSpan *trace = VgiTraceStart(
        server->telemetry, call.protocol, call.method ? call.method->name : "unknown",
        call.method && call.method->kind == VGI_METHOD_STREAM ? "stream" : "unary",
        VgiTransportKindName(call.transport_kind), 0, (const char *)call.metadata.data, NULL, NULL);
    struct VgiTraceContext previous;
    VgiTraceContextSwap(VgiTraceSpanContext(trace), &previous);
    int invoked_stream = !rc && call.method->kind == VGI_METHOD_STREAM;
    if (!rc) rc = VgiRpcInvoke(&call, &reply);
    if (rc) {
        if (!*call.error.message) ArrowErrorSet(&call.error, "RPC call failed (error %d)", rc);
        rc = write_error(output, &call, error);
        if (!rc && invoked_stream) {
            struct ArrowIpcInputStream incoming = {0};
            struct VgiIpcReader drain = {0};
            struct ArrowSchema schema = {0};
            struct ArrowArray batch = {0};
            rc = ArrowIpcInputStreamInitFile(&incoming, input, 0);
            if (!rc) rc = VgiIpcReaderInit(&drain, &incoming);
            if (!rc) rc = VgiIpcReaderSchema(&drain, &schema, error);
            while (!rc) {
                rc = VgiIpcReaderNext(&drain, &batch, NULL, error);
                if (rc || !batch.release) break;
                ArrowArrayRelease(&batch);
            }
            if (batch.release) ArrowArrayRelease(&batch);
            if (schema.release) ArrowSchemaRelease(&schema);
            VgiIpcReaderReset(&drain);
            if (incoming.release) incoming.release(&incoming);
        }
    } else if (call.method->kind == VGI_METHOD_STREAM) {
        rc = serve_stream(&call, &reply, input, output, shm_response);
        if (rc) *error = call.error;
    } else {
        if (!reply.value.array.release) {
            rc = VgiRpcBatchInitNamed(&reply.value, "Empty", error);
            if (!rc) rc = VgiBatchFinish(&reply.value, error);
        }
        int published =
            VgiMetadataGet((const char *)reply.metadata.data, VGI_RPC_LOCATION_KEY).data != NULL;
        if (!rc && !published)
            rc = VgiMetadataSet(&reply.metadata, VGI_RPC_SERVER_ID_KEY, server->id);
        if (!rc && !published)
            rc = VgiMetadataSet(&reply.metadata, VGI_RPC_REQUEST_ID_KEY, call.request_id);
        if (!rc)
            rc = write_batch(output, &reply.value, (const char *)reply.metadata.data, &call, error,
                             shm_response);
    }
    VgiRpcReplyReset(&reply);
    if (call.error_type) VgiTraceError(trace, call.error_type);
    VgiTraceContextSwap(&previous, NULL);
    VgiTraceEnd(trace, rc || call.error.message[0]);
    if (connection_shm) {
        *connection_shm = call.shm;
        call.shm = NULL;
    }
    VgiRpcCallReset(&call);
    return rc;
}

int VgiRpcServerServeOne(struct VgiRpcServer *server, FILE *input, FILE *output,
                         struct ArrowError *error) {
    return serve_one(server, input, output, error, NULL);
}
int VgiRpcServerServe(struct VgiRpcServer *server, FILE *input, FILE *output,
                      struct ArrowError *error) {
    if (!server || !input || !output) return EINVAL;
    struct VgiShm *shm = NULL;
    int rc = 0;
    for (;;) {
        int first = fgetc(input);
        if (first == EOF) {
            rc = ferror(input) ? EIO : 0;
            break;
        }
        if (ungetc(first, input) == EOF) {
            rc = EIO;
            break;
        }
        rc = serve_one(server, input, output, error, &shm);
        if (rc) break;
    }
    VgiShmFree(shm);
    return rc;
}

int VgiRpcBatchInitNamed(struct VgiBatch *batch, const char *name, struct ArrowError *error) {
    struct ArrowSchema schema = {0};
    int rc = VgiRpcSchemaInit(!strcmp(name, "Empty") ? "RpcEmpty" : name, &schema, error);
    if (!rc) rc = VgiBatchInit(batch, &schema, error);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}

int VgiRpcLog(struct VgiRpcCall *c, const char *level, const char *message) {
    return VgiRpcLogExtra(c, level, message, NULL);
}

const char *VgiRpcProtocolHash(struct VgiRpcServer *server, const char *name) {
    if (!strcmp(name, VGI_REFLECTION_NAME)) return VGI_REFLECTION_HASH;
    const struct VgiRpcProtocol *p = find_protocol(server, name);
    return p ? p->hash : "";
}

void VgiRpcServerSetStartHook(struct VgiRpcServer *s,
                              int (*hook)(void *, enum VgiTransportKind, struct ArrowError *),
                              void *state) {
    if (!s || s->serving) return;
    s->on_start = hook;
    s->start_state = state;
}
void VgiRpcServerBindTransport(struct VgiRpcServer *s, enum VgiTransportKind kind) {
    pthread_mutex_lock(&s->start_mutex);
    if (s->transport_kind != kind) {
        s->transport_kind = kind;
        s->started = 0;
    }
    pthread_mutex_unlock(&s->start_mutex);
}
int VgiRpcServerEnsureStarted(struct VgiRpcServer *s, struct ArrowError *error) {
    pthread_mutex_lock(&s->start_mutex);
    int rc = !s->started && s->on_start ? s->on_start(s->start_state, s->transport_kind, error) : 0;
    if (!rc) s->started = 1;
    pthread_mutex_unlock(&s->start_mutex);
    return rc;
}
const char *VgiTransportKindName(enum VgiTransportKind kind) {
    const char *names[] = {"pipe", "http", "unix", "tcp"};
    return kind >= VGI_TRANSPORT_PIPE && kind <= VGI_TRANSPORT_TCP ? names[kind] : "unknown";
}
