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

/* ------------------------------------------------------------------------- */
/* Protocol version negotiation (PROTOCOL_VERSIONS_V1 §6)                    */
/* ------------------------------------------------------------------------- */

void VgiClientObserveVersions(struct VgiClient *c, const char *advertisement) {
    if (!advertisement || !c->versioned) return;
    char *copy = strdup(advertisement);
    if (!copy) return;
    free(c->advertised);
    c->advertised = copy; /* the newest statement replaces the old (a redeploy) */
    c->seen = 1;
    c->legacy_set = 0;
}
/* A pre-spec refusal names the server version as "Server: X.Y[.Z]" or
 * "server requires X.Y[.Z]", in the message or a precondition description. */
static int legacy_version(const char *text, uint64_t out[2]) {
    const char *labels[] = {"Server:", "server requires"};
    for (int k = 0; k < 2; ++k)
        for (const char *at = text ? strstr(text, labels[k]) : NULL; at;
             at = strstr(at + 1, labels[k])) {
            const char *v = at + strlen(labels[k]);
            while (*v == ' ' || *v == '\t' || *v == '\n') ++v;
            size_t n = 0;
            while ((v[n] >= '0' && v[n] <= '9') || v[n] == '.') ++n;
            /* X.Y, then an optional .Z */
            const char *dot = memchr(v, '.', n);
            const char *second = dot ? memchr(dot + 1, '.', n - (size_t)(dot + 1 - v)) : NULL;
            if (dot && !VgiVersionMajorMinor(v, second ? (size_t)(second - v) : n, out)) return 1;
        }
    return 0;
}
static void observe_refusal(struct VgiClient *c) {
    if (!c->versioned) return;
    struct json_object *details =
        json_tokener_parse(c->error.details_json ? c->error.details_json : "");
    const char *advertised = NULL;
    char *descriptions = NULL;
    size_t length = 0;
    for (size_t i = 0; details && json_object_is_type(details, json_type_array) &&
                       i < json_object_array_length(details);
         ++i) {
        struct json_object *item = json_object_array_get_idx(details, i), *metadata = NULL,
                           *violations = NULL;
        const char *type = json_text(item, "@type");
        if (!strcmp(type, "vgi_rpc.ErrorInfo") &&
            json_object_object_get_ex(item, "metadata", &metadata) &&
            *json_text(metadata, VGI_RPC_PROTOCOL_VERSIONS_KEY))
            advertised = json_text(metadata, VGI_RPC_PROTOCOL_VERSIONS_KEY);
        if (!strcmp(type, "vgi_rpc.PreconditionFailure") &&
            json_object_object_get_ex(item, "violations", &violations) &&
            json_object_is_type(violations, json_type_array))
            for (size_t j = 0; j < json_object_array_length(violations); ++j) {
                const char *d = json_text(json_object_array_get_idx(violations, j), "description");
                size_t n = strlen(d);
                char *grown = realloc(descriptions, length + n + 2);
                if (!grown) break;
                descriptions = grown;
                memcpy(descriptions + length, d, n);
                length += n;
                descriptions[length++] = '\n';
                descriptions[length] = 0;
            }
    }
    if (advertised)
        VgiClientObserveVersions(c, advertised);
    else if (!c->seen) {
        uint64_t version[2];
        if (legacy_version(c->error.message, version) || legacy_version(descriptions, version)) {
            c->legacy[0] = version[0];
            c->legacy[1] = version[1];
            c->legacy_set = 1;
        }
    }
    free(descriptions);
    json_object_put(details);
}
/* Choose what the next call speaks (c->version), or fail without sending when
 * the server's window and the client's do not overlap. */
static int choose_version(struct VgiClient *c) {
    if (!c->versioned) return 0;
    struct VgiVersionWindow server;
    int known = 0;
    if (c->seen && VgiVersionsFind(c->advertised, c->protocol, &server))
        known = 1;
    else if (c->legacy_set) {
        server.min[0] = server.max[0] = c->legacy[0];
        server.min[1] = server.max[1] = c->legacy[1];
        known = 1;
    }
    uint64_t chosen[2] = {c->window.max[0], c->window.max[1]};
    if (known) {
        const uint64_t *top = server.max;
        if (c->window.max[0] < top[0] || (c->window.max[0] == top[0] && c->window.max[1] < top[1]))
            top = c->window.max;
        chosen[0] = top[0];
        chosen[1] = top[1];
        if (!VgiVersionWindowContains(&server, chosen) ||
            !VgiVersionWindowContains(&c->window, chosen)) {
            char theirs[96], ours[96], advice[512], message[1024];
            VgiVersionWindowFormat(&server, theirs, sizeof(theirs));
            VgiVersionWindowFormat(&c->window, ours, sizeof(ours));
            const char *p = c->protocol;
            if (server.max[0] < c->window.min[0] ||
                (server.max[0] == c->window.min[0] && server.max[1] < c->window.min[1]))
                snprintf(advice, sizeof(advice),
                         "The server is too old (this client is too new for it): upgrade the "
                         "worker to one supporting %s %llu.%llu or newer, or use a client that "
                         "supports %s %llu.%llu.",
                         p, (unsigned long long)c->window.min[0],
                         (unsigned long long)c->window.min[1], p, (unsigned long long)server.max[0],
                         (unsigned long long)server.max[1]);
            else
                snprintf(advice, sizeof(advice),
                         "The client is too old (the server is too new for it): upgrade the "
                         "client to one supporting %s %llu.%llu or newer, or use a worker that "
                         "supports %s %llu.%llu.",
                         p, (unsigned long long)server.min[0], (unsigned long long)server.min[1], p,
                         (unsigned long long)c->window.max[0],
                         (unsigned long long)c->window.max[1]);
            snprintf(message, sizeof(message),
                     VGI_RPC_PROTOCOL_VERSION_UNSUPPORTED
                     ": no common %s version. This client supports %s %s; the server%s%s "
                     "supports %s %s. %s",
                     p, p, ours, c->options.target ? " at " : "",
                     c->options.target ? c->options.target : "", p, theirs, advice);
            VgiClientSetError(c, "NoCommonVersionError", "FAILED_PRECONDITION", message);
            free((void *)c->error.kind);
            c->error.kind = strdup(VGI_RPC_PROTOCOL_VERSION_UNSUPPORTED);
            return EIO;
        }
    }
    char wire[96];
    snprintf(wire, sizeof(wire), "%llu.%llu.%llu", (unsigned long long)chosen[0],
             (unsigned long long)chosen[1], (unsigned long long)c->patch);
    char *next = strdup(wire);
    if (!next) return ENOMEM;
    free(c->version);
    c->version = next;
    return 0;
}
/* Whether a call addresses this client's own (negotiated) protocol. */
static int negotiated_call(struct VgiClient *c, const char *method,
                           const struct VgiAnnotatedBatch *input) {
    if (!c->versioned || !strcmp(method, "__transport_options__")) return 0;
    struct ArrowStringView protocol =
        VgiMetadataGet((const char *)input->metadata.data, VGI_RPC_PROTOCOL_KEY);
    return !protocol.data || (protocol.size_bytes == (int64_t)strlen(c->protocol) &&
                              !memcmp(protocol.data, c->protocol, strlen(c->protocol)));
}
static int list_protocols_probe(struct VgiClient *);
/* Before a negotiated call: probe a byte-stream server's windows once when the
 * client's own window is wider than one version (Decision 11), then choose. */
static int prepare_version(struct VgiClient *c, const char *method,
                           const struct VgiAnnotatedBatch *input) {
    if (!negotiated_call(c, method, input)) return 0;
    if (!c->http && !c->probed && !c->seen &&
        (c->window.min[0] != c->window.max[0] || c->window.min[1] != c->window.max[1])) {
        c->probed = 1;
        list_protocols_probe(c); /* a server without reflection leaves us unlearned */
        VgiClientClearError(c);
    }
    return choose_version(c);
}
static int init_versions(struct VgiClient *c, const struct VgiClientOptions *options) {
    const char *declared = options->protocol_version ? options->protocol_version : "";
    /* A protocol without a canonical MAJOR.MINOR.PATCH version is not negotiated. */
    uint64_t parts[3];
    const char *s = declared;
    for (int i = 0; i < 3; ++i) {
        const char *start = s;
        parts[i] = 0;
        while (*s >= '0' && *s <= '9') {
            if (parts[i] > (UINT64_MAX - 9) / 10) return 0;
            parts[i] = parts[i] * 10 + (uint64_t)(*s++ - '0');
        }
        if (s == start || (s - start > 1 && *start == '0')) return 0;
        if (i < 2 && *s++ != '.') return 0;
    }
    if (*s) return 0;
    struct VgiVersionWindow window = {{parts[0], parts[1]}, {parts[0], parts[1]}};
    if (options->protocol_version_range && *options->protocol_version_range) {
        if (VgiVersionWindowParse(options->protocol_version_range,
                                  strlen(options->protocol_version_range), &window) ||
            window.max[0] != parts[0] || window.max[1] != parts[1])
            return EINVAL; /* the range widens protocol_version downward only */
    }
    char ranges[512], text[96];
    VgiVersionWindowFormat(&window, text, sizeof(text));
    snprintf(ranges, sizeof(ranges), "%s=%s", options->protocol, text);
    c->client_versions = strdup(ranges);
    if (!c->client_versions) return ENOMEM;
    c->window = window;
    c->patch = parts[2];
    c->versioned = 1;
    return 0;
}
const char *VgiClientProtocolVersionsAdvertised(const struct VgiClient *c) {
    return c && c->seen ? c->advertised : NULL;
}
const struct VgiClientError *VgiClientLastError(const struct VgiClient *c) { return &c->error; }
int VgiClientReadMetadata(struct VgiClient *c, const char *metadata) {
    struct ArrowStringView advertised = VgiMetadataGet(metadata, VGI_RPC_PROTOCOL_VERSIONS_KEY);
    if (advertised.data) {
        char *text = text_copy(advertised);
        if (text) VgiClientObserveVersions(c, text);
        free(text);
    }
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
        if (c->error.kind && (!strcmp(c->error.kind, VGI_RPC_PROTOCOL_VERSION_UNSUPPORTED) ||
                              !strcmp(c->error.kind, VGI_RPC_PROTOCOL_VERSION_MISMATCH)))
            observe_refusal(c);
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
        rc = VgiExternalResolveOptions(c->external_enabled ? &c->external : NULL, 0, &batch->batch,
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
    /* §4.2: the client's own window for the protocol it calls (informational). */
    struct ArrowStringView routed = VgiMetadataGet((const char *)out->data, VGI_RPC_PROTOCOL_KEY);
    if (!rc && c->client_versions && !transport_options && routed.data &&
        routed.size_bytes == (int64_t)strlen(c->protocol) &&
        !memcmp(routed.data, c->protocol, strlen(c->protocol)) &&
        !VgiMetadataGet(metadata, VGI_RPC_CLIENT_PROTOCOL_VERSIONS_KEY).data)
        rc = VgiMetadataSet(out, VGI_RPC_CLIENT_PROTOCOL_VERSIONS_KEY, c->client_versions);
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
    if (!rc) rc = init_versions(c, options);
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
    free(c->client_versions);
    free(c->advertised);
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
    int rc = prepare_version(c, method, input);
    if (rc) return rc;
    trace_start(c, method, "unary", (const char *)input->metadata.data);
    for (int attempt = 0;; ++attempt) {
        if (c->http)
            rc = VgiHttpClientUnary(c, method, input, out);
        else {
            rc = request(c, method, input);
            if (!rc) rc = substream(c, out);
        }
        /* §6: after a version refusal, retry once at the renegotiated version,
         * only for a call that is safe to repeat. Reflection is; this client
         * cannot see an application method's declared idempotency, so other
         * refused calls are surfaced and the next call speaks the learned version. */
        struct ArrowStringView routed =
            VgiMetadataGet((const char *)input->metadata.data, VGI_RPC_PROTOCOL_KEY);
        int reflection = routed.data && routed.size_bytes == (int64_t)strlen(VGI_REFLECTION_NAME) &&
                         !memcmp(routed.data, VGI_REFLECTION_NAME, strlen(VGI_REFLECTION_NAME));
        if (attempt || rc != ECANCELED || !reflection || !c->error.kind ||
            (strcmp(c->error.kind, VGI_RPC_PROTOCOL_VERSION_UNSUPPORTED) &&
             strcmp(c->error.kind, VGI_RPC_PROTOCOL_VERSION_MISMATCH)))
            break;
        VgiAnnotatedBatchReset(out);
        VgiClientClearError(c);
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
    int prepared = prepare_version(c, method, input);
    if (prepared) return prepared;
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
static int list_protocols_probe(struct VgiClient *c) {
    struct VgiAnnotatedBatch listing = {0};
    int rc = reflection_call(c, "list_protocols", NULL, &listing);
    VgiAnnotatedBatchReset(&listing);
    return rc;
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
