#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <json-c/json.h>
#include <netdb.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "client_internal.h"
#include "external_internal.h"
#include "rpc_internal.h"
#include "shm_internal.h"
#include "telemetry_internal.h"
extern char **environ;

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
int VgiClientReusable(struct VgiClient *c) {
    if (!c || c->broken || c->streaming || c->child <= 0 || !c->input) return 0;
    int status;
    pid_t result = waitpid(c->child, &status, WNOHANG);
    if (result == c->child || (result < 0 && errno == ECHILD)) {
        c->child = 0;
        c->broken = 1;
        return 0;
    }
    struct pollfd fd = {fileno(c->input), POLLIN, 0};
    int rc;
    do {
        rc = poll(&fd, 1, 0);
    } while (rc < 0 && errno == EINTR);
    return rc == 0;
}
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
static void resolved_log(void *state, const char *metadata) {
    (void)VgiClientReadMetadata(state, metadata);
}
int VgiClientResolve(struct VgiClient *c, struct VgiAnnotatedBatch *batch) {
    struct VgiRpcCall call;
    VgiRpcCallInit(&call, c->resolver);
    int rc = VgiShmResolve(c->shm, &batch->batch, &batch->metadata, &call.error);
    if (!rc) rc = VgiExternalResolve(&call, &batch->batch, &batch->metadata);
    VgiRpcDrainLogs(&call, resolved_log, c);
    if (rc)
        VgiClientSetError(c, call.error_type ? call.error_type : "ExternalLocationError",
                          call.error_code ? call.error_code : "INVALID_ARGUMENT",
                          call.error.message);
    VgiRpcCallReset(&call);
    return rc;
}
int VgiClientRequestMetadata(struct VgiClient *c, const char *method, const char *metadata,
                             struct ArrowBuffer *out) {
    int rc = ArrowMetadataBuilderInit(out, metadata);
    const char *keys[] = {"vgi_rpc.method", VGI_RPC_PROTOCOL_KEY, VGI_RPC_PROTOCOL_VERSION_KEY,
                          VGI_RPC_REQUEST_VERSION_KEY};
    const char *values[] = {method, c->protocol, c->version, "1"};
    for (int i = 0; !rc && i < 4; ++i)
        if (values[i] && *values[i] && !VgiMetadataGet(metadata, keys[i]).data)
            rc = VgiMetadataSet(out, keys[i], values[i]);
    if (!rc) rc = VgiShmAdvertise(c->shm, out);
    if (!rc) rc = VgiTraceInject(c->trace, out);
    return rc;
}
static int spawn_child(struct VgiClient *c, const char *const *argv) {
    if (!argv || !argv[0]) return EINVAL;
    int input[2], output[2];
    if (pipe(input)) return errno;
    if (pipe(output)) {
        int rc = errno;
        close(input[0]);
        close(input[1]);
        return rc;
    }
    int fds[] = {input[0], input[1], output[0], output[1]};
    for (int i = 0; i < 4; ++i) fcntl(fds[i], F_SETFD, FD_CLOEXEC);
    posix_spawn_file_actions_t actions;
    int rc = posix_spawn_file_actions_init(&actions);
    if (!rc) {
        rc = posix_spawn_file_actions_adddup2(&actions, input[0], STDIN_FILENO);
        if (!rc) rc = posix_spawn_file_actions_adddup2(&actions, output[1], STDOUT_FILENO);
        for (int i = 0; !rc && i < 4; ++i) rc = posix_spawn_file_actions_addclose(&actions, fds[i]);
        if (!rc)
            rc = posix_spawnp(&c->child, argv[0], &actions, NULL, (char *const *)argv, environ);
        posix_spawn_file_actions_destroy(&actions);
    }
    close(input[0]);
    close(output[1]);
    if (!rc) {
        c->input = fdopen(output[0], "rb");
        c->output = fdopen(input[1], "wb");
        if (!c->input || !c->output) rc = errno;
    }
    if (!c->input) close(output[0]);
    if (!c->output) close(input[1]);
    return rc;
}
static int connect_socket(struct VgiClient *c) {
    int fd = -1, rc = 0;
    const char *target = c->options.target;
    if (!target) return EINVAL;
    if (c->options.transport == VGI_CLIENT_UNIX) {
        struct sockaddr_un address = {0};
        address.sun_family = AF_UNIX;
        if (strlen(target) >= sizeof(address.sun_path)) return ENAMETOOLONG;
        strcpy(address.sun_path, target);
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd >= 0 && connect(fd, (struct sockaddr *)&address, sizeof(address))) {
            rc = errno;
            close(fd);
            fd = -1;
        }
    } else {
        char *host = strdup(target);
        if (!host) return ENOMEM;
        char *port = strrchr(host, ':');
        if (!port) {
            free(host);
            return EINVAL;
        }
        *port++ = 0;
        struct addrinfo hints = {0}, *addresses = NULL;
        hints.ai_socktype = SOCK_STREAM;
        rc = getaddrinfo(*host ? host : "127.0.0.1", port, &hints, &addresses) ? EINVAL : 0;
        for (struct addrinfo *a = addresses; !rc && a; a = a->ai_next) {
            fd = socket(a->ai_family, a->ai_socktype, a->ai_protocol);
            if (fd < 0) continue;
            if (!connect(fd, a->ai_addr, a->ai_addrlen)) break;
            close(fd);
            fd = -1;
        }
        freeaddrinfo(addresses);
        free(host);
    }
    if (fd < 0) return rc ? rc : ECONNREFUSED;
    struct timeval timeout = {(time_t)c->options.timeout_seconds, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#ifdef SO_NOSIGPIPE
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
    fcntl(fd, F_SETFD, FD_CLOEXEC);
    int other = dup(fd);
    if (other < 0) {
        rc = errno;
        close(fd);
        return rc;
    }
    fcntl(other, F_SETFD, FD_CLOEXEC);
    c->input = fdopen(fd, "rb");
    c->output = fdopen(other, "wb");
    if (!c->input) close(fd);
    if (!c->output) close(other);
    return c->input && c->output ? 0 : ENOMEM;
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
    c->resolver = VgiRpcServerNew();
    int rc = c->protocol && c->version && c->resolver ? 0 : ENOMEM;
    if (!rc) rc = VgiRpcServerSetExternal(c->resolver, options->external);
    if (!rc && options->shared_memory_size) {
        rc = VgiShmCreate(options->shared_memory_size, &c->shm, error);
        if (!rc) VgiShmSetThreshold(c->shm, options->shared_memory_threshold);
    }
    if (!rc)
        rc = options->transport == VGI_CLIENT_STDIO  ? spawn_child(c, options->argv)
             : options->transport == VGI_CLIENT_HTTP ? VgiHttpClientInit(c)
                                                     : connect_socket(c);
    if (rc) {
        ArrowErrorSet(error, "Cannot connect: %s",
                      c->error.message ? c->error.message : strerror(rc));
        VgiClientFree(c);
    } else {
        if (c->input && c->output) {
            int fd = fileno(c->input);
            fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
            fd = fileno(c->output);
            fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
        }
        *out = c;
    }
    return rc;
}
void VgiClientFree(struct VgiClient *c) {
    if (!c) return;
    trace_end(c, c->streaming);
    VgiHttpClientReset(c);
    VgiIpcWriterReset(&c->writer);
    VgiIpcReaderReset(&c->reader);
    if (c->output_schema.release) ArrowSchemaRelease(&c->output_schema);
    if (c->input) fclose(c->input);
    if (c->output) fclose(c->output);
    if (c->child > 0) {
        int status, waited = 0;
        /* EOF lets cooperative workers release state and flush telemetry. */
        for (int i = 0; i < 40; ++i) {
            if (i == 20) kill(c->child, SIGTERM);
            pid_t result = waitpid(c->child, &status, WNOHANG);
            if (result == c->child || (result < 0 && errno == ECHILD)) {
                waited = 1;
                break;
            }
            struct timespec delay = {0, 10000000};
            nanosleep(&delay, NULL);
        }
        if (!waited) {
            kill(c->child, SIGKILL);
            while (waitpid(c->child, &status, 0) < 0 && errno == EINTR) {
            }
        }
    }
    VgiRpcServerFree(c->resolver);
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
    if (!rc && fflush(c->output)) rc = EIO;
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
                fflush(c->output);
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
    if (!rc && fflush(c->output)) rc = EIO;
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
    if (!rc && fflush(c->output)) rc = EIO;
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
