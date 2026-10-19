#define _POSIX_C_SOURCE 200809L
#ifdef __APPLE__
#define _DARWIN_C_SOURCE 1
#endif
#include "vgi_rpc/http.h"

#include <ctype.h>
#include <errno.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/http.h>
#include <event2/keyvalq_struct.h>
#include <event2/thread.h>
#include <json-c/json.h>
#include <limits.h>
#include <math.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <time.h>

#include "external_internal.h"
#include "http_codec.h"
#include "rpc_internal.h"
#include "session.h"
#include "telemetry_internal.h"
#include "token.h"

#define HTTP_ARROW "application/vnd.apache.arrow.stream"
#define STATE_KEY "vgi_rpc.stream_state#b64"
#define CALL_KEY "vgi_rpc.call_state#b64"
struct HttpWire;
struct HttpStream {
    struct VgiRpcCall call;
    struct VgiRpcReply reply;
    char *principal;
    char *domain;
    char *cursor;
    char id[33];
    char *token;
    int stateless;
    int64_t response_limit;
    time_t touched;
    struct HttpStream *next;
};
struct HttpServer {
    struct HttpWire *wires;
    struct VgiProxyProof *proof;
    struct VgiSessions *sessions;
    FILE *access_log;
    struct VgiRpcServer *rpc;
    struct VgiHttpOptions options;
    struct event_base *base;
    struct evhttp *http;
    struct evhttp_bound_socket *listener;
    unsigned char key[32];
    pthread_mutex_t mutex;
    struct HttpStream *streams;
    size_t stream_count;
    unsigned active;
    int stopping;
};
struct HttpTask {
    struct VgiTraceSpan *trace;
    struct VgiSessionBinding session;
    struct HttpServer *server;
    struct evhttp_request *request;
    char *path, *authorization, *type, *encoding, *accept, *custom_accept, *budget;
    char id[129];
    struct json_object *log;
    struct ArrowBuffer body, response;
    int status, rpc_error, is_delete;
    int64_t external_bytes;
    char retry_after[32];
    struct timespec started;
    const char *response_type, *response_encoding, *encoding_header, *auth_reason;
};
/* libevent generates parser errors (notably 413) before its application callback.
 * Complete the mandatory capability header at the HTTP framing boundary, while
 * retaining libevent's bounded body parser. Payload bytes pass through untouched. */
struct HttpWire {
    struct HttpServer *server;
    struct bufferevent *bev;
    struct event *flush;
    struct evbuffer_cb_entry *queued;
    struct evbuffer_cb_entry *drained;
    bufferevent_data_cb read_callback, write_callback;
    bufferevent_event_cb event_callback;
    void *callback_state;
    struct HttpWire *next;
    int64_t remaining;
    int head;
};
static void wire_read_ready(struct bufferevent *bev, void *opaque) {
    struct HttpWire *wire = opaque;
    if (wire->read_callback) wire->read_callback(bev, wire->callback_state);
}
static void wire_write_ready(struct bufferevent *bev, void *opaque) {
    struct HttpWire *wire = opaque;
    if (!evbuffer_get_length(bufferevent_get_output(bufferevent_get_underlying(bev))) &&
        wire->write_callback)
        wire->write_callback(bev, wire->callback_state);
}
static void wire_event_ready(struct bufferevent *bev, short events, void *opaque) {
    struct HttpWire *wire = opaque;
    if (wire->event_callback) wire->event_callback(bev, events, wire->callback_state);
}
static void wire_wrap(struct HttpWire *wire) {
    bufferevent_data_cb read_callback, write_callback;
    bufferevent_event_cb event_callback;
    void *state;
    bufferevent_getcb(wire->bev, &read_callback, &write_callback, &event_callback, &state);
    if (write_callback == wire_write_ready && state == wire) return;
    wire->read_callback = read_callback;
    wire->write_callback = write_callback;
    wire->event_callback = event_callback;
    wire->callback_state = state;
    bufferevent_setcb(wire->bev, wire_read_ready, wire_write_ready, wire_event_ready, wire);
}
static enum bufferevent_filter_result wire_output(struct evbuffer *source,
                                                  struct evbuffer *destination, ev_ssize_t limit,
                                                  enum bufferevent_flush_mode mode, void *opaque) {
    (void)limit;
    (void)mode;
    struct HttpWire *wire = opaque;
    wire_wrap(wire);
    int moved = 0;
    while (evbuffer_get_length(source)) {
        if (!wire->remaining) {
            struct evbuffer_ptr end = evbuffer_search(source, "\r\n\r\n", 4, NULL);
            if (end.pos < 0)
                return evbuffer_get_length(source) > 65536 ? BEV_ERROR
                       : moved                             ? BEV_OK
                                                           : BEV_NEED_MORE;
            size_t length = (size_t)end.pos + 4;
            char *header = malloc(length + 1);
            if (!header) return BEV_ERROR;
            if (evbuffer_copyout(source, header, length) != (ev_ssize_t)length) {
                free(header);
                return BEV_ERROR;
            }
            header[length] = 0;
            int status = 0, supported = 0;
            int64_t body_length = -1;
            if (sscanf(header, "HTTP/%*u.%*u %d", &status) != 1) {
                free(header);
                return BEV_ERROR;
            }
            char *save = NULL;
            for (char *line = strtok_r(header, "\r\n", &save); line;
                 line = strtok_r(NULL, "\r\n", &save)) {
                if (!strncasecmp(line, "VGI-Accept-Max-Response-Bytes-Support:", 38)) supported = 1;
                if (!strncasecmp(line, "Content-Length:", 15)) {
                    char *tail = NULL;
                    errno = 0;
                    long long parsed = strtoll(line + 15, &tail, 10);
                    while (tail && (*tail == ' ' || *tail == '\t')) ++tail;
                    if (errno || parsed < 0 || !tail || *tail) {
                        free(header);
                        return BEV_ERROR;
                    }
                    body_length = (int64_t)parsed;
                }
            }
            free(header);
            if (status < 200 || status == 204 || status == 304 || wire->head) body_length = 0;
            if (status >= 200) wire->head = 0;
            if (evbuffer_remove_buffer(source, destination, length - 2) < 0) return BEV_ERROR;
            static const char capability[] = "VGI-Accept-Max-Response-Bytes-Support: true\r\n";
            if (!supported && evbuffer_add(destination, capability, sizeof(capability) - 1))
                return BEV_ERROR;
            if (evbuffer_remove_buffer(source, destination, 2) < 0) return BEV_ERROR;
            wire->remaining = body_length;
            moved = 1;
        } else {
            size_t n = evbuffer_get_length(source);
            if (wire->remaining > 0 && (uint64_t)wire->remaining < n) n = (size_t)wire->remaining;
            if (evbuffer_remove_buffer(source, destination, n) < 0) return BEV_ERROR;
            if (wire->remaining > 0) wire->remaining -= (int64_t)n;
            moved = 1;
        }
    }
    return moved ? BEV_OK : BEV_NEED_MORE;
}
static void wire_free(void *opaque) {
    struct HttpWire *wire = opaque;
    if (wire->flush) event_free(wire->flush);
    if (wire->queued) evbuffer_remove_cb_entry(bufferevent_get_output(wire->bev), wire->queued);
    if (wire->drained)
        evbuffer_remove_cb_entry(bufferevent_get_output(bufferevent_get_underlying(wire->bev)),
                                 wire->drained);
    struct HttpWire **link = &wire->server->wires;
    while (*link && *link != wire) link = &(*link)->next;
    if (*link) *link = wire->next;
    free(wire);
}
static void wire_flush(evutil_socket_t fd, short events, void *opaque) {
    (void)fd;
    (void)events;
    struct HttpWire *wire = opaque;
    if (!(bufferevent_get_enabled(wire->bev) & EV_WRITE)) return;
    wire_wrap(wire);
    if (!evbuffer_get_length(bufferevent_get_output(wire->bev)))
        wire_write_ready(wire->bev, wire);
    else
        bufferevent_flush(wire->bev, EV_WRITE, BEV_FLUSH);
}
static void wire_queued(struct evbuffer *buffer, const struct evbuffer_cb_info *info,
                        void *opaque) {
    (void)buffer;
    struct HttpWire *wire = opaque;
    /* evhttp queues headers while EV_WRITE is disabled, then enables it.
     * libevent 2.1 filters need a flush after that transition. */
    if (info->n_added) event_active(wire->flush, EV_TIMEOUT, 0);
}
static void wire_drained(struct evbuffer *buffer, const struct evbuffer_cb_info *info,
                         void *opaque) {
    struct HttpWire *wire = opaque;
    if (info->n_deleted && !evbuffer_get_length(buffer)) event_active(wire->flush, EV_TIMEOUT, 0);
}
static struct bufferevent *wire_new(struct event_base *base, void *opaque) {
    struct HttpServer *server = opaque;
    struct HttpWire *wire = calloc(1, sizeof(*wire));
    if (!wire) return NULL;
    struct bufferevent *socket = bufferevent_socket_new(base, -1, BEV_OPT_CLOSE_ON_FREE);
    if (!socket) {
        free(wire);
        return NULL;
    }
    wire->server = server;
    wire->bev =
        bufferevent_filter_new(socket, NULL, wire_output, BEV_OPT_CLOSE_ON_FREE, wire_free, wire);
    if (!wire->bev) {
        bufferevent_free(socket);
        free(wire);
        return NULL;
    }
    wire->flush = event_new(base, -1, 0, wire_flush, wire);
    if (wire->flush)
        wire->queued = evbuffer_add_cb(bufferevent_get_output(wire->bev), wire_queued, wire);
    if (wire->flush)
        wire->drained = evbuffer_add_cb(bufferevent_get_output(socket), wire_drained, wire);
    if (!wire->flush || !wire->queued || !wire->drained) {
        bufferevent_free(wire->bev);
        return NULL;
    }
    wire->next = server->wires;
    server->wires = wire;
    return wire->bev;
}
static struct ArrowBufferView bytes(const struct ArrowBuffer *b) {
    return (struct ArrowBufferView){{b->data}, b->size_bytes};
}
static char *copy(const char *s) { return strdup(s ? s : ""); }
static int equal(struct ArrowStringView a, const char *b) {
    return a.size_bytes == (int64_t)strlen(b) && !memcmp(a.data, b, (size_t)a.size_bytes);
}
static void random_text(char *out, size_t length) {
    unsigned char raw[32];
    randombytes_buf(raw, sizeof(raw));
    sodium_bin2hex(out, length, raw, sizeof(raw));
}
static void stream_free(struct HttpStream *s) {
    if (!s) return;
    VgiRpcReplyReset(&s->reply);
    VgiRpcCallReset(&s->call);
    free(s->principal);
    free(s->domain);
    free(s->cursor);
    free(s->token);
    free(s);
}
static struct HttpStream *stream_new(struct HttpTask *task) {
    struct HttpStream *s = calloc(1, sizeof(*s));
    if (!s) return NULL;
    unsigned char stream_id[16];
    randombytes_buf(stream_id, sizeof(stream_id));
    sodium_bin2hex(s->id, sizeof(s->id), stream_id, sizeof(stream_id));
    VgiRpcCallInit(&s->call, task->server->rpc);
    ArrowBufferInit(&s->reply.metadata);
    s->call.sealing_key = task->server->key;
    if (task->server->options.signing_key && *task->server->options.signing_key)
        s->call.signing_key = task->server->key;
    memcpy(s->call.request_id, task->id, sizeof(task->id));
    s->response_limit = task->server->options.max_response_bytes;
    s->touched = time(NULL);
    return s;
}
static void remove_stream(struct HttpServer *server, struct HttpStream *stream) {
    struct HttpStream **p = &server->streams;
    while (*p && *p != stream) p = &(*p)->next;
    if (*p) {
        *p = stream->next;
        --server->stream_count;
    }
    stream_free(stream);
}

void VgiHttpOptionsInit(struct VgiHttpOptions *options) {
    memset(options, 0, sizeof(*options));
    VgiProofOptionsInit(&options->proof);
    options->host = "127.0.0.1";
    options->prefix = "/vgi";
    options->compression = 1;
    options->max_request_bytes = 64 * 1024 * 1024;
    options->max_response_bytes = 256 * 1024 * 1024;
    options->max_externalized_response_bytes = 1024 * 1024 * 1024;
    options->max_streams = 4096;
    options->stream_ttl_seconds = 300;
    options->max_concurrent_requests = 64;
    options->sticky_ttl_seconds = 300;
    options->max_sessions = 4096;
}
static int token_principal(const char *configured, const char *token, char **principal) {
    if (!configured || !*configured) return 0;
    unsigned char digest[32];
    crypto_hash_sha256(digest, (const unsigned char *)token, strlen(token));
    const char *start = configured;
    int rc = 0;
    while (*start) {
        const char *end = strchr(start, ',');
        if (!end) end = start + strlen(start);
        const char *separator = memchr(start, '=', (size_t)(end - start));
        if (!separator || separator == start || separator + 1 == end) return EINVAL;
        unsigned char expected[32];
        crypto_hash_sha256(expected, (const unsigned char *)start,
                           (unsigned long long)(separator - start));
        if (!sodium_memcmp(digest, expected, 32)) {
            char *name = malloc((size_t)(end - separator));
            if (!name) return ENOMEM;
            memcpy(name, separator + 1, (size_t)(end - separator - 1));
            name[end - separator - 1] = 0;
            free(*principal);
            *principal = name;
            rc = 1;
        }
        start = *end ? end + 1 : end;
    }
    return rc;
}
static const char *request_header(void *state, const char *name) {
    return evhttp_find_header(evhttp_request_get_input_headers(state), name);
}
static int header_count(struct evhttp_request *, const char *);
static int authenticate(struct HttpTask *task, struct HttpStream *stream) {
    struct VgiHttpOptions *o = &task->server->options;
    int rc = 0;
    struct VgiProofResult proof = {0};
    if (task->server->proof) {
        rc = VgiProxyProofCheck(
            task->server->proof, request_header(task->request, "VGI-Proxy-Proof"),
            (size_t)header_count(task->request, "VGI-Proxy-Proof"), (uint64_t)time(NULL), &proof);
        if (rc) {
            task->auth_reason = "proxy_required";
            goto auth_complete;
        }
    }
    const struct VgiGrantKeys *keys =
        o->grant_keys ? o->grant_keys : (o->identity ? o->identity->grant_keys : NULL);
    int is_grant = !strncasecmp(task->authorization, "Bearer ", 7) &&
                   !strncmp(task->authorization + 7, "vgig1.", 6);
    if (is_grant) {
        struct VgiGrant grant = {0};
        rc =
            keys ? VgiGrantOpen(keys, task->authorization + 7, (double)time(NULL), &grant) : EINVAL;
        if (!rc) {
            stream->principal = copy(grant.principal);
            stream->domain = copy("grant");
            struct json_object *claims = json_object_new_object(),
                               *scopes = json_object_new_array();
            json_object_object_add(claims, "grant_id", json_object_new_string(grant.grant_id));
            json_object_object_add(claims, "purpose", json_object_new_string(grant.purpose));
            for (size_t i = 0; i < grant.scope_count; ++i)
                json_object_array_add(scopes, json_object_new_string(grant.scopes[i]));
            json_object_object_add(claims, "scopes", scopes);
            stream->call.auth_claims_json =
                copy(json_object_to_json_string_ext(claims, JSON_C_TO_STRING_PLAIN));
            json_object_put(claims);
            if (!stream->principal || !stream->domain) rc = ENOMEM;
        }
        VgiGrantReset(&grant);
    } else if (o->authenticate || o->authenticate_request) {
        struct VgiHttpIdentity identity = {0};
        struct VgiHttpRequest request = {task->authorization, request_header, task->request};
        rc = o->authenticate_request
                 ? o->authenticate_request(o->auth_state, &request, &identity, &stream->call.error)
                 : o->authenticate(o->auth_state, task->authorization, &identity,
                                   &stream->call.error);
        if (rc)
            task->auth_reason = identity.failure_reason ? identity.failure_reason : "unauthorized";
        if (!rc) {
            if (identity.claims_json) stream->call.auth_claims_json = copy(identity.claims_json);
            stream->principal = identity.principal ? copy(identity.principal) : NULL;
            stream->domain = copy(identity.domain);
            stream->call.auth_time = identity.auth_time;
            stream->call.has_auth_time = identity.has_auth_time;
            if ((identity.principal && !stream->principal) || !stream->domain) rc = ENOMEM;
        }
    } else {
        int required = o->bearer_tokens && *o->bearer_tokens;
        if (!*task->authorization)
            rc = required ? EACCES : 0;
        else if (strncasecmp(task->authorization, "Bearer ", 7) || !task->authorization[7] ||
                 strchr(task->authorization + 7, ' '))
            rc = EACCES;
        else {
            int matched =
                token_principal(o->bearer_tokens, task->authorization + 7, &stream->principal);
            if (matched == 0)
                matched = token_principal(o->optional_bearer_tokens, task->authorization + 7,
                                          &stream->principal);
            rc = matched == 1 ? 0 : matched == 0 ? EACCES : matched;
        }
        if (!rc && stream->principal) {
            stream->domain = copy("bearer");
            if (!stream->domain) rc = ENOMEM;
        }
    }
    if (!is_grant && (rc == EACCES || (!rc && !stream->principal)) &&
        !strncasecmp(task->authorization, "Bearer ", 7) && o->identity &&
        o->identity->resolve_token) {
        const char *token = task->authorization + 7;
        rc = EACCES;
        if (VgiIdentitySubjectOk(token)) {
            struct VgiTokenIdentity result = {.ttl_seconds = 300};
            rc = o->identity->resolve_token(o->identity->state, &stream->call, token, &result);
            if (!rc && result.principal && *result.principal) {
                free(stream->principal);
                free(stream->domain);
                stream->principal = copy(result.principal);
                stream->domain = copy("token");
                struct json_object *claims = json_object_new_object();
                json_object_object_add(
                    claims, "token_name",
                    json_object_new_string(result.token_name ? result.token_name : ""));
                stream->call.auth_claims_json =
                    copy(json_object_to_json_string_ext(claims, JSON_C_TO_STRING_PLAIN));
                json_object_put(claims);
            } else if (rc == EAGAIN || (stream->call.error_code &&
                                        !strcmp(stream->call.error_code, "UNAVAILABLE"))) {
                rc = EAGAIN;
                struct json_object *details = stream->call.error_details
                                                  ? json_tokener_parse(stream->call.error_details)
                                                  : NULL;
                double retry = 5;
                if (details && json_object_is_type(details, json_type_array)) {
                    for (size_t i = 0; i < json_object_array_length(details); ++i) {
                        struct json_object *item = json_object_array_get_idx(details, i),
                                           *value = NULL;
                        if (json_object_object_get_ex(item, "retry_delay_seconds", &value))
                            retry = json_object_get_double(value);
                    }
                }
                snprintf(task->retry_after, sizeof(task->retry_after), "%.0f", ceil(retry));
                if (details) json_object_put(details);
            } else
                rc = EACCES;
            if (o->identity->release_result)
                o->identity->release_result(o->identity->state, &result, 0);
        }
    }
auth_complete:
    if (!rc && task->server->proof) {
        struct json_object *claims = stream->call.auth_claims_json
                                         ? json_tokener_parse(stream->call.auth_claims_json)
                                         : NULL;
        if (!claims || !json_object_is_type(claims, json_type_object)) {
            if (claims) json_object_put(claims);
            claims = json_object_new_object();
        }
        struct json_object *attribution = json_object_new_object();
        json_object_object_add(attribution, "verified",
                               json_object_new_string(proof.verified ? "true" : "false"));
        json_object_object_add(attribution, "kid", json_object_new_string(proof.kid));
        json_object_object_add(attribution, "origin_id",
                               json_object_new_string(o->proof.origin_id));
        json_object_object_add(attribution, "reason", json_object_new_string(proof.reason));
        if (proof.verified) {
            json_object_object_add(attribution, "proxy", json_object_new_string(proof.kid));
            if (task->log)
                json_object_object_add(task->log, "proxy", json_object_new_string(proof.kid));
        }
        json_object_object_add(claims, "vgi_proxy_proof", attribution);
        free(stream->call.auth_claims_json);
        stream->call.auth_claims_json =
            copy(json_object_to_json_string_ext(claims, JSON_C_TO_STRING_PLAIN));
        json_object_put(claims);
        if (!stream->call.auth_claims_json) rc = ENOMEM;
    }
    stream->call.principal = stream->principal;
    stream->call.auth_domain = stream->domain;
    if (task->log) {
        json_object_object_add(
            task->log, "authenticated",
            json_object_new_boolean(!rc && stream->principal && *stream->principal));
        if (!rc && stream->principal)
            json_object_object_add(task->log, "principal",
                                   json_object_new_string(stream->principal));
        if (!rc && stream->domain)
            json_object_object_add(task->log, "auth_domain",
                                   json_object_new_string(stream->domain));
    }
    if (rc) {
        task->status = rc == EAGAIN ? 503 : 401;
        if (!task->auth_reason)
            task->auth_reason =
                rc == ESTALE ? "expired_credential"
                             : (*task->authorization ? "invalid_credential" : "missing_credential");
        task->response_type = "application/json";
        struct json_object *body = json_object_new_object();
        json_object_object_add(
            body, "error",
            json_object_new_string(rc == EAGAIN ? "identity_unavailable" : "unauthorized"));
        json_object_object_add(body, "reason", json_object_new_string(task->auth_reason));
        json_object_object_add(
            body, "detail",
            json_object_new_string(rc == EAGAIN ? "Authentication authority unavailable"
                                                : "Authentication required"));
        if (o->proof.mode == VGI_PROOF_REQUIRE)
            json_object_object_add(
                body, "proxy_hint",
                json_object_new_string(
                    "Requests must arrive through the configured proxy with VGI-Proxy-Proof."));
        const char *message = json_object_to_json_string_ext(body, JSON_C_TO_STRING_PLAIN);
        int append_rc = ArrowBufferAppend(&task->response, message, (int64_t)strlen(message));
        json_object_put(body);
        if (append_rc) task->status = 500;
    }
    return rc;
}
static int writer_init(struct VgiIpcWriter *writer, const struct ArrowSchema *schema,
                       struct ArrowBuffer *body, struct ArrowError *error) {
    struct ArrowIpcOutputStream output = {0};
    int rc = ArrowIpcOutputStreamInitBuffer(&output, body);
    if (!rc) rc = VgiIpcWriterInit(writer, schema, &output, error);
    if (output.release) output.release(&output);
    return rc;
}
static int append_batch(struct ArrowBuffer *body, const struct VgiBatch *batch,
                        const char *metadata, struct VgiRpcCall *call) {
    struct VgiIpcWriter writer = {0};
    int rc = writer_init(&writer, &batch->schema, body, &call->error);
    if (!rc) rc = VgiRpcWriteLogs(call, &writer, &batch->schema, &call->error);
    if (!rc) rc = VgiIpcWriterNext(&writer, &batch->array, metadata, &call->error);
    if (!rc) rc = VgiIpcWriterFinish(&writer, &call->error);
    VgiIpcWriterReset(&writer);
    return rc;
}
static int64_t buffer_size(struct ArrowArrayView *view) {
    int64_t total = 0;
    for (int64_t i = 0; i < ArrowArrayViewGetNumBuffers(view); ++i) {
        int64_t size = ArrowArrayViewGetBufferView(view, i).size_bytes;
        if (size > INT64_MAX - total) return INT64_MAX;
        if (size > 0) total += size;
    }
    for (int64_t i = 0; i < view->n_children; ++i) {
        int64_t size = buffer_size(view->children[i]);
        if (size > INT64_MAX - total) return INT64_MAX;
        total += size;
    }
    if (view->dictionary) {
        int64_t size = buffer_size(view->dictionary);
        if (size > INT64_MAX - total) return INT64_MAX;
        total += size;
    }
    return total;
}
static int write_output(struct HttpTask *task, struct VgiIpcWriter *writer,
                        const struct VgiBatch *batch, const char *metadata,
                        struct VgiRpcCall *call) {
    const struct VgiExternalOptions *external = VgiRpcServerExternal(call->server);
    int externalize = 0, rc = 0;
    if (external && external->allocate && batch->array.length) {
        struct ArrowArrayView view = {0};
        rc = ArrowArrayViewInitFromSchema(&view, &batch->schema, &call->error);
        if (!rc) rc = ArrowArrayViewSetArray(&view, &batch->array, &call->error);
        if (!rc) externalize = buffer_size(&view) >= external->threshold_bytes;
        ArrowArrayViewReset(&view);
    }
    if (rc) return rc;
    if (!externalize) {
        rc = VgiRpcWriteLogs(call, writer, &batch->schema, &call->error);
        if (!rc) rc = VgiIpcWriterNext(writer, &batch->array, metadata, &call->error);
        return rc;
    }
    struct ArrowBuffer payload, pointer_metadata;
    ArrowBufferInit(&payload);
    ArrowBufferInit(&pointer_metadata);
    struct VgiExternalRef ref = {0};
    struct VgiBatch pointer = {0};
    /* Include all logs belonging to this output cycle in the stored stream. */
    rc = append_batch(&payload, batch, metadata, call);
    if (!rc && payload.size_bytes >
                   task->server->options.max_externalized_response_bytes - task->external_bytes)
        rc = VgiRpcFail(call, "ResponseTooLargeError", "RESOURCE_EXHAUSTED", "",
                        "RPC response exceeds max_externalized_response_bytes");
    if (!rc)
        rc = VgiExternalPublish(external, &batch->schema, bytes(&payload), 1, &ref, &call->error);
    if (!rc) task->external_bytes += payload.size_bytes;
    if (!rc)
        rc = VgiExternalPointer(&batch->schema, &ref, &pointer, &pointer_metadata, &call->error);
    if (!rc)
        rc = VgiIpcWriterNext(writer, &pointer.array, (const char *)pointer_metadata.data,
                              &call->error);
    VgiBatchReset(&pointer);
    VgiExternalRefReset(&ref);
    ArrowBufferReset(&payload);
    ArrowBufferReset(&pointer_metadata);
    return rc;
}
static int append_output(struct HttpTask *task, const struct VgiBatch *batch, const char *metadata,
                         struct VgiRpcCall *call) {
    struct VgiIpcWriter writer = {0};
    int rc = writer_init(&writer, &batch->schema, &task->response, &call->error);
    if (!rc) rc = write_output(task, &writer, batch, metadata, call);
    if (!rc) rc = VgiIpcWriterFinish(&writer, &call->error);
    VgiIpcWriterReset(&writer);
    return rc;
}
static int write_failure(struct HttpTask *task, struct VgiRpcCall *call,
                         const struct ArrowSchema *schema) {
    task->rpc_error = 1;
    VgiTraceError(task->trace, call->error_type);
    if (task->log && call->error_type) {
        json_object_object_add(task->log, "error_type", json_object_new_string(call->error_type));
        if (call->error_code)
            json_object_object_add(task->log, "error_code",
                                   json_object_new_string(call->error_code));
    }
    struct VgiBatch empty = {0};
    struct ArrowBuffer metadata;
    ArrowBufferInit(&metadata);
    struct ArrowError saved = call->error;
    int rc = schema ? VgiBatchInit(&empty, schema, &call->error)
                    : VgiRpcBatchInitNamed(&empty, "Empty", &call->error);
    if (!rc) rc = VgiBatchFinish(&empty, &call->error);
    call->error = saved;
    if (!call->error.message[0]) ArrowErrorSet(&call->error, "RPC request failed");
    if (!rc) rc = VgiRpcErrorMetadata(call, &metadata);
    ArrowBufferReset(&task->response);
    if (!rc) rc = append_batch(&task->response, &empty, (const char *)metadata.data, call);
    VgiBatchReset(&empty);
    ArrowBufferReset(&metadata);
    return rc;
}
static int response_limit(struct HttpTask *task, struct HttpStream *stream) {
    if (!*task->budget) return 0;
    const char *p = task->budget;
    uint64_t value = 0;
    while (*p) {
        if (!isdigit((unsigned char)*p) || value > 9007199254740991ULL / 10)
            return VgiRpcFail(&stream->call, "ValueError", "INVALID_ARGUMENT", "",
                              "VGI-Accept-Max-Response-Bytes must be an integer between 65536 and "
                              "9007199254740991");
        value = value * 10 + (unsigned)(*p++ - '0');
    }
    if (value < 65536 || value > 9007199254740991ULL)
        return VgiRpcFail(
            &stream->call, "ValueError", "INVALID_ARGUMENT", "",
            "VGI-Accept-Max-Response-Bytes must be an integer between 65536 and 9007199254740991");
    if ((int64_t)value < stream->response_limit) stream->response_limit = (int64_t)value;
    return 0;
}
static struct VgiTokenContext token_context(struct HttpServer *server, struct HttpStream *s,
                                            const char *purpose, const char *protocol,
                                            const char *method) {
    return (struct VgiTokenContext){server->key, purpose,   protocol,
                                    method,      s->domain, s->principal};
}
static int token_prefix(struct ArrowBuffer *b, struct HttpStream *s) {
    unsigned char id[16];
    size_t n = 0;
    if (sodium_hex2bin(id, sizeof(id), s->id, 32, NULL, &n, NULL)) return EINVAL;
    int rc = VgiTokenAppendU64(b, (uint64_t)time(NULL));
    if (!rc) rc = ArrowBufferAppend(b, id, sizeof(id));
    if (!rc) rc = VgiTokenAppendU64(b, (uint64_t)s->response_limit);
    return rc;
}
static int call_token(struct HttpServer *server, struct HttpStream *s) {
    struct ArrowBuffer plain, request, schema;
    ArrowBufferInit(&plain);
    ArrowBufferInit(&request);
    ArrowBufferInit(&schema);
    struct VgiBatch empty = {0};
    int rc = token_prefix(&plain, s);
    if (!rc && s->stateless) {
        rc = VgiIpcWriteBatch(&s->call.request.schema, &s->call.request.array,
                              (const char *)s->call.metadata.data, &request, &s->call.error);
        if (!rc) rc = VgiBatchInit(&empty, &s->reply.stream.output_schema, &s->call.error);
        if (!rc) rc = VgiBatchFinish(&empty, &s->call.error);
        if (!rc) rc = VgiIpcWriteBatch(&empty.schema, &empty.array, NULL, &schema, &s->call.error);
        if (!rc) rc = VgiTokenAppendU64(&plain, (uint64_t)request.size_bytes);
        if (!rc) rc = ArrowBufferAppend(&plain, request.data, request.size_bytes);
        if (!rc) rc = ArrowBufferAppend(&plain, schema.data, schema.size_bytes);
    }
    struct VgiTokenContext context = token_context(server, s, s->stateless ? "call" : "local-call",
                                                   s->call.protocol, s->call.method->name);
    if (!rc) rc = VgiTokenSeal(&context, bytes(&plain), &s->token);
    VgiBatchReset(&empty);
    ArrowBufferReset(&plain);
    ArrowBufferReset(&request);
    ArrowBufferReset(&schema);
    return rc;
}
static int cursor_token(struct HttpServer *server, struct HttpStream *s) {
    char *next = NULL;
    if (!s->stateless) {
        next = malloc(65);
        if (!next) return ENOMEM;
        random_text(next, 65);
    } else {
        struct ArrowBuffer state;
        ArrowBufferInit(&state);
        int rc = token_prefix(&state, s);
        if (!rc) rc = s->reply.stream.serialize(s->reply.stream.state, &state, &s->call.error);
        struct VgiTokenContext context =
            token_context(server, s, "cursor", s->call.protocol, s->call.method->name);
        if (!rc) rc = VgiTokenSeal(&context, bytes(&state), &next);
        ArrowBufferReset(&state);
        if (rc) return rc;
    }
    free(s->cursor);
    s->cursor = next;
    return 0;
}
static int token_current(struct HttpServer *server, const struct ArrowBuffer *token) {
    if (token->size_bytes < 32) return 0;
    uint64_t created = VgiTokenReadU64(token->data), now = (uint64_t)time(NULL);
    return created <= now + 30 &&
           (created > now || now - created <= server->options.stream_ttl_seconds);
}
static int resume_stream(struct HttpTask *task, struct HttpStream *incoming, const char *protocol,
                         const char *method, struct HttpStream **out) {
    struct ArrowBuffer cursor, call, schema_metadata;
    ArrowBufferInit(&cursor);
    ArrowBufferInit(&call);
    ArrowBufferInit(&schema_metadata);
    struct VgiBatch schema = {0};
    struct HttpStream *s = NULL;
    struct VgiTokenContext context =
        token_context(task->server, incoming, "cursor", protocol, method);
    /* Authentication of the cursor and its call id MUST precede call resolution. */
    int rc = VgiTokenOpen(
        &context, VgiMetadataGet((const char *)incoming->call.metadata.data, STATE_KEY), &cursor);
    if (!rc && !token_current(task->server, &cursor)) rc = EINVAL;
    context.purpose = "call";
    if (!rc)
        rc = VgiTokenOpen(
            &context, VgiMetadataGet((const char *)incoming->call.metadata.data, CALL_KEY), &call);
    if (!rc && (call.size_bytes < 40 || !token_current(task->server, &call) ||
                sodium_memcmp(cursor.data + 8, call.data + 8, 16)))
        rc = EINVAL;
    uint64_t request_length = !rc ? VgiTokenReadU64(call.data + 32) : 0;
    if (!rc && request_length > (uint64_t)call.size_bytes - 40) rc = EINVAL;
    if (!rc) {
        s = stream_new(task);
        if (!s) rc = ENOMEM;
    }
    if (!rc) {
        s->principal = copy(incoming->principal);
        s->domain = copy(incoming->domain);
        if (!s->principal || !s->domain) rc = ENOMEM;
        s->call.principal = s->principal;
        s->call.auth_domain = s->domain;
        s->call.auth_time = incoming->call.auth_time;
        s->call.has_auth_time = incoming->call.has_auth_time;
        if (incoming->call.auth_claims_json) {
            s->call.auth_claims_json = copy(incoming->call.auth_claims_json);
            if (!s->call.auth_claims_json) rc = ENOMEM;
        }
        s->call.session_binding = &task->session;
        s->stateless = 1;
        sodium_bin2hex(s->id, sizeof(s->id), call.data + 8, 16);
        uint64_t frozen_limit = VgiTokenReadU64(cursor.data + 24);
        if (frozen_limit < 65536 || frozen_limit > INT64_MAX)
            rc = EINVAL;
        else if ((int64_t)frozen_limit < s->response_limit)
            s->response_limit = (int64_t)frozen_limit;
    }
    if (!rc)
        rc = VgiIpcReadBatch((struct ArrowBufferView){{call.data + 40}, (int64_t)request_length},
                             &s->call.request.schema, &s->call.request.array, &s->call.metadata,
                             &s->call.error);
    if (!rc) rc = VgiBatchView(&s->call.request, &s->call.error);
    if (!rc)
        rc = VgiIpcReadBatch(
            (struct ArrowBufferView){{call.data + 40 + request_length},
                                     call.size_bytes - 40 - (int64_t)request_length},
            &schema.schema, &schema.array, &schema_metadata, &s->call.error);
    if (!rc && schema.array.length) rc = EINVAL;
    if (!rc) rc = VgiRpcPrepare(&s->call, protocol, method);
    if (!rc)
        rc = VgiRpcResume(&s->call,
                          (struct ArrowBufferView){{cursor.data + 32}, cursor.size_bytes - 32},
                          &schema.schema, &s->reply);
    if (!rc && (!s->reply.stream.next || !s->reply.stream.serialize ||
                !s->reply.stream.output_schema.release))
        rc = EINVAL;
    if (!rc) {
        *out = s;
        s = NULL;
    }
    stream_free(s);
    VgiBatchReset(&schema);
    ArrowBufferReset(&cursor);
    ArrowBufferReset(&call);
    ArrowBufferReset(&schema_metadata);
    return rc;
}
static int route(struct HttpTask *task, char **protocol, char **method, int *mode) {
    char *path = task->path;
    const char *prefix = task->server->options.prefix ? task->server->options.prefix : "/vgi";
    size_t n = strlen(prefix);
    if (!strncmp(path, prefix, n) && path[n] == '/') path += n;
    if (*path++ != '/') return EINVAL;
    char *slash = strchr(path, '/');
    if (!slash || slash == path) return EINVAL;
    *slash++ = 0;
    *protocol = path;
    *method = slash;
    char *suffix = strchr(slash, '/');
    *mode = 0;
    if (suffix) {
        *suffix++ = 0;
        if (!strcmp(suffix, "init"))
            *mode = 1;
        else if (!strcmp(suffix, "exchange"))
            *mode = 2;
        else
            return EINVAL;
    }
    for (char *p = path; p < slash - 1; ++p)
        if (!isalnum((unsigned char)*p) && *p != '.' && *p != '_' && *p != '-') return EINVAL;
    if (!*slash) return EINVAL;
    for (char *p = slash; *p; ++p)
        if (!isalnum((unsigned char)*p) && *p != '_') return EINVAL;
    return 0;
}
static int turn(struct HttpTask *task, struct HttpStream *stream, const struct VgiBatch *input,
                const char *input_metadata, int initial, int *finished) {
    if (task->log) {
        json_object_object_add(task->log, "stream_id", json_object_new_string(stream->id));
        if (!initial) {
            json_object_object_del(task->log, "request_fields");
            json_object_object_del(task->log, "request_rows");
        }
    }
    struct VgiRpcCall *call = &stream->call;
    struct VgiRpcStream *rpc = &stream->reply.stream;
    struct VgiIpcWriter writer = {0};
    struct VgiBatch output = {0}, empty = {0};
    struct ArrowBuffer metadata, clean;
    ArrowBufferInit(&metadata);
    ArrowBufferInit(&clean);
    int rc =
        initial && rpc->header.array.release ? append_output(task, &rpc->header, NULL, call) : 0;
    if (!rc) rc = writer_init(&writer, &rpc->output_schema, &task->response, &call->error);
    if (!rc) rc = VgiBatchInit(&empty, &rpc->output_schema, &call->error);
    if (!rc) rc = VgiBatchFinish(&empty, &call->error);
    if (!rc && VgiMetadataGet(input_metadata, VGI_RPC_CANCEL_KEY).data) {
        *finished = 1;
        if (rpc->on_cancel) rpc->on_cancel(rpc->state, call);
    } else if (!rc && (!initial || rpc->is_producer)) {
        rc = ArrowMetadataBuilderInit(&clean, input_metadata);
        if (!rc) rc = ArrowMetadataBuilderRemove(&clean, ArrowCharView(STATE_KEY));
        if (!rc) rc = ArrowMetadataBuilderRemove(&clean, ArrowCharView(CALL_KEY));
        if (!rc) rc = ArrowMetadataBuilderRemove(&clean, ArrowCharView(VGI_RPC_CANCEL_KEY));
        if (!rc)
            rc = rpc->next(rpc->state, call, input, (const char *)clean.data, &output, &metadata,
                           finished);
        if (!rc && !*finished) rc = cursor_token(task->server, stream);
        if (!rc && !*finished && output.array.release && !rpc->is_producer)
            rc = VgiMetadataSet(&metadata, STATE_KEY, stream->cursor);
        if (!rc && output.array.release)
            rc = write_output(task, &writer, &output, (const char *)metadata.data, call);
        else if (!rc)
            rc = VgiRpcWriteLogs(call, &writer, &rpc->output_schema, &call->error);
    }
    if (!rc && initial && !rpc->is_producer) rc = cursor_token(task->server, stream);
    if (rc && writer.writer.private_data) {
        task->rpc_error = 1;
        *finished = 1;
        ArrowBufferReset(&metadata);
        int error_rc = VgiRpcErrorMetadata(call, &metadata);
        if (!error_rc)
            error_rc =
                VgiIpcWriterNext(&writer, &empty.array, (const char *)metadata.data, &call->error);
        rc = error_rc;
    }
    if (!rc && !*finished && (initial || rpc->is_producer || !output.array.length)) {
        ArrowBufferReset(&metadata);
        rc = VgiMetadataSet(&metadata, STATE_KEY, stream->cursor);
        if (!rc && initial) rc = VgiMetadataSet(&metadata, CALL_KEY, stream->token);
        if (!rc)
            rc = VgiIpcWriterNext(&writer, &empty.array, (const char *)metadata.data, &call->error);
    }
    if (!rc) rc = VgiIpcWriterFinish(&writer, &call->error);
    VgiIpcWriterReset(&writer);
    VgiBatchReset(&output);
    VgiBatchReset(&empty);
    ArrowBufferReset(&metadata);
    ArrowBufferReset(&clean);
    if (!rc && task->response.size_bytes > stream->response_limit) {
        *finished = 1;
        VgiRpcFail(call, "ResponseTooLargeError", "RESOURCE_EXHAUSTED", "",
                   "RPC response exceeds max_response_bytes");
        rc = write_failure(task, call, &rpc->output_schema);
    }
    return rc;
}
static int identities_equal(const struct HttpStream *a, const struct HttpStream *b) {
    return !strcmp(a->principal ? a->principal : "", b->principal ? b->principal : "") &&
           !strcmp(a->domain ? a->domain : "", b->domain ? b->domain : "");
}
static int upload_urls(struct HttpTask *task, struct HttpStream *stream) {
    struct VgiRpcCall *call = &stream->call;
    const struct VgiExternalOptions *external = VgiRpcServerExternal(call->server);
    if (!equal(VgiMetadataGet((const char *)call->metadata.data, VGI_RPC_RPC_METHOD_KEY),
               "__upload_url__") ||
        !equal(VgiMetadataGet((const char *)call->metadata.data, VGI_RPC_REQUEST_VERSION_KEY),
               "1") ||
        call->request.array.length != 1 || call->request.schema.n_children != 1 ||
        strcmp(call->request.schema.children[0]->format, "l") ||
        !call->request.schema.children[0]->name ||
        strcmp(call->request.schema.children[0]->name, "count"))
        return VgiRpcFail(call, "TypeError", "INVALID_ARGUMENT", "", "Invalid upload URL request");
    int64_t count = VgiBatchInt(&call->request, "count", 1);
    if (count < 1) count = 1;
    if (count > 100) count = 100;
    struct ArrowSchema schema = {0};
    ArrowSchemaInit(&schema);
    int rc = ArrowSchemaSetTypeStruct(&schema, 3);
    const char *names[] = {"upload_url", "download_url", "expires_at"};
    const char *formats[] = {"u", "u", "tsu:UTC"};
    for (int i = 0; !rc && i < 3; ++i) {
        rc = ArrowSchemaSetName(schema.children[i], names[i]);
        if (!rc) rc = ArrowSchemaSetFormat(schema.children[i], formats[i]);
    }
    struct VgiBatch result = {0};
    if (!rc) rc = VgiBatchInit(&result, &schema, &call->error);
    for (int64_t i = 0; !rc && i < count; ++i) {
        struct VgiExternalUploadUrl url = {0};
        rc = external->allocate(external->storage_state, &call->request.schema, &url, &call->error);
        if (!rc && (!url.upload_url || !url.download_url)) rc = EINVAL;
        if (!rc)
            rc = ArrowArrayAppendString(result.array.children[0], ArrowCharView(url.upload_url));
        if (!rc)
            rc = ArrowArrayAppendString(result.array.children[1], ArrowCharView(url.download_url));
        if (!rc) rc = ArrowArrayAppendInt(result.array.children[2], url.expires_at_us);
        if (!rc) rc = ArrowArrayFinishElement(&result.array);
        VgiExternalUploadUrlReset(&url);
    }
    if (!rc) rc = VgiBatchFinish(&result, &call->error);
    if (!rc) rc = append_batch(&task->response, &result, NULL, call);
    if (!rc && task->response.size_bytes > stream->response_limit)
        rc = VgiRpcFail(call, "ResponseTooLargeError", "RESOURCE_EXHAUSTED", "",
                        "Upload URL response exceeds max_response_bytes");
    VgiBatchReset(&result);
    ArrowSchemaRelease(&schema);
    return rc;
}
static int dispatch_core(struct HttpTask *task) {
    struct HttpServer *server = task->server;
    struct HttpStream *s = stream_new(task);
    if (!s) return ENOMEM;
    int rc = VgiRpcServerEnsureStarted(server->rpc, &s->call.error);
    if (rc) {
        task->status = 500;
        write_failure(task, &s->call, NULL);
        stream_free(s);
        return 0;
    }
    rc = authenticate(task, s);
    if (rc) {
        stream_free(s);
        return 0;
    }
    const char *path = task->path;
    const char *prefix = server->options.prefix ? server->options.prefix : "/vgi";
    size_t prefix_length = strlen(prefix);
    if (!strncmp(path, prefix, prefix_length) && path[prefix_length] == '/') path += prefix_length;
    int deleting_session = task->is_delete && !strcmp(path, "/__session__") && server->sessions;
    const struct VgiExternalOptions *external = VgiRpcServerExternal(server->rpc);
    int upload_control = external && external->allocate && !strcmp(path, "/__upload_url__/init");
    const char *accept = request_header(task->request, "VGI-Session-Accept");
    rc = VgiSessionBind(server->sessions, &s->call, request_header(task->request, "VGI-Session"),
                        accept && !strcasecmp(accept, "true"), &task->session);
    if (deleting_session) {
        if (!rc && task->session.entry) {
            VgiRpcSessionClose(&s->call);
            task->status = 204;
        }
        task->response_type = "application/json";
        stream_free(s);
        return 0;
    }
    if (!rc && server->options.control) {
        int handled =
            server->options.control(server->options.control_state, path, task->is_delete, &s->call);
        if (!handled) {
            task->status = 204;
            task->response_type = "application/json";
            stream_free(s);
            return 0;
        }
        if (handled != ENOENT) rc = handled;
    }
    if (rc) {
        write_failure(task, &s->call, NULL);
        stream_free(s);
        return 0;
    }
    if (task->is_delete) {
        task->status = 404;
        stream_free(s);
        return 0;
    }
    if (strncmp(task->type, HTTP_ARROW, strlen(HTTP_ARROW))) {
        task->status = 415;
        stream_free(s);
        return 0;
    }
    char *protocol = NULL, *method = NULL;
    int mode = 0;
    rc = response_limit(task, s);
    if (upload_control) {
        protocol = (char *)VgiRpcPrimaryProtocol(server->rpc);
        method = "__upload_url__";
    }
    if (!rc && !upload_control && route(task, &protocol, &method, &mode)) {
        task->status = 404;
        rc = VgiRpcFail(&s->call, "AttributeError", "NOT_FOUND", "", "No RPC route for this path");
    }
    if (task->log && protocol && method) {
        json_object_object_add(task->log, "protocol", json_object_new_string(protocol));
        json_object_object_add(task->log, "method", json_object_new_string(method));
        json_object_object_add(task->log, "protocol_hash",
                               json_object_new_string(VgiRpcProtocolHash(server->rpc, protocol)));
        json_object_object_add(task->log, "method_type",
                               json_object_new_string(mode ? "stream" : "unary"));
    }
    struct ArrowBuffer decoded;
    ArrowBufferInit(&decoded);
    if (!rc)
        rc = VgiHttpDecode(task->encoding, bytes(&task->body), server->options.max_request_bytes,
                           &decoded);
    if (rc == EFBIG)
        task->status = 413;
    else if (rc == ENOTSUP)
        task->status = 415;
    if (!rc)
        rc = VgiIpcReadBatch(bytes(&decoded), &s->call.request.schema, &s->call.request.array,
                             &s->call.metadata, &s->call.error);
    if (!rc && task->trace && !request_header(task->request, "traceparent")) {
        VgiTraceReparent(task->trace, (const char *)s->call.metadata.data);
        VgiTraceContextSwap(VgiTraceSpanContext(task->trace), NULL);
    }
    if (!rc) {
        int resolved = VgiExternalResolve(&s->call, &s->call.request, &s->call.metadata);
        if (resolved) {
            ArrowBufferReset(&decoded);
            write_failure(task, &s->call, NULL);
            stream_free(s);
            return 0;
        }
    }
    if (!rc) rc = VgiBatchView(&s->call.request, &s->call.error);
    if (!rc && task->log) {
        struct json_object *fields = json_object_new_array();
        for (int64_t i = 0; i < s->call.request.schema.n_children; ++i) {
            struct json_object *field = json_object_new_object();
            struct ArrowSchema *schema = s->call.request.schema.children[i];
            char type[512];
            ArrowSchemaToString(schema, type, sizeof(type), 0);
            json_object_object_add(field, "name",
                                   json_object_new_string(schema->name ? schema->name : ""));
            json_object_object_add(field, "type", json_object_new_string(type));
            json_object_array_add(fields, field);
        }
        json_object_object_add(task->log, "request_fields", fields);
        json_object_object_add(task->log, "request_rows",
                               json_object_new_int64(s->call.request.array.length));
    }
    ArrowBufferReset(&decoded);
    if (rc) {
        if (task->status == 200) task->status = 400;
        write_failure(task, &s->call, NULL);
        stream_free(s);
        return 0;
    }
    if (upload_control) {
        rc = upload_urls(task, s);
        if (rc) write_failure(task, &s->call, NULL);
        stream_free(s);
        return 0;
    }
    if (mode == 2) {
        struct ArrowStringView cursor =
                                   VgiMetadataGet((const char *)s->call.metadata.data, STATE_KEY),
                               token =
                                   VgiMetadataGet((const char *)s->call.metadata.data, CALL_KEY);
        struct HttpStream *live = NULL;
        int local = cursor.size_bytes == 64;
        if (local) {
            pthread_mutex_lock(&server->mutex);
            live = server->streams;
            while (live && !equal(cursor, live->cursor)) live = live->next;
            if (!live || !identities_equal(s, live) || strcmp(protocol, live->call.protocol) ||
                strcmp(method, live->call.method->name) || !equal(token, live->token))
                rc = EINVAL;
        } else
            rc = resume_stream(task, s, protocol, method, &live);
        if (rc) {
            if (local) pthread_mutex_unlock(&server->mutex);
            task->status = 400;
            VgiRpcFail(&s->call, "ProtocolError", "INVALID_ARGUMENT", "",
                       "Unknown or missing stream state token");
            write_failure(task, &s->call, NULL);
            stream_free(s);
            return 0;
        }
        if (s->response_limit < live->response_limit) live->response_limit = s->response_limit;
        memcpy(live->call.request_id, task->id, sizeof(task->id));
        live->touched = time(NULL);
        live->call.session_binding = &task->session;
        int finished = 0;
        rc = turn(task, live, &s->call.request, (const char *)s->call.metadata.data, 0, &finished);
        live->call.session_binding = NULL;
        if (rc) {
            write_failure(task, &live->call, &live->reply.stream.output_schema);
            finished = 1;
        }
        if (!local)
            stream_free(live);
        else {
            if (finished) remove_stream(server, live);
            pthread_mutex_unlock(&server->mutex);
        }
        stream_free(s);
        return 0;
    }
    rc = VgiRpcPrepare(&s->call, protocol, method);
    if (rc) {
        task->status =
            s->call.error_kind && (!strcmp(s->call.error_kind, "protocol_not_supported") ||
                                   !strcmp(s->call.error_kind, "method_not_implemented"))
                ? 404
                : 400;
    }
    if (!rc && ((s->call.method->kind == VGI_METHOD_STREAM) != (mode == 1))) {
        task->status = 404;
        rc = VgiRpcFail(&s->call, "MethodNotImplementedError", "UNIMPLEMENTED",
                        "method_not_implemented", "Protocol has no such method endpoint");
    }
    memcpy(s->call.request_id, task->id, sizeof(task->id));
    if (!rc) rc = VgiRpcInvoke(&s->call, &s->reply);
    if (rc) {
        write_failure(task, &s->call, NULL);
        stream_free(s);
        return 0;
    }
    if (mode == 1) {
        s->stateless = s->reply.stream.serialize && VgiRpcCanResume(&s->call);
        if (!s->stateless && server->options.require_stateless_streams)
            rc = VgiRpcFail(&s->call, "NotImplementedError", "UNIMPLEMENTED", "",
                            "Stream does not support stateless HTTP continuations");
        if (!rc) rc = call_token(server, s);
        struct VgiBatch tick = {0};
        if (!rc) rc = VgiRpcBatchInitNamed(&tick, "Empty", &s->call.error);
        if (!rc) rc = VgiBatchFinish(&tick, &s->call.error);
        int finished = 0;
        if (!rc) rc = turn(task, s, &tick, (const char *)s->call.metadata.data, 1, &finished);
        VgiBatchReset(&tick);
        if (rc) {
            write_failure(task, &s->call, NULL);
            finished = 1;
        }
        if (!finished && !s->stateless) {
            pthread_mutex_lock(&server->mutex);
            if (server->stream_count >= server->options.max_streams) {
                VgiRpcFail(&s->call, "ResourceExhaustedError", "RESOURCE_EXHAUSTED", "",
                           "Too many active streams");
                write_failure(task, &s->call, NULL);
            } else {
                s->next = server->streams;
                server->streams = s;
                ++server->stream_count;
                s->call.session_binding = NULL;
                s = NULL;
            }
            pthread_mutex_unlock(&server->mutex);
        }
    } else {
        if (!s->reply.value.array.release) {
            rc = VgiRpcBatchInitNamed(&s->reply.value, "Empty", &s->call.error);
            if (!rc) rc = VgiBatchFinish(&s->reply.value, &s->call.error);
        }
        int published =
            VgiMetadataGet((const char *)s->reply.metadata.data, VGI_RPC_LOCATION_KEY).data != NULL;
        if (!rc && !published)
            rc = VgiMetadataSet(&s->reply.metadata, VGI_RPC_SERVER_ID_KEY,
                                VgiRpcServerId(server->rpc));
        if (!rc && !published)
            rc = VgiMetadataSet(&s->reply.metadata, VGI_RPC_REQUEST_ID_KEY, task->id);
        if (!rc)
            rc = append_output(task, &s->reply.value, (const char *)s->reply.metadata.data,
                               &s->call);
        if (!rc && task->response.size_bytes > s->response_limit)
            rc = VgiRpcFail(&s->call, "ResponseTooLargeError", "RESOURCE_EXHAUSTED", "",
                            "RPC response exceeds max_response_bytes");
        if (rc) write_failure(task, &s->call, NULL);
    }
    stream_free(s);
    return 0;
}
static int dispatch(struct HttpTask *task) {
    /* Route a copy: route() splits its path buffer in place. */
    struct HttpTask copy = *task;
    copy.path = strdup(task->path);
    char *protocol = NULL, *method = NULL;
    int mode = 0;
    if (copy.path && !route(&copy, &protocol, &method, &mode))
        task->trace = VgiTraceStart(VgiRpcServerTelemetry(task->server->rpc), protocol, method,
                                    mode == 1   ? "stream.init"
                                    : mode == 2 ? "stream.exchange"
                                                : "unary",
                                    "http", 0, NULL, request_header(task->request, "traceparent"),
                                    request_header(task->request, "tracestate"));
    free(copy.path);
    struct VgiTraceContext previous;
    VgiTraceContextSwap(VgiTraceSpanContext(task->trace), &previous);
    int rc = dispatch_core(task);
    VgiSessionUnbind(&task->session);
    VgiTraceContextSwap(&previous, NULL);
    VgiTraceEnd(task->trace, rc || task->rpc_error || task->status >= 400);
    task->trace = NULL;
    return rc;
}

static int offered(const char *list, const char *codec) {
    const char *p = list;
    while (p && *p) {
        while (*p == ' ' || *p == '\t' || *p == ',') ++p;
        const char *end = strchr(p, ',');
        if (!end) end = p + strlen(p);
        const char *semi = memchr(p, ';', (size_t)(end - p));
        const char *last = semi ? semi : end;
        while (last > p && isspace((unsigned char)last[-1])) --last;
        if ((size_t)(last - p) == strlen(codec) && !strncasecmp(p, codec, (size_t)(last - p))) {
            if (semi) {
                const char *q = semi + 1;
                while (q < end && isspace((unsigned char)*q)) ++q;
                if (end - q >= 2 && !strncasecmp(q, "q=", 2)) {
                    char value[32];
                    size_t n = (size_t)(end - q - 2);
                    if (n >= sizeof(value)) return 0;
                    memcpy(value, q + 2, n);
                    value[n] = 0;
                    char *tail = NULL;
                    double weight = strtod(value, &tail);
                    while (tail && isspace((unsigned char)*tail)) ++tail;
                    if (tail == value || *tail || !(weight > 0 && weight <= 1)) return 0;
                }
            }
            return 1;
        }
        p = *end ? end + 1 : end;
    }
    return 0;
}
static const char *choose(const char *list) {
    const char *p = list;
    while (p && *p) {
        while (*p == ' ' || *p == '\t' || *p == ',') ++p;
        const char *end = strchr(p, ',');
        if (!end) end = p + strlen(p);
        size_t n = (size_t)(end - p);
        char token[64];
        if (n < sizeof(token)) {
            memcpy(token, p, n);
            token[n] = 0;
            const char *names[] = {"identity", "zstd", "gzip"};
            for (int i = 0; i < 3; ++i)
                if (offered(token, names[i])) return names[i];
        }
        p = *end ? end + 1 : end;
    }
    return NULL;
}
static void task_free(struct HttpTask *t) {
    free(t->path);
    free(t->authorization);
    free(t->type);
    free(t->encoding);
    free(t->accept);
    free(t->custom_accept);
    free(t->budget);
    ArrowBufferReset(&t->body);
    ArrowBufferReset(&t->response);
    if (t->log) json_object_put(t->log);
    free(t);
}
static void capabilities(struct HttpServer *server, struct evhttp_request *request,
                         const char *id) {
    struct evkeyvalq *h = evhttp_request_get_output_headers(request);
    char number[32];
    evhttp_add_header(h, "X-Request-ID", id);
    if (server->options.proof.mode == VGI_PROOF_REQUIRE)
        evhttp_add_header(h, "VGI-Proxy-Proof-Required", "true");
    if (server->sessions) {
        evhttp_add_header(h, "VGI-Sticky-Enabled", "true");
        snprintf(number, sizeof(number), "%u", server->options.sticky_ttl_seconds);
        evhttp_add_header(h, "VGI-Sticky-Default-TTL", number);
        if (server->options.sticky_echo_count) {
            struct ArrowBuffer names;
            ArrowBufferInit(&names);
            int rc = 0;
            for (size_t i = 0; !rc && i < server->options.sticky_echo_count; ++i) {
                if (i) rc = ArrowBufferAppend(&names, ", ", 2);
                const char *name = server->options.sticky_echo_names[i];
                if (!rc) rc = ArrowBufferAppend(&names, name, (int64_t)strlen(name));
            }
            if (!rc) rc = ArrowBufferAppend(&names, "", 1);
            if (!rc) evhttp_add_header(h, "VGI-Sticky-Echo-Headers", (const char *)names.data);
            ArrowBufferReset(&names);
        }
    }
    const struct VgiExternalOptions *external = VgiRpcServerExternal(server->rpc);
    evhttp_add_header(h, "VGI-Externalization-Enabled",
                      external && external->allocate ? "true" : "false");
    if (external && external->allocate) {
        evhttp_add_header(h, "VGI-Upload-URL-Support", "true");
        snprintf(number, sizeof(number), "%lld", (long long)external->max_fetch_bytes);
        evhttp_add_header(h, "VGI-Max-Upload-Bytes", number);
    }
    snprintf(number, sizeof(number), "%lld",
             (long long)server->options.max_externalized_response_bytes);
    evhttp_add_header(h, "VGI-Max-Externalized-Response-Bytes", number);
    evhttp_add_header(h, "VGI-Accept-Max-Response-Bytes-Support", "true");
    evhttp_add_header(h, "VGI-Supported-Encodings",
                      server->options.compression ? "zstd, gzip" : "");
    snprintf(number, sizeof(number), "%lld", (long long)server->options.max_request_bytes);
    evhttp_add_header(h, "VGI-Max-Request-Bytes", number);
    snprintf(number, sizeof(number), "%lld", (long long)server->options.max_response_bytes);
    evhttp_add_header(h, "VGI-Max-Response-Bytes", number);
    evhttp_add_header(h, "Cache-Control", "no-store");
    if (server->options.cors_origin && *server->options.cors_origin) {
        const char *origin =
            evhttp_find_header(evhttp_request_get_input_headers(request), "Origin");
        if (!strcmp(server->options.cors_origin, "*") || !origin ||
            !strcmp(origin, server->options.cors_origin)) {
            evhttp_add_header(h, "Access-Control-Allow-Origin", server->options.cors_origin);
            evhttp_add_header(h, "Vary", "Origin");
            evhttp_add_header(h, "Access-Control-Allow-Methods",
                              "GET, HEAD, POST, DELETE, OPTIONS");
            evhttp_add_header(
                h, "Access-Control-Allow-Headers",
                "Content-Type, Authorization, X-Request-ID, X-VGI-Accept-Encoding, "
                "X-VGI-Content-Encoding, VGI-Accept-Max-Response-Bytes, VGI-Proxy-Proof, "
                "VGI-Session, VGI-Session-Accept");
            evhttp_add_header(
                h, "Access-Control-Expose-Headers",
                "X-Request-ID, X-VGI-RPC-Error, X-VGI-Content-Encoding, VGI-Supported-Encodings, "
                "VGI-Max-Request-Bytes, VGI-Max-Response-Bytes, VGI-Externalization-Enabled, "
                "VGI-Accept-Max-Response-Bytes-Support, VGI-Auth-Reason, WWW-Authenticate, "
                "VGI-Proxy-Proof-Required, VGI-Auth-Proxy-Required, VGI-Sticky-Enabled, "
                "VGI-Sticky-Default-TTL, VGI-Sticky-Echo-Headers, VGI-Session, VGI-Session-Close, "
                "VGI-Upload-URL-Support, VGI-Max-Upload-Bytes, "
                "VGI-Max-Externalized-Response-Bytes");
            evhttp_add_header(h, "Access-Control-Max-Age", "600");
            evhttp_add_header(h, "Cross-Origin-Resource-Policy", "cross-origin");
        }
    }
}
static void complete(evutil_socket_t fd, short events, void *opaque) {
    (void)fd;
    (void)events;
    struct HttpTask *task = opaque;
    struct HttpServer *server = task->server;
    struct evkeyvalq *headers = evhttp_request_get_output_headers(task->request);
    capabilities(server, task->request, task->id);
    evhttp_add_header(headers, "Content-Type", task->response_type);
    if (*task->session.token) {
        evhttp_add_header(headers, "VGI-Session", task->session.token);
        for (size_t i = 0; i < server->options.sticky_echo_count; ++i) {
            char name[512];
            snprintf(name, sizeof(name), "VGI-Echo-%s", server->options.sticky_echo_names[i]);
            evhttp_add_header(headers, name, server->options.sticky_echo_values[i]);
        }
    }
    if (task->session.closed) evhttp_add_header(headers, "VGI-Session-Close", "true");
    if (task->rpc_error) evhttp_add_header(headers, "X-VGI-RPC-Error", "true");
    if (task->response_encoding)
        evhttp_add_header(headers, task->encoding_header, task->response_encoding);
    if (task->status == 401) {
        if (server->options.proof.mode == VGI_PROOF_REQUIRE)
            evhttp_add_header(headers, "VGI-Auth-Proxy-Required", "true");
        evhttp_add_header(headers, "Cache-Control", "no-store");
        evhttp_add_header(headers, "WWW-Authenticate", "Bearer realm=\"vgi\"");
        evhttp_add_header(headers, "VGI-Auth-Reason",
                          task->auth_reason ? task->auth_reason : "invalid_credential");
    }
    if (task->status == 503)
        evhttp_add_header(headers, "Retry-After", *task->retry_after ? task->retry_after : "1");
    struct evbuffer *body = evbuffer_new();
    if (body) evbuffer_add(body, task->response.data, (size_t)task->response.size_bytes);
    evhttp_send_reply(task->request, body ? task->status : 500, NULL, body);
    if (body) evbuffer_free(body);
    if (server->access_log && task->log) {
        if (server->sessions) {
            json_object_object_add(task->log, "session_id",
                                   json_object_new_string(task->session.id));
            json_object_object_add(
                task->log, "session_action",
                json_object_new_string(task->session.action ? task->session.action : "none"));
        }
        json_object_object_add(task->log, "request_id", json_object_new_string(task->id));
        struct timespec now, wall;
        clock_gettime(CLOCK_MONOTONIC, &now);
        clock_gettime(CLOCK_REALTIME, &wall);
        struct tm utc;
        gmtime_r(&wall.tv_sec, &utc);
        char stamp[64], base[32];
        strftime(base, sizeof(base), "%Y-%m-%dT%H:%M:%S", &utc);
        snprintf(stamp, sizeof(stamp), "%s.%03ldZ", base, wall.tv_nsec / 1000000);
        json_object_object_add(task->log, "timestamp", json_object_new_string(stamp));
        double duration = (now.tv_sec - task->started.tv_sec) * 1000.0 +
                          (now.tv_nsec - task->started.tv_nsec) / 1000000.0;
        json_object_object_add(task->log, "duration_ms", json_object_new_double(duration));
        json_object_object_add(
            task->log, "status",
            json_object_new_string(task->rpc_error || task->status >= 400 ? "error" : "ok"));
        struct json_object *existing = NULL;
        if (!json_object_object_get_ex(task->log, "error_type", &existing))
            json_object_object_add(task->log, "error_type", json_object_new_string(""));
        json_object_object_add(task->log, "status_code", json_object_new_int(task->status));
        json_object_object_add(task->log, "rpc_error", json_object_new_boolean(task->rpc_error));
        json_object_object_add(task->log, "request_bytes",
                               json_object_new_int64(task->body.size_bytes));
        json_object_object_add(task->log, "response_bytes",
                               json_object_new_int64(task->response.size_bytes));
        fprintf(server->access_log, "%s\n",
                json_object_to_json_string_ext(task->log, JSON_C_TO_STRING_PLAIN));
        fflush(server->access_log);
    }
    --server->active;
    task_free(task);
    if (server->stopping && !server->active) event_base_loopbreak(server->base);
}
static void *run_task(void *opaque) {
    struct HttpTask *task = opaque;
    if (dispatch(task)) {
        task->status = 500;
        ArrowBufferReset(&task->response);
    }
    if (task->server->options.compression && task->response.size_bytes &&
        !strcmp(task->response_type, HTTP_ARROW)) {
        const char *codec = choose(task->custom_accept);
        if (!codec) codec = choose(task->accept);
        if (codec && strcmp(codec, "identity")) {
            struct ArrowBuffer compressed;
            ArrowBufferInit(&compressed);
            if (!VgiHttpEncode(codec, bytes(&task->response), &compressed)) {
                ArrowBufferReset(&task->response);
                ArrowBufferMove(&compressed, &task->response);
                task->response_encoding = codec;
                task->encoding_header =
                    offered(task->accept, codec) ? "Content-Encoding" : "X-VGI-Content-Encoding";
            }
            ArrowBufferReset(&compressed);
        }
    }
    struct timeval now = {0, 0};
    event_base_once(task->server->base, -1, EV_TIMEOUT, complete, task, &now);
    return NULL;
}
static char *header(struct evhttp_request *request, const char *name) {
    return copy(evhttp_find_header(evhttp_request_get_input_headers(request), name));
}
static int header_count(struct evhttp_request *request, const char *name) {
    int count = 0;
    struct evkeyval *p = evhttp_request_get_input_headers(request)->tqh_first;
    for (; p; p = p->next.tqe_next) count += !strcasecmp(p->key, name);
    return count;
}
static void receive(struct evhttp_request *request, void *opaque) {
    struct HttpServer *server = opaque;
    char id[65];
    random_text(id, sizeof(id));
    const char *uri = evhttp_request_get_uri(request);
    enum evhttp_cmd_type method = evhttp_request_get_command(request);
    struct bufferevent *bev =
        evhttp_connection_get_bufferevent(evhttp_request_get_connection(request));
    for (struct HttpWire *wire = server->wires; wire; wire = wire->next)
        if (wire->bev == bev) {
            wire->head = method == EVHTTP_REQ_HEAD;
            break;
        }
    char health_path[1024];
    snprintf(health_path, sizeof(health_path), "%s/health",
             server->options.prefix ? server->options.prefix : "/vgi");
    if (((!strcmp(uri, "/health") || !strcmp(uri, health_path)) &&
         (method == EVHTTP_REQ_GET || method == EVHTTP_REQ_HEAD)) ||
        method == EVHTTP_REQ_OPTIONS) {
        capabilities(server, request, id);
        struct evbuffer *body = evbuffer_new();
        if (body && method == EVHTTP_REQ_GET) {
            struct json_object *info = json_object_new_object();
            json_object_object_add(info, "status", json_object_new_string("ok"));
            json_object_object_add(info, "server_id",
                                   json_object_new_string(VgiRpcServerId(server->rpc)));
            json_object_object_add(info, "protocol",
                                   json_object_new_string(VgiRpcPrimaryProtocol(server->rpc)));
            const char *text = json_object_to_json_string_ext(info, JSON_C_TO_STRING_PLAIN);
            evbuffer_add(body, text, strlen(text));
            json_object_put(info);
        }
        evhttp_add_header(evhttp_request_get_output_headers(request), "Content-Type",
                          "application/json");
        evhttp_send_reply(request, 200, NULL, body);
        if (body) evbuffer_free(body);
        return;
    }
    if (method != EVHTTP_REQ_POST && method != EVHTTP_REQ_DELETE) {
        capabilities(server, request, id);
        evhttp_send_reply(request, 404, NULL, NULL);
        return;
    }
    if (server->active >= server->options.max_concurrent_requests || server->stopping) {
        capabilities(server, request, id);
        evhttp_send_reply(request, 503, NULL, NULL);
        return;
    }
    if (header_count(request, "Authorization") > 1 ||
        header_count(request, "VGI-Accept-Max-Response-Bytes") > 1 ||
        header_count(request, "Content-Encoding") +
                header_count(request, "X-VGI-Content-Encoding") >
            1) {
        capabilities(server, request, id);
        evhttp_send_reply(request, 400, NULL, NULL);
        return;
    }
    struct HttpTask *task = calloc(1, sizeof(*task));
    if (!task) {
        evhttp_send_reply(request, 500, NULL, NULL);
        return;
    }
    task->server = server;
    if (server->access_log) {
        task->log = json_object_new_object();
        json_object_object_add(task->log, "request_id", json_object_new_string(id));
        json_object_object_add(task->log, "transport", json_object_new_string("http"));
        json_object_object_add(task->log, "level", json_object_new_string("INFO"));
        json_object_object_add(task->log, "logger", json_object_new_string("vgi_rpc.access"));
        json_object_object_add(task->log, "message", json_object_new_string("RPC request"));
        json_object_object_add(task->log, "server_id",
                               json_object_new_string(VgiRpcServerId(server->rpc)));
        json_object_object_add(task->log, "server_version",
                               json_object_new_string("vgi-rpc-c/0.1.0"));
        json_object_object_add(task->log, "principal", json_object_new_string(""));
        json_object_object_add(task->log, "auth_domain", json_object_new_string(""));
        char *address = NULL;
        ev_uint16_t peer_port = 0;
        evhttp_connection_get_peer(evhttp_request_get_connection(request), &address, &peer_port);
        json_object_object_add(task->log, "remote_addr",
                               json_object_new_string(address ? address : ""));
        clock_gettime(CLOCK_MONOTONIC, &task->started);
    }
    task->request = request;
    task->is_delete = method == EVHTTP_REQ_DELETE;
    task->status = 200;
    task->response_type = HTTP_ARROW;
    ArrowBufferInit(&task->body);
    ArrowBufferInit(&task->response);
    task->path = copy(uri);
    task->authorization = header(request, "Authorization");
    task->type = header(request, "Content-Type");
    task->encoding = header(request, evhttp_find_header(evhttp_request_get_input_headers(request),
                                                        "X-VGI-Content-Encoding")
                                         ? "X-VGI-Content-Encoding"
                                         : "Content-Encoding");
    task->accept = header(request, "Accept-Encoding");
    task->custom_accept = header(request, "X-VGI-Accept-Encoding");
    task->budget = header(request, "VGI-Accept-Max-Response-Bytes");
    snprintf(task->id, sizeof(task->id), "%s", id);
    const char *request_id =
        evhttp_find_header(evhttp_request_get_input_headers(request), "X-Request-ID");
    if (request_id && strlen(request_id) < sizeof(task->id)) {
        int valid = 1;
        for (const char *p = request_id; *p; ++p)
            valid &= (unsigned char)*p >= 32 && (unsigned char)*p < 127;
        if (valid) strcpy(task->id, request_id);
    }
    struct evbuffer *input = evhttp_request_get_input_buffer(request);
    size_t size = evbuffer_get_length(input);
    int rc = !task->path || !task->authorization || !task->type || !task->encoding ||
                     !task->accept || !task->custom_accept || !task->budget
                 ? ENOMEM
                 : ArrowBufferAppend(&task->body, evbuffer_pullup(input, -1), (int64_t)size);
    if (rc) {
        task_free(task);
        evhttp_send_reply(request, 500, NULL, NULL);
        return;
    }
    evhttp_request_own(request);
    ++server->active;
    pthread_t thread;
    rc = pthread_create(&thread, NULL, run_task, task);
    if (rc) {
        task->status = 503;
        complete(-1, 0, task);
    } else
        pthread_detach(thread);
}
static void sweep(evutil_socket_t fd, short events, void *opaque) {
    (void)fd;
    (void)events;
    struct HttpServer *server = opaque;
    time_t now = time(NULL);
    VgiSessionsSweep(server->sessions);
    pthread_mutex_lock(&server->mutex);
    struct HttpStream *s = server->streams;
    while (s) {
        struct HttpStream *next = s->next;
        if (now - s->touched > (time_t)server->options.stream_ttl_seconds) remove_stream(server, s);
        s = next;
    }
    pthread_mutex_unlock(&server->mutex);
}
static void stop(evutil_socket_t fd, short events, void *opaque) {
    (void)fd;
    (void)events;
    struct HttpServer *server = opaque;
    server->stopping = 1;
    VgiSessionsDrain(server->sessions, 1);
    if (server->listener) {
        evhttp_del_accept_socket(server->http, server->listener);
        server->listener = NULL;
    }
    if (!server->active) event_base_loopbreak(server->base);
}
static pthread_once_t event_once = PTHREAD_ONCE_INIT;
static void event_threads(void) { evthread_use_pthreads(); }
int VgiRpcServerServeHttp(struct VgiRpcServer *rpc, const struct VgiHttpOptions *options,
                          struct ArrowError *error) {
    if (!rpc || !options || options->port > 65535 || options->max_request_bytes < 1 ||
        options->max_response_bytes < 65536 || options->max_externalized_response_bytes < 1 ||
        !options->max_streams || !options->stream_ttl_seconds ||
        !options->max_concurrent_requests ||
        (options->sticky_enabled && (!options->sticky_ttl_seconds || !options->max_sessions)))
        return EINVAL;
    const struct VgiGrantKeys *keys =
        options->grant_keys ? options->grant_keys
                            : (options->identity ? options->identity->grant_keys : NULL);
    if (keys && VgiGrantKeysValidate(keys)) {
        ArrowErrorSet(error, "Invalid grant key configuration");
        return EINVAL;
    }
    struct VgiIdentityOptions identity =
        options->identity ? *options->identity : (struct VgiIdentityOptions){0};
    if (keys) identity.grant_keys = keys;
    int configured = VgiRpcServerSetIdentity(rpc, &identity, error);
    if (configured) return configured;
    VgiRpcServerBindTransport(rpc, VGI_TRANSPORT_HTTP);
    VgiRpcServerFreeze(rpc);
    pthread_once(&event_once, event_threads);
    struct HttpServer server = {0};
    int proof_rc = VgiProxyProofNew(&options->proof, &server.proof, error);
    if (proof_rc) return proof_rc;
    server.rpc = rpc;
    server.options = *options;
    pthread_mutex_init(&server.mutex, NULL);
    const char *raw = options->signing_key;
    if (raw && *raw) {
        if (strlen(raw) == 32)
            memcpy(server.key, raw, 32);
        else
            crypto_hash_sha256(server.key, (const unsigned char *)raw, strlen(raw));
    } else
        randombytes_buf(server.key, 32);
    int key_rc = 0;
    if (options->token_key) {
        size_t decoded = 0;
        if (strlen(options->token_key) != 64 ||
            sodium_hex2bin(server.key, 32, options->token_key, 64, NULL, &decoded, NULL) ||
            decoded != 32)
            key_rc = EINVAL;
    }
    if (!key_rc && options->sticky_enabled)
        key_rc = VgiSessionsNew(server.key, VgiRpcServerId(rpc), options->sticky_ttl_seconds,
                                options->max_sessions, &server.sessions);
    server.base = event_base_new();
    server.http = server.base ? evhttp_new(server.base) : NULL;
    int rc = key_rc ? key_rc : server.http ? 0 : ENOMEM;
    if (!rc && options->access_log && *options->access_log) {
        server.access_log = fopen(options->access_log, "a");
        if (!server.access_log) rc = errno;
    }
    if (!rc) {
        evhttp_set_max_body_size(server.http, (ev_ssize_t)options->max_request_bytes);
        evhttp_set_max_headers_size(server.http, 65536);
        evhttp_set_timeout(server.http, 60);
        evhttp_set_allowed_methods(server.http, EVHTTP_REQ_GET | EVHTTP_REQ_HEAD | EVHTTP_REQ_POST |
                                                    EVHTTP_REQ_OPTIONS | EVHTTP_REQ_DELETE);
        evhttp_set_gencb(server.http, receive, &server);
        evhttp_set_bevcb(server.http, wire_new, &server);
        server.listener = evhttp_bind_socket_with_handle(
            server.http, options->host ? options->host : "127.0.0.1", (ev_uint16_t)options->port);
        if (!server.listener) rc = EADDRINUSE;
    }
    struct event *interrupt = NULL, *terminate = NULL, *timer = NULL;
    if (!rc) {
        struct sockaddr_storage address;
        socklen_t size = sizeof(address);
        if (getsockname(evhttp_bound_socket_get_fd(server.listener), (struct sockaddr *)&address,
                        &size))
            rc = errno;
        else {
            unsigned port = address.ss_family == AF_INET
                                ? ntohs(((struct sockaddr_in *)&address)->sin_port)
                                : ntohs(((struct sockaddr_in6 *)&address)->sin6_port);
            printf("PORT:%u\n", port);
            fflush(stdout);
        }
    }
    if (!rc) {
        interrupt = evsignal_new(server.base, SIGINT, stop, &server);
        terminate = evsignal_new(server.base, SIGTERM, stop, &server);
        timer = event_new(server.base, -1, EV_PERSIST, sweep, &server);
        struct timeval interval = {10, 0};
        if (!interrupt || !terminate || !timer)
            rc = ENOMEM;
        else {
            event_add(interrupt, NULL);
            event_add(terminate, NULL);
            event_add(timer, &interval);
            if (event_base_dispatch(server.base) < 0) rc = EIO;
        }
    }
    if (timer) event_free(timer);
    if (interrupt) event_free(interrupt);
    if (terminate) event_free(terminate);
    while (server.streams) remove_stream(&server, server.streams);
    if (server.http) evhttp_free(server.http);
    if (server.base) event_base_free(server.base);
    pthread_mutex_destroy(&server.mutex);
    VgiProxyProofFree(server.proof);
    VgiSessionsFree(server.sessions);
    if (server.access_log) fclose(server.access_log);
    sodium_memzero(server.key, 32);
    if (rc) ArrowErrorSet(error, "HTTP server failed: %s", strerror(rc));
    return rc;
}
