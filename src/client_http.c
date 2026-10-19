#define _POSIX_C_SOURCE 200809L
#include <ctype.h>
#include <curl/curl.h>
#include <errno.h>
#include <json-c/json.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <zstd.h>

#include "client_internal.h"
#include "external_internal.h"
#include "http_codec.h"
#include "rpc_internal.h"

struct PendingBatch {
    struct VgiAnnotatedBatch value;
    struct PendingBatch *next;
};
struct ClientSession {
    char *token;
    struct VgiClientHeader *echo;
    size_t echo_count;
    int detached;
    struct ClientSession *previous;
};
struct VgiHttpClient {
    CURL *curl;
    char *base, *method, *protocol, *cursor, *call_token;
    struct json_object *defaults, *headers;
    struct VgiClientCapabilities caps;
    struct ClientSession *session;
    struct PendingBatch *head, *tail;
    struct ArrowSchema input_schema, output_schema;
    int64_t response_limit;
};
static struct ArrowBufferView bytes(const struct ArrowBuffer *b) {
    return (struct ArrowBufferView){{b->data}, b->size_bytes};
}
static char *view_text(struct ArrowStringView v) {
    if (v.size_bytes < 0 || (v.size_bytes && memchr(v.data, 0, (size_t)v.size_bytes))) return NULL;
    char *s = malloc((size_t)v.size_bytes + 1);
    if (s) {
        if (v.size_bytes) memcpy(s, v.data, (size_t)v.size_bytes);
        s[v.size_bytes] = 0;
    }
    return s;
}
static const char *header(struct VgiHttpClient *h, const char *name) {
    struct json_object *v = json_object_object_get(h->headers, name);
    return v ? json_object_get_string(v) : "";
}
static int yes(const char *s) { return !strcasecmp(s, "true"); }
static int64_t number(const char *s) {
    if (!*s) return -1;
    errno = 0;
    char *end;
    long long n = strtoll(s, &end, 10);
    return errno || *end || n < 0 ? -1 : n;
}
static void caps_read(struct VgiHttpClient *h) {
    h->caps.sticky_enabled = yes(header(h, "vgi-sticky-enabled"));
    h->caps.upload_url_support = yes(header(h, "vgi-upload-url-support"));
    h->caps.externalization_enabled = yes(header(h, "vgi-externalization-enabled"));
    h->caps.sticky_default_ttl = number(header(h, "vgi-sticky-default-ttl"));
    h->caps.max_request_bytes = number(header(h, "vgi-max-request-bytes"));
    h->caps.max_response_bytes = number(header(h, "vgi-max-response-bytes"));
    h->caps.max_externalized_response_bytes =
        number(header(h, "vgi-max-externalized-response-bytes"));
    h->caps.max_upload_bytes = number(header(h, "vgi-max-upload-bytes"));
    free((void *)h->caps.supported_encodings);
    free((void *)h->caps.sticky_echo_headers);
    h->caps.supported_encodings = strdup(header(h, "vgi-supported-encodings"));
    h->caps.sticky_echo_headers = strdup(header(h, "vgi-sticky-echo-headers"));
}
static void session_echo_clear(struct ClientSession *s) {
    for (size_t i = 0; i < s->echo_count; ++i) {
        free((void *)s->echo[i].name);
        free((void *)s->echo[i].value);
    }
    free(s->echo);
    s->echo = NULL;
    s->echo_count = 0;
}
static void session_headers(struct VgiHttpClient *h) {
    struct ClientSession *s = h->session;
    if (!s) return;
    const char *token = header(h, "vgi-session");
    if (yes(header(h, "vgi-session-close"))) {
        free(s->token);
        s->token = NULL;
        session_echo_clear(s);
    }
    if (*token) {
        free(s->token);
        s->token = strdup(token);
    }
    if (!*token) return;
    session_echo_clear(s);
    char *names = strdup(h->caps.sticky_echo_headers ? h->caps.sticky_echo_headers : ""),
         *save = NULL;
    if (!names) return;
    for (char *name = strtok_r(names, ",", &save); name; name = strtok_r(NULL, ",", &save)) {
        while (isspace((unsigned char)*name)) ++name;
        size_t n = strlen(name);
        while (n && isspace((unsigned char)name[n - 1])) name[--n] = 0;
        char *key = malloc(n + 10);
        if (!key) break;
        strcpy(key, "vgi-echo-");
        for (size_t i = 0; i <= n; ++i) key[9 + i] = (char)tolower((unsigned char)name[i]);
        const char *value = header(h, key);
        struct VgiClientHeader *next = realloc(s->echo, (s->echo_count + 1) * sizeof(*next));
        if (next && *value) {
            s->echo = next;
            s->echo[s->echo_count++] = (struct VgiClientHeader){strdup(name), strdup(value)};
        } else if (next)
            s->echo = next;
        free(key);
    }
    free(names);
}
struct ResponseSink {
    struct ArrowBuffer *body;
    int64_t limit;
    int failed;
};
static size_t receive_body(char *data, size_t size, size_t count, void *state) {
    struct ResponseSink *sink = state;
    if (size && count > SIZE_MAX / size) {
        sink->failed = EFBIG;
        return 0;
    }
    size_t n = size * count;
    if (n > (uint64_t)(sink->limit - sink->body->size_bytes)) {
        sink->failed = EFBIG;
        return 0;
    }
    sink->failed = ArrowBufferAppend(sink->body, data, (int64_t)n);
    return sink->failed ? 0 : n;
}
static size_t receive_header(char *data, size_t size, size_t count, void *state) {
    struct VgiHttpClient *h = state;
    size_t n = size * count;
    const char *colon = memchr(data, ':', n);
    if (!colon) return n;
    size_t key_length = (size_t)(colon - data), begin = key_length + 1, end = n;
    while (begin < end && isspace((unsigned char)data[begin])) ++begin;
    while (end > begin && isspace((unsigned char)data[end - 1])) --end;
    char *key = malloc(key_length + 1);
    if (!key) return 0;
    for (size_t i = 0; i < key_length; ++i) key[i] = (char)tolower((unsigned char)data[i]);
    key[key_length] = 0;
    json_object_object_add(h->headers, key,
                           json_object_new_string_len(data + begin, (int)(end - begin)));
    free(key);
    return n;
}
static int append_header(struct curl_slist **headers, const char *name, const char *value) {
    if (strpbrk(name, "\r\n:") || strpbrk(value, "\r\n")) return EINVAL;
    size_t n = strlen(name) + strlen(value) + 3;
    char *line = malloc(n);
    if (!line) return ENOMEM;
    snprintf(line, n, "%s: %s", name, value);
    struct curl_slist *next = curl_slist_append(*headers, line);
    free(line);
    if (!next) return ENOMEM;
    *headers = next;
    return 0;
}
static int exchange_http(struct VgiClient *c, const char *verb, const char *path,
                         const struct ArrowBuffer *input, struct ArrowBuffer *out, long *status) {
    struct VgiHttpClient *h = c->http;
    size_t n = strlen(h->base) + strlen(path) + 1;
    char *url = malloc(n);
    if (!url) return ENOMEM;
    snprintf(url, n, "%s%s", h->base, path);
    struct curl_slist *headers = NULL;
    int rc = append_header(&headers, "Accept", "application/vnd.apache.arrow.stream");
    if (!rc) rc = append_header(&headers, "Content-Type", "application/vnd.apache.arrow.stream");
    if (!rc) rc = append_header(&headers, "Accept-Encoding", "identity");
    if (!rc) rc = append_header(&headers, "X-VGI-Accept-Encoding", "zstd,gzip,identity");
    if (!rc) rc = append_header(&headers, "Expect", "");
    json_object_object_foreach(h->defaults, key, value) {
        if (!rc) rc = append_header(&headers, key, json_object_get_string(value));
    }
    if (!rc && c->options.accepted_max_response_bytes) {
        char value[32];
        snprintf(value, sizeof(value), "%lld", (long long)c->options.accepted_max_response_bytes);
        rc = append_header(&headers, "VGI-Accept-Max-Response-Bytes", value);
    }
    if (!rc && h->session) {
        if (h->session->token)
            rc = append_header(&headers, "VGI-Session", h->session->token);
        else
            rc = append_header(&headers, "VGI-Session-Accept", "true");
        for (size_t i = 0; !rc && i < h->session->echo_count; ++i)
            rc = append_header(&headers, h->session->echo[i].name, h->session->echo[i].value);
    }
    struct ArrowBuffer encoded, wire;
    ArrowBufferInit(&encoded);
    ArrowBufferInit(&wire);
    const struct ArrowBuffer *body = input;
    if (!rc && input && input->size_bytes && c->options.compression_level) {
        size_t bound = ZSTD_compressBound((size_t)input->size_bytes);
        rc = ArrowBufferResize(&encoded, (int64_t)bound, 0);
        if (!rc) {
            size_t compressed =
                ZSTD_compress(encoded.data, bound, input->data, (size_t)input->size_bytes,
                              c->options.compression_level);
            if (ZSTD_isError(compressed))
                rc = EINVAL;
            else {
                encoded.size_bytes = (int64_t)compressed;
                body = &encoded;
            }
        }
        if (!rc) rc = append_header(&headers, "Content-Encoding", "zstd");
    }
    json_object_put(h->headers);
    h->headers = json_object_new_object();
    struct ResponseSink sink = {&wire, h->response_limit, 0};
    if (!rc) {
        curl_easy_reset(h->curl);
        curl_easy_setopt(h->curl, CURLOPT_URL, url);
        curl_easy_setopt(h->curl, CURLOPT_CUSTOMREQUEST, verb);
        curl_easy_setopt(h->curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(h->curl, CURLOPT_POSTFIELDS, body ? (const char *)body->data : "");
        curl_easy_setopt(h->curl, CURLOPT_POSTFIELDSIZE_LARGE,
                         (curl_off_t)(body ? body->size_bytes : 0));
        curl_easy_setopt(h->curl, CURLOPT_TIMEOUT, (long)c->options.timeout_seconds);
        curl_easy_setopt(h->curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(h->curl, CURLOPT_PROTOCOLS_STR, "http,https");
        curl_easy_setopt(h->curl, CURLOPT_SSL_VERIFYPEER, 1L);
        curl_easy_setopt(h->curl, CURLOPT_SSL_VERIFYHOST, 2L);
        curl_easy_setopt(h->curl, CURLOPT_SSLVERSION, CURL_SSLVERSION_TLSv1_2);
        if (c->options.tls_ca_file)
            curl_easy_setopt(h->curl, CURLOPT_CAINFO, c->options.tls_ca_file);
        if (c->options.tls_certificate_file)
            curl_easy_setopt(h->curl, CURLOPT_SSLCERT, c->options.tls_certificate_file);
        if (c->options.tls_key_file)
            curl_easy_setopt(h->curl, CURLOPT_SSLKEY, c->options.tls_key_file);
        curl_easy_setopt(h->curl, CURLOPT_WRITEFUNCTION, receive_body);
        curl_easy_setopt(h->curl, CURLOPT_WRITEDATA, &sink);
        curl_easy_setopt(h->curl, CURLOPT_HEADERFUNCTION, receive_header);
        curl_easy_setopt(h->curl, CURLOPT_HEADERDATA, h);
        CURLcode result = curl_easy_perform(h->curl);
        if (result) {
            rc = sink.failed ? sink.failed : EIO;
            if (result == CURLE_PEER_FAILED_VERIFICATION)
                VgiClientSetError(c, "TLSVerificationError", "UNAUTHENTICATED",
                                  curl_easy_strerror(result));
        } else
            curl_easy_getinfo(h->curl, CURLINFO_RESPONSE_CODE, status);
    }
    if (!rc) {
        const char *encoding = header(h, "x-vgi-content-encoding");
        if (!*encoding) encoding = header(h, "content-encoding");
        rc = VgiHttpDecode(encoding, bytes(&wire), h->response_limit, out);
        session_headers(h);
    }
    curl_slist_free_all(headers);
    free(url);
    ArrowBufferReset(&encoded);
    ArrowBufferReset(&wire);
    if (rc && !c->error.type)
        VgiClientSetError(c, rc == EFBIG ? "ResponseTooLargeError" : "TransportError",
                          rc == EFBIG ? "RESOURCE_EXHAUSTED" : "UNAVAILABLE",
                          "HTTP transfer failed");
    return rc;
}
static int http_status(struct VgiClient *c, long status, const struct ArrowBuffer *body) {
    if (status >= 200 && status < 300) return 0;
    if (!strncasecmp(header(c->http, "content-type"), "application/vnd.apache.arrow.stream", 35))
        return 0;
    struct json_tokener *parser = json_tokener_new();
    struct json_object *value =
        parser ? json_tokener_parse_ex(parser, (const char *)body->data, (int)body->size_bytes)
               : NULL;
    const char *message =
        value ? json_object_get_string(json_object_object_get(value, "detail")) : NULL;
    char text[128];
    snprintf(text, sizeof(text), "HTTP request failed with status %ld", status);
    int rc = VgiClientSetError(
        c, status == 401 || status == 403 ? "AuthenticationError" : "ProtocolError",
        status == 401   ? "UNAUTHENTICATED"
        : status == 413 ? "RESOURCE_EXHAUSTED"
                        : "INVALID_ARGUMENT",
        message ? message : text);
    json_object_put(value);
    if (parser) json_tokener_free(parser);
    return rc;
}
int VgiHttpClientInit(struct VgiClient *c) {
    if (!c->options.target ||
        (strncmp(c->options.target, "http://", 7) && strncmp(c->options.target, "https://", 8)))
        return EINVAL;
    struct VgiHttpClient *h = calloc(1, sizeof(*h));
    if (!h) return ENOMEM;
    c->http = h;
    h->base = strdup(c->options.target);
    h->curl = curl_easy_init();
    h->defaults = json_object_new_object();
    h->headers = json_object_new_object();
    h->response_limit = INT64_C(1024) * 1024 * 1024;
    if (!h->base || !h->curl || !h->defaults || !h->headers) return ENOMEM;
    size_t n = strlen(h->base);
    while (n && h->base[n - 1] == '/') h->base[--n] = 0;
    for (size_t i = 0; i < c->options.header_count; ++i) {
        const struct VgiClientHeader *v = &c->options.headers[i];
        if (!v->name || !v->value || strpbrk(v->name, "\r\n:") || strpbrk(v->value, "\r\n"))
            return EINVAL;
        json_object_object_add(h->defaults, v->name, json_object_new_string(v->value));
    }
    struct ArrowBuffer body;
    ArrowBufferInit(&body);
    long status = 0;
    int rc = exchange_http(c, "OPTIONS", "/health", NULL, &body, &status);
    if (!rc) rc = http_status(c, status, &body);
    if (!rc) caps_read(h);
    ArrowBufferReset(&body);
    return rc;
}
static void pending_clear(struct VgiHttpClient *h) {
    while (h->head) {
        struct PendingBatch *p = h->head;
        h->head = p->next;
        VgiAnnotatedBatchReset(&p->value);
        free(p);
    }
    h->tail = NULL;
}
static void stream_clear(struct VgiClient *c) {
    struct VgiHttpClient *h = c->http;
    pending_clear(h);
    free(h->method);
    h->method = NULL;
    free(h->protocol);
    h->protocol = NULL;
    free(h->cursor);
    h->cursor = NULL;
    free(h->call_token);
    h->call_token = NULL;
    if (h->input_schema.release) ArrowSchemaRelease(&h->input_schema);
    if (h->output_schema.release) ArrowSchemaRelease(&h->output_schema);
    c->streaming = 0;
    c->eos = 1;
}
void VgiHttpClientReset(struct VgiClient *c) {
    struct VgiHttpClient *h = c->http;
    if (!h) return;
    while (h->session) VgiClientSessionEnd(c);
    stream_clear(c);
    if (h->curl) curl_easy_cleanup(h->curl);
    free(h->base);
    json_object_put(h->defaults);
    json_object_put(h->headers);
    free((void *)h->caps.supported_encodings);
    free((void *)h->caps.sticky_echo_headers);
    free(h);
    c->http = NULL;
}
static char *path_for(struct VgiClient *c, const char *protocol, const char *method,
                      const char *suffix) {
    char *p = curl_easy_escape(c->http->curl, protocol, 0),
         *m = curl_easy_escape(c->http->curl, method, 0);
    char *path = NULL;
    if (p && m) {
        size_t n = strlen(p) + strlen(m) + strlen(suffix) + 3;
        path = malloc(n);
        if (path) snprintf(path, n, "/%s/%s%s", p, m, suffix);
    }
    curl_free(p);
    curl_free(m);
    return path;
}
struct MemoryInput {
    const struct ArrowBuffer *data;
    int64_t offset;
};
static int memory_read(struct ArrowIpcInputStream *stream, uint8_t *out, int64_t size,
                       int64_t *read_out, struct ArrowError *error) {
    (void)error;
    struct MemoryInput *m = stream->private_data;
    int64_t left = m->data->size_bytes - m->offset;
    *read_out = size < left ? size : left;
    if (*read_out) memcpy(out, m->data->data + m->offset, (size_t)*read_out);
    m->offset += *read_out;
    return 0;
}
static void memory_release(struct ArrowIpcInputStream *stream) { stream->release = NULL; }
static int queue(struct VgiHttpClient *h, struct VgiAnnotatedBatch *value) {
    struct PendingBatch *p = calloc(1, sizeof(*p));
    if (!p) return ENOMEM;
    p->value = *value;
    memset(value, 0, sizeof(*value));
    ArrowBufferInit(&value->metadata);
    if (h->tail)
        h->tail->next = p;
    else
        h->head = p;
    h->tail = p;
    return 0;
}
static int pop(struct VgiHttpClient *h, struct VgiAnnotatedBatch *out) {
    if (!h->head) return 0;
    struct PendingBatch *p = h->head;
    h->head = p->next;
    if (!h->head) h->tail = NULL;
    *out = p->value;
    free(p);
    return 1;
}
static int parse_response(struct VgiClient *c, const struct ArrowBuffer *body, int streaming,
                          int initial, int has_header, struct VgiAnnotatedBatch *header_out) {
    struct VgiHttpClient *h = c->http;
    struct MemoryInput memory = {body, 0};
    int rc = 0, stream_index = 0;
    if (streaming) {
        free(h->cursor);
        h->cursor = NULL;
    }
    while (!rc && memory.offset < body->size_bytes) {
        struct ArrowIpcInputStream input = {memory_read, memory_release, &memory};
        struct VgiIpcReader reader = {0};
        struct ArrowSchema schema = {0};
        rc = VgiIpcReaderInit(&reader, &input);
        if (!rc) rc = VgiIpcReaderSchema(&reader, &schema, &c->arrow_error);
        int is_header = initial && has_header && stream_index == 0;
        if (!rc && streaming && !is_header) {
            if (h->output_schema.release) ArrowSchemaRelease(&h->output_schema);
            rc = ArrowSchemaDeepCopy(&schema, &h->output_schema);
        }
        while (!rc) {
            struct VgiAnnotatedBatch value = {0};
            ArrowBufferInit(&value.metadata);
            rc = VgiIpcReaderNext(&reader, &value.batch.array, &value.metadata, &c->arrow_error);
            if (rc || !value.batch.array.release) {
                VgiAnnotatedBatchReset(&value);
                break;
            }
            if (!rc) rc = ArrowSchemaDeepCopy(&schema, &value.batch.schema);
            if (!rc) rc = VgiClientResolve(c, &value);
            int kind = !rc ? VgiClientReadMetadata(c, (const char *)value.metadata.data) : 0;
            if (kind > 1) rc = kind;
            if (!rc && !kind) {
                struct ArrowStringView cursor = VgiMetadataGet((const char *)value.metadata.data,
                                                               VGI_RPC_STATE_KEY),
                                       call = VgiMetadataGet((const char *)value.metadata.data,
                                                             VGI_RPC_CALL_STATE_KEY);
                if (streaming && cursor.data) {
                    free(h->cursor);
                    h->cursor = view_text(cursor);
                    if (!h->cursor) rc = ENOMEM;
                }
                if (streaming && initial && call.data) {
                    free(h->call_token);
                    h->call_token = view_text(call);
                    if (!h->call_token) rc = ENOMEM;
                }
                int sentinel = streaming && !value.batch.array.length && cursor.data;
                if (!rc && is_header) {
                    if (header_out->batch.array.release)
                        rc = EINVAL;
                    else {
                        *header_out = value;
                        memset(&value, 0, sizeof(value));
                        ArrowBufferInit(&value.metadata);
                    }
                } else if (!rc && !sentinel)
                    rc = queue(h, &value);
            }
            VgiAnnotatedBatchReset(&value);
        }
        VgiIpcReaderReset(&reader);
        if (input.release) input.release(&input);
        if (schema.release) ArrowSchemaRelease(&schema);
        ++stream_index;
    }
    if (!rc && !stream_index) rc = EINVAL;
    if (!rc && has_header && initial && !header_out->batch.array.release) rc = EINVAL;
    if (streaming) c->eos = !h->cursor;
    if (!rc && streaming && !initial && c->exchange && !h->head && h->cursor) {
        struct VgiAnnotatedBatch value = {0};
        ArrowBufferInit(&value.metadata);
        rc = VgiBatchInit(&value.batch, &h->output_schema, &c->arrow_error);
        if (!rc) rc = VgiBatchFinish(&value.batch, &c->arrow_error);
        if (!rc) rc = queue(h, &value);
        VgiAnnotatedBatchReset(&value);
    }
    return rc;
}
static int allocate_upload(void *state, const struct ArrowSchema *schema,
                           struct VgiExternalUploadUrl *url, struct ArrowError *error) {
    (void)schema;
    (void)error;
    struct VgiClient *c = state;
    struct VgiExternalUploadUrl *urls = NULL;
    size_t count = 0;
    int rc = VgiClientUploadUrls(c, 1, &urls, &count);
    if (!rc && count != 1) rc = EINVAL;
    if (!rc) {
        *url = urls[0];
        memset(&urls[0], 0, sizeof(urls[0]));
    }
    VgiClientUploadUrlsFree(urls, count);
    return rc;
}
static int request_body(struct VgiClient *c, const char *method,
                        const struct VgiAnnotatedBatch *input, int continuation,
                        struct ArrowBuffer *out, char **protocol) {
    struct ArrowBuffer metadata;
    ArrowBufferInit(&metadata);
    int rc = continuation ? ArrowMetadataBuilderInit(&metadata, (const char *)input->metadata.data)
                          : VgiClientRequestMetadata(c, method, (const char *)input->metadata.data,
                                                     &metadata);
    if (!rc && continuation) {
        rc = VgiMetadataSet(&metadata, VGI_RPC_STATE_KEY, c->http->cursor);
        if (!rc && c->http->call_token)
            rc = VgiMetadataSet(&metadata, VGI_RPC_CALL_STATE_KEY, c->http->call_token);
    }
    if (!rc && protocol)
        *protocol = view_text(VgiMetadataGet((const char *)metadata.data, VGI_RPC_PROTOCOL_KEY));
    if (!rc)
        rc = VgiIpcWriteBatch(&input->batch.schema, &input->batch.array,
                              (const char *)metadata.data, out, &c->arrow_error);
    if (!rc && c->http->caps.upload_url_support && c->http->caps.max_request_bytes > 0 &&
        out->size_bytes > c->http->caps.max_request_bytes) {
        const struct VgiExternalOptions *configured = VgiRpcServerExternal(c->resolver);
        struct VgiExternalOptions external;
        if (configured)
            external = *configured;
        else {
            VgiExternalOptionsInit(&external);
            external.validate_url = NULL;
        }
        external.allocate = allocate_upload;
        external.storage_state = c;
        external.compression = c->options.compression_level ? "zstd" : "identity";
        struct VgiExternalRef ref = {0};
        rc = VgiExternalPublish(&external, &input->batch.schema, bytes(out), 1, &ref,
                                &c->arrow_error);
        struct VgiAnnotatedBatch pointer = {0};
        ArrowBufferInit(&pointer.metadata);
        if (!rc)
            rc = VgiExternalPointer(&input->batch.schema, &ref, &pointer.batch, &pointer.metadata,
                                    &c->arrow_error);
        /* Request routing is read before external payload resolution. Preserve
         * the request's dispatch and continuation metadata on the pointer too. */
        struct ArrowMetadataReader reader;
        if (!rc) rc = ArrowMetadataReaderInit(&reader, (const char *)metadata.data);
        while (!rc && reader.remaining_keys) {
            struct ArrowStringView key, value;
            rc = ArrowMetadataReaderRead(&reader, &key, &value);
            if (!rc) rc = ArrowMetadataBuilderSet(&pointer.metadata, key, value);
        }
        if (!rc) {
            ArrowBufferReset(out);
            rc = VgiIpcWriteBatch(&pointer.batch.schema, &pointer.batch.array,
                                  (const char *)pointer.metadata.data, out, &c->arrow_error);
        }
        VgiAnnotatedBatchReset(&pointer);
        VgiExternalRefReset(&ref);
    }
    ArrowBufferReset(&metadata);
    return rc;
}
int VgiHttpClientUnary(struct VgiClient *c, const char *method,
                       const struct VgiAnnotatedBatch *input, struct VgiAnnotatedBatch *out) {
    struct ArrowBuffer body, response;
    ArrowBufferInit(&body);
    ArrowBufferInit(&response);
    char *protocol = NULL, *path = NULL;
    long status = 0;
    int rc = request_body(c, method, input, 0, &body, &protocol);
    if (!rc && !(path = path_for(c, protocol, method, ""))) rc = ENOMEM;
    if (!rc) rc = exchange_http(c, "POST", path, &body, &response, &status);
    if (!rc) rc = http_status(c, status, &response);
    if (!rc) rc = parse_response(c, &response, 0, 0, 0, NULL);
    if (!rc && (!pop(c->http, out) || c->http->head)) rc = EINVAL;
    pending_clear(c->http);
    ArrowBufferReset(&body);
    ArrowBufferReset(&response);
    free(protocol);
    free(path);
    return rc;
}
int VgiHttpClientOpen(struct VgiClient *c, const char *method,
                      const struct VgiAnnotatedBatch *input, int exchange, int has_header,
                      struct VgiAnnotatedBatch *header_out) {
    struct VgiHttpClient *h = c->http;
    stream_clear(c);
    c->exchange = exchange;
    c->eos = 0;
    struct ArrowBuffer body, response;
    ArrowBufferInit(&body);
    ArrowBufferInit(&response);
    char *path = NULL;
    long status = 0;
    int rc = request_body(c, method, input, 0, &body, &h->protocol);
    h->method = strdup(method);
    if (!h->method) rc = ENOMEM;
    if (!rc && !(path = path_for(c, h->protocol, method, "/init"))) rc = ENOMEM;
    if (!rc) rc = exchange_http(c, "POST", path, &body, &response, &status);
    if (!rc) rc = http_status(c, status, &response);
    if (!rc) rc = parse_response(c, &response, 1, 1, has_header, header_out);
    if (!rc)
        c->streaming = 1;
    else
        stream_clear(c);
    ArrowBufferReset(&body);
    ArrowBufferReset(&response);
    free(path);
    return rc;
}
int VgiHttpClientStep(struct VgiClient *c, const struct VgiAnnotatedBatch *input,
                      struct VgiAnnotatedBatch *out, int *done) {
    struct VgiHttpClient *h = c->http;
    *done = 0;
    if (!c->exchange && pop(h, out)) return 0;
    if (c->eos) {
        *done = 1;
        stream_clear(c);
        return 0;
    }
    if (!h->input_schema.release) {
        int rc = ArrowSchemaDeepCopy(&input->batch.schema, &h->input_schema);
        if (rc) return rc;
    }
    struct ArrowBuffer body, response;
    ArrowBufferInit(&body);
    ArrowBufferInit(&response);
    long status = 0;
    int rc = request_body(c, h->method, input, 1, &body, NULL);
    char *path = !rc ? path_for(c, h->protocol, h->method, "/exchange") : NULL;
    if (!rc && !path) rc = ENOMEM;
    if (!rc) rc = exchange_http(c, "POST", path, &body, &response, &status);
    if (!rc) rc = http_status(c, status, &response);
    if (!rc) rc = parse_response(c, &response, 1, 0, 0, NULL);
    if (!rc && !pop(h, out)) {
        *done = 1;
        stream_clear(c);
    }
    if (rc) {
        *done = 1;
        stream_clear(c);
    }
    ArrowBufferReset(&body);
    ArrowBufferReset(&response);
    free(path);
    return rc;
}
int VgiHttpClientClose(struct VgiClient *c, int cancel) {
    struct VgiHttpClient *h = c->http;
    int rc = 0;
    if (cancel && !c->eos && h->cursor) {
        pending_clear(h);
        struct VgiAnnotatedBatch input = {0}, output = {0};
        ArrowBufferInit(&input.metadata);
        struct ArrowSchema empty = {0};
        if (!h->input_schema.release) rc = VgiSchemaRpcEmpty(&empty, &c->arrow_error);
        if (!rc)
            rc = VgiBatchInit(&input.batch, h->input_schema.release ? &h->input_schema : &empty,
                              &c->arrow_error);
        if (!rc) rc = VgiBatchFinish(&input.batch, &c->arrow_error);
        if (!rc) rc = VgiMetadataSet(&input.metadata, VGI_RPC_CANCEL_KEY, "1");
        int done;
        if (!rc) rc = VgiHttpClientStep(c, &input, &output, &done);
        VgiAnnotatedBatchReset(&input);
        VgiAnnotatedBatchReset(&output);
        if (empty.release) ArrowSchemaRelease(&empty);
    }
    stream_clear(c);
    return rc;
}
const char *VgiClientStreamToken(const struct VgiClient *c) {
    return c->http ? c->http->cursor : NULL;
}
const struct VgiClientCapabilities *VgiClientCapabilities(const struct VgiClient *c) {
    return c->http ? &c->http->caps : NULL;
}
int VgiClientSessionBegin(struct VgiClient *c, const char *token) {
    if (!c->http) return ENOTSUP;
    struct ClientSession *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    if (token && *token) {
        s->token = strdup(token);
        if (!s->token) {
            free(s);
            return ENOMEM;
        }
    }
    s->previous = c->http->session;
    c->http->session = s;
    return 0;
}
const char *VgiClientSessionToken(const struct VgiClient *c) {
    return c->http && c->http->session ? c->http->session->token : NULL;
}
const char *VgiClientSessionDetach(struct VgiClient *c) {
    if (c->http && c->http->session) c->http->session->detached = 1;
    return VgiClientSessionToken(c);
}
size_t VgiClientSessionEchoHeaders(const struct VgiClient *c, const struct VgiClientHeader **out) {
    *out = c->http && c->http->session ? c->http->session->echo : NULL;
    return c->http && c->http->session ? c->http->session->echo_count : 0;
}
int VgiClientSessionEnd(struct VgiClient *c) {
    if (!c->http) return ENOTSUP;
    struct ClientSession *s = c->http->session;
    if (!s) return 0;
    if (s->token && !s->detached) {
        struct ArrowBuffer response;
        ArrowBufferInit(&response);
        long status = 0;
        exchange_http(c, "DELETE", "/__session__", NULL, &response, &status);
        ArrowBufferReset(&response);
    }
    c->http->session = s->previous;
    free(s->token);
    session_echo_clear(s);
    free(s);
    return 0;
}
void VgiClientUploadUrlsFree(struct VgiExternalUploadUrl *urls, size_t count) {
    for (size_t i = 0; i < count; ++i) VgiExternalUploadUrlReset(&urls[i]);
    free(urls);
}
int VgiClientUploadUrls(struct VgiClient *c, int count, struct VgiExternalUploadUrl **out,
                        size_t *out_count) {
    if (!c->http || !out || *out || !out_count) return EINVAL;
    *out_count = 0;
    struct ArrowSchema schema = {0};
    ArrowSchemaInit(&schema);
    struct VgiAnnotatedBatch request = {0}, result = {0};
    ArrowBufferInit(&request.metadata);
    struct ArrowBuffer body, response;
    ArrowBufferInit(&body);
    ArrowBufferInit(&response);
    int rc = ArrowSchemaSetTypeStruct(&schema, 1);
    if (!rc) rc = ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT64);
    if (!rc) rc = ArrowSchemaSetName(schema.children[0], "count");
    if (!rc) rc = VgiBatchInit(&request.batch, &schema, &c->arrow_error);
    if (!rc) rc = VgiBatchSetInt(&request.batch, "count", count);
    if (!rc) rc = VgiBatchFinishRow(&request.batch, &c->arrow_error);
    if (!rc) rc = VgiMetadataSet(&request.metadata, VGI_RPC_RPC_METHOD_KEY, "__upload_url__");
    if (!rc) rc = VgiMetadataSet(&request.metadata, VGI_RPC_REQUEST_VERSION_KEY, "1");
    if (!rc)
        rc = VgiIpcWriteBatch(&request.batch.schema, &request.batch.array,
                              (const char *)request.metadata.data, &body, &c->arrow_error);
    long status = 0;
    if (!rc) rc = exchange_http(c, "POST", "/__upload_url__/init", &body, &response, &status);
    if (!rc) rc = http_status(c, status, &response);
    /* This control hop may occur while a stream has queued data. Preserve it. */
    struct PendingBatch *head = c->http->head, *tail = c->http->tail;
    c->http->head = c->http->tail = NULL;
    if (!rc) rc = parse_response(c, &response, 0, 0, 0, NULL);
    if (!rc && (!pop(c->http, &result) || c->http->head)) rc = EINVAL;
    pending_clear(c->http);
    c->http->head = head;
    c->http->tail = tail;
    if (!rc) rc = VgiBatchView(&result.batch, &c->arrow_error);
    const struct ArrowArrayView *uploads = !rc ? VgiBatchGet(&result.batch, "upload_url") : NULL,
                                *downloads =
                                    !rc ? VgiBatchGet(&result.batch, "download_url") : NULL,
                                *expires = !rc ? VgiBatchGet(&result.batch, "expires_at") : NULL;
    if (!rc && (!uploads || !downloads || result.batch.array.length < 0 ||
                result.batch.array.length > 100))
        rc = EINVAL;
    struct VgiExternalUploadUrl *urls =
        !rc ? calloc((size_t)result.batch.array.length, sizeof(*urls)) : NULL;
    if (!rc && !urls) rc = ENOMEM;
    size_t size = !rc ? (size_t)result.batch.array.length : 0;
    for (size_t i = 0; !rc && i < size; ++i) {
        urls[i].upload_url = view_text(ArrowArrayViewGetStringUnsafe(uploads, (int64_t)i));
        urls[i].download_url = view_text(ArrowArrayViewGetStringUnsafe(downloads, (int64_t)i));
        if (expires && !ArrowArrayViewIsNull(expires, (int64_t)i))
            urls[i].expires_at_us = ArrowArrayViewGetIntUnsafe(expires, (int64_t)i);
        if (!urls[i].upload_url || !urls[i].download_url) rc = ENOMEM;
    }
    if (!rc) {
        *out = urls;
        *out_count = size;
        urls = NULL;
    } else
        VgiClientUploadUrlsFree(urls, size);
    VgiAnnotatedBatchReset(&request);
    VgiAnnotatedBatchReset(&result);
    ArrowSchemaRelease(&schema);
    ArrowBufferReset(&body);
    ArrowBufferReset(&response);
    return rc;
}
