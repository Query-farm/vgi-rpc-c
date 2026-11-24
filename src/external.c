#define _POSIX_C_SOURCE 200809L
#include <curl/curl.h>
#include <errno.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "external_internal.h"
#include "http_codec.h"
#include "platform.h"
#include "rpc_internal.h"

static int https_only(void *state, const char *url) {
    (void)state;
    return strncasecmp(url, "https://", 8) ? EACCES : 0;
}
void VgiExternalOptionsInit(struct VgiExternalOptions *o) {
    memset(o, 0, sizeof(*o));
    o->threshold_bytes = 1024 * 1024;
    o->max_fetch_bytes = 1024 * 1024 * 1024;
    o->max_decompressed_bytes = 1024 * 1024 * 1024;
    o->max_redirects = 5;
    o->timeout_seconds = VgiStorageStallTimeoutSeconds();
    o->connect_timeout_seconds = 30;
    o->attempts = 3;
    o->validate_url = https_only;
}
unsigned VgiStorageStallTimeoutSeconds(void) {
    const char *text = getenv(VGI_RPC_STORAGE_STALL_TIMEOUT_ENV);
    if (!text || !*text) return 30;
    char *end = NULL;
    errno = 0;
    unsigned long value = strtoul(text, &end, 10);
    if (errno || *end || *text == '-' || value < 1 || value > 86400) return 30;
    return (unsigned)value;
}
void VgiExternalUploadUrlReset(struct VgiExternalUploadUrl *url) {
    free(url->upload_url);
    free(url->download_url);
    for (size_t i = 0; i < url->header_count; ++i) {
        free(url->headers[i].name);
        free(url->headers[i].value);
    }
    free(url->headers);
    free(url->object_id);
    memset(url, 0, sizeof(*url));
}
int VgiExternalUploadUrlAddHeader(struct VgiExternalUploadUrl *url, const char *name,
                                  const char *value) {
    if (!url || !name || !*name || !value || strpbrk(name, ":\r\n") || strpbrk(value, "\r\n"))
        return EINVAL;
    struct VgiExternalHeader *grown =
        realloc(url->headers, (url->header_count + 1) * sizeof(*url->headers));
    if (!grown) return ENOMEM;
    url->headers = grown;
    struct VgiExternalHeader *h = &url->headers[url->header_count];
    h->name = strdup(name);
    h->value = strdup(value);
    if (!h->name || !h->value) {
        free(h->name);
        free(h->value);
        return ENOMEM;
    }
    ++url->header_count;
    return 0;
}
int VgiExternalSha256Hex(const void *data, size_t size, char out[65]) {
    if ((!data && size) || !out) return EINVAL;
    unsigned char digest[32];
    if (crypto_hash_sha256(digest, (const unsigned char *)data, (unsigned long long)size))
        return EIO;
    sodium_bin2hex(out, 65, digest, sizeof(digest));
    return 0;
}
void VgiExternalRefReset(struct VgiExternalRef *ref) {
    free(ref->location);
    memset(ref, 0, sizeof(*ref));
}
struct Download {
    struct ArrowBuffer body;
    char encoding[32];
    int64_t limit;
    int error;
    /* Stall detection: when bytes last moved, in either direction. */
    CURL *curl;
    unsigned stall_seconds;
    double last_progress;
    curl_off_t last_down, last_up;
    int stalled;
};
static CURLcode curl_initialized;
static void curl_start(void) { curl_initialized = curl_global_init(CURL_GLOBAL_DEFAULT); }
#if defined(_WIN32)
static INIT_ONCE curl_once = INIT_ONCE_STATIC_INIT;
static BOOL CALLBACK curl_start_windows(PINIT_ONCE once, PVOID parameter, PVOID *context) {
    (void)once;
    (void)parameter;
    (void)context;
    curl_start();
    return TRUE;
}
static void ensure_curl(void) { InitOnceExecuteOnce(&curl_once, curl_start_windows, NULL, NULL); }
#else
#include <pthread.h>
static pthread_once_t curl_once = PTHREAD_ONCE_INIT;
static void ensure_curl(void) { pthread_once(&curl_once, curl_start); }
#endif
static size_t receive_body(char *data, size_t size, size_t count, void *state) {
    struct Download *download = state;
    if (size && count > SIZE_MAX / size) {
        download->error = EOVERFLOW;
        return 0;
    }
    size_t n = size * count;
    if (n > (uint64_t)(download->limit - download->body.size_bytes)) {
        download->error = EFBIG;
        return 0;
    }
    download->error = ArrowBufferAppend(&download->body, data, (int64_t)n);
    return download->error ? 0 : n;
}
static size_t receive_header(char *data, size_t size, size_t count, void *state) {
    struct Download *download = state;
    if (size && count > SIZE_MAX / size) return 0;
    size_t n = size * count;
    const char *label = "Content-Encoding:";
    size_t prefix = strlen(label);
    if (n >= prefix && !strncasecmp(data, label, prefix)) {
        size_t end = n;
        while (prefix < end && (data[prefix] == ' ' || data[prefix] == '\t')) ++prefix;
        while (end > prefix &&
               (data[end - 1] == '\r' || data[end - 1] == '\n' || data[end - 1] == ' '))
            --end;
        if (end - prefix >= sizeof(download->encoding)) {
            download->error = EINVAL;
            return 0;
        }
        memcpy(download->encoding, data + prefix, end - prefix);
        download->encoding[end - prefix] = 0;
    }
    return n;
}
/* Abort when no bytes have moved, either way, for stall_seconds once the
 * connection is up (connect has its own timeout). libcurl calls this about
 * once a second even while idle. This is exact where CURLOPT_LOW_SPEED_TIME,
 * which averages over a few seconds of samples, overshoots by several. */
static int on_progress(void *state, curl_off_t down_total, curl_off_t down_now, curl_off_t up_total,
                       curl_off_t up_now) {
    (void)down_total;
    (void)up_total;
    struct Download *d = state;
    double now = VgiMonotonicSeconds();
    curl_off_t connected = 0;
    curl_easy_getinfo(d->curl, CURLINFO_PRETRANSFER_TIME_T, &connected);
    if (!connected || down_now != d->last_down || up_now != d->last_up || !d->last_progress) {
        d->last_down = down_now;
        d->last_up = up_now;
        d->last_progress = now;
        return 0;
    }
    if (now - d->last_progress >= (double)d->stall_seconds) {
        d->stalled = 1;
        return 1;
    }
    return 0;
}
/* S3-style error bodies name the refusal in <Code>; surface it (never the URL). */
static void storage_error_code(const struct ArrowBuffer *body, char *out, size_t size) {
    *out = 0;
    const char *open = "<Code>", *close = "</Code>";
    if (!body->size_bytes || size < 2) return;
    const char *start = NULL, *end = NULL, *data = (const char *)body->data;
    size_t n = (size_t)body->size_bytes;
    for (size_t i = 0; i + 6 <= n && !start; ++i)
        if (!memcmp(data + i, open, 6)) start = data + i + 6;
    if (!start) return;
    for (const char *p = start; p + 7 <= data + n && !end; ++p)
        if (!memcmp(p, close, 7)) end = p;
    if (!end || end == start || (size_t)(end - start) >= size) return;
    for (const char *p = start; p < end; ++p)
        if (!((*p >= 'A' && *p <= 'Z') || (*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9')))
            return;
    memcpy(out, start, (size_t)(end - start));
    out[end - start] = 0;
}
/* One attempt of one hop. *retry is set for a transient failure (transport
 * error, stall, 429, 5xx); *next receives a redirect target. */
static int perform_once(const struct VgiExternalOptions *o, const char *method, const char *current,
                        struct ArrowBufferView body, const char *encoding,
                        const struct VgiExternalHeader *extra, size_t extra_count,
                        long connect_timeout, struct ArrowBuffer *out, char **next, int *retry,
                        unsigned attempt, struct ArrowError *error) {
    CURL *curl = curl_easy_init();
    if (!curl) {
        return ENOMEM;
    }
    struct Download download = {0};
    ArrowBufferInit(&download.body);
    download.limit = o->max_fetch_bytes;
    struct curl_slist *headers = NULL;
    curl_easy_setopt(curl, CURLOPT_URL, current);
    curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, "http,https");
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    /* No CURLOPT_TIMEOUT: a whole-request deadline would kill a slow
     * transfer that is still progressing. Connect is bounded on its own
     * (TCP and TLS); after that the request fails only when no bytes move
     * for timeout_seconds (a stall; on_progress). */
    download.curl = curl;
    download.stall_seconds = o->timeout_seconds;
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, connect_timeout);
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
    curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, on_progress);
    curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &download);
    curl_easy_setopt(curl, CURLOPT_HTTP_CONTENT_DECODING, 0L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, receive_body);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &download);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, receive_header);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &download);
    if (strcmp(method, "GET")) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.size_bytes ? body.data.data : "");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)body.size_bytes);
        int typed = 0;
        for (size_t i = 0; i < extra_count; ++i)
            if (!strcasecmp(extra[i].name, "Content-Type")) typed = 1;
        /* A PUT sends the provider's signed content type when it has one,
         * otherwise application/octet-stream (as the reference does). */
        if (!typed)
            headers = curl_slist_append(headers, !strcmp(method, "POST")
                                                     ? "Content-Type: application/json"
                                                     : "Content-Type: application/octet-stream");
        if (encoding && *encoding && strcmp(encoding, "identity")) {
            char field[96];
            snprintf(field, sizeof(field), "Content-Encoding: %s", encoding);
            headers = curl_slist_append(headers, field);
        }
        /* Signed URLs cover the length; never let curl switch to chunked
         * transfer or stall on 100-continue. */
        headers = curl_slist_append(headers, "Expect:");
        for (size_t i = 0; i < extra_count; ++i) {
            size_t length = strlen(extra[i].name) + strlen(extra[i].value) + 3;
            char *field = malloc(length);
            if (!field) continue;
            snprintf(field, length, "%s: %s", extra[i].name, extra[i].value);
            headers = curl_slist_append(headers, field);
            free(field);
        }
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    }
    CURLcode result = curl_easy_perform(curl);
    long status = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
    char *redirect = NULL;
    int rc = 0;
    curl_easy_getinfo(curl, CURLINFO_REDIRECT_URL, &redirect);
    if (download.error) {
        rc = download.error;
        ArrowErrorSet(error, rc == EFBIG ? "External response exceeds max_fetch_bytes"
                                         : "Invalid external response");
    } else if (result != CURLE_OK) {
        /* Connect errors, resets and stalls are transient. */
        rc = EIO;
        *retry = 1;
        if (download.stalled)
            ArrowErrorSet(error,
                          "External HTTP request stalled: no bytes moved for %u s (attempt %u)",
                          o->timeout_seconds, attempt);
        else
            ArrowErrorSet(error, "External HTTP request failed (attempt %u): %s", attempt,
                          curl_easy_strerror(result));
    } else if (status >= 300 && status < 400 && redirect && !strcmp(method, "GET")) {
        if (!(*next = strdup(redirect))) rc = ENOMEM;
    } else if (status < 200 || status >= 300) {
        if (status == 429 || status >= 500) *retry = 1;
        /* 404 is the one status callers must tell apart: the object store
         * contract reports a missing object distinctly from transient
         * failures. */
        rc = status == 404 ? ENOENT : EIO;
        char code[64];
        storage_error_code(&download.body, code, sizeof(code));
        if (!strcmp(method, "PUT"))
            ArrowErrorSet(error, "PUT to upload URL failed: HTTP %ld%s%s", status, *code ? " " : "",
                          code);
        else if (*code)
            ArrowErrorSet(error, "External HTTP request returned status %ld (%s)", status, code);
        else
            ArrowErrorSet(error, "External HTTP request returned status %ld", status);
    } else {
        rc = VgiHttpDecode(download.encoding,
                           (struct ArrowBufferView){{download.body.data}, download.body.size_bytes},
                           o->max_decompressed_bytes, out);
        if (rc)
            ArrowErrorSet(error, rc == EFBIG ? "External response exceeds max_decompressed_bytes"
                                             : "Invalid external content encoding");
    }
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    ArrowBufferReset(&download.body);
    return rc;
}
/* 0.25 s doubling per attempt, capped at 5 s, jittered to [0.5, 1.0). */
static void storage_backoff(unsigned attempt) {
    double delay = 0.25 * (double)(1u << (attempt < 6 ? attempt - 1 : 5));
    if (delay > 5) delay = 5;
    delay *= 0.5 + (double)randombytes_uniform(1000) / 2000.0;
    VgiSleepMilliseconds((unsigned)(delay * 1000));
}
int VgiExternalHttp(const struct VgiExternalOptions *o, const char *method, const char *url,
                    struct ArrowBufferView body, const char *encoding,
                    const struct VgiExternalHeader *extra, size_t extra_count,
                    struct ArrowBuffer *out, struct ArrowError *error) {
    if (!o || !method || !url || !out || out->size_bytes || o->max_fetch_bytes < 1 ||
        o->max_decompressed_bytes < 1 || !o->timeout_seconds || body.size_bytes < 0)
        return EINVAL;
    ensure_curl();
    if (curl_initialized) return EIO;
    char *current = strdup(url);
    if (!current) return ENOMEM;
    int rc = 0;
    for (unsigned hop = 0; !rc; ++hop) {
        if (o->validate_url && o->validate_url(o->validate_state, current)) {
            ArrowErrorSet(error, "External URL rejected by validator");
            rc = EACCES;
            break;
        }
        if (strncasecmp(current, "http://", 7) && strncasecmp(current, "https://", 8)) {
            ArrowErrorSet(error, "External URL scheme is unsupported");
            rc = ENOTSUP;
            break;
        }
        const unsigned attempts = o->attempts ? o->attempts : 3;
        const long connect_timeout =
            (long)(o->connect_timeout_seconds ? o->connect_timeout_seconds : 30);
        char *next = NULL;
        for (unsigned attempt = 0;; ++attempt) {
            if (attempt) storage_backoff(attempt);
            int retry = 0;
            rc = perform_once(o, method, current, body, encoding, extra, extra_count,
                              connect_timeout, out, &next, &retry, attempt + 1, error);
            if (!retry || attempt + 1 >= attempts) break;
        }
        if (rc) {
            free(next);
            next = NULL;
        } else if (next && hop >= o->max_redirects) {
            free(next);
            next = NULL;
            rc = ELOOP;
            ArrowErrorSet(error, "External redirect limit exceeded");
        }
        free(current);
        current = next;
        if (!next) break;
    }
    free(current);
    if (rc) ArrowBufferReset(out);
    return rc;
}
int VgiExternalFetch(const struct VgiExternalOptions *o, const char *url, const char *sha256,
                     struct ArrowBuffer *out, struct ArrowError *error) {
    int rc = VgiExternalHttp(o, "GET", url, (struct ArrowBufferView){{NULL}, 0}, NULL, NULL, 0, out,
                             error);
    if (!rc && sha256 && *sha256) {
        unsigned char expected[32], actual[32];
        size_t n = 0;
        if (strlen(sha256) != 64 || sodium_hex2bin(expected, 32, sha256, 64, NULL, &n, NULL) ||
            n != 32)
            rc = EINVAL;
        if (!rc) {
            crypto_hash_sha256(actual, out->data, (unsigned long long)out->size_bytes);
            if (sodium_memcmp(expected, actual, 32)) rc = EINVAL;
        }
        if (rc) {
            ArrowErrorSet(error, "ExternalLocation sha256 mismatch (SHA-256 checksum mismatch)");
            ArrowBufferReset(out);
        }
    }
    return rc;
}
int VgiExternalPublish(const struct VgiExternalOptions *o, const struct ArrowSchema *schema,
                       struct ArrowBufferView ipc, int include_sha256, struct VgiExternalRef *out,
                       struct ArrowError *error) {
    if (!o || !o->allocate || !out || out->location || ipc.size_bytes < 0) return EINVAL;
    (void)schema;
    struct VgiExternalUploadUrl urls = {0};
    struct ArrowBuffer encoded, response;
    ArrowBufferInit(&encoded);
    ArrowBufferInit(&response);
    char body_sha256[65];
    int rc = VgiHttpEncode(o->compression ? o->compression : "identity", ipc, &encoded);
    if (!rc && encoded.size_bytes < 1) rc = EINVAL;
    if (!rc) rc = VgiExternalSha256Hex(encoded.data, (size_t)encoded.size_bytes, body_sha256);
    if (!rc) rc = o->allocate(o->storage_state, encoded.size_bytes, body_sha256, &urls, error);
    if (!rc && (!urls.upload_url || !urls.download_url)) rc = EINVAL;
    for (size_t i = 0; !rc && i < urls.header_count; ++i)
        if (!strcasecmp(urls.headers[i].name, "Content-Length") ||
            !strcasecmp(urls.headers[i].name, "Transfer-Encoding")) {
            ArrowErrorSet(error, "Upload URL headers must not set %s", urls.headers[i].name);
            rc = EINVAL;
        }
    if (!rc)
        rc = VgiExternalHttp(o, "PUT", urls.upload_url,
                             (struct ArrowBufferView){{encoded.data}, encoded.size_bytes},
                             o->compression, urls.headers, urls.header_count, &response, error);
    if (!rc) {
        out->location = urls.download_url;
        urls.download_url = NULL;
        if (include_sha256) {
            unsigned char digest[32];
            crypto_hash_sha256(digest, ipc.data.as_uint8, (unsigned long long)ipc.size_bytes);
            sodium_bin2hex(out->sha256, sizeof(out->sha256), digest, sizeof(digest));
        }
    }
    VgiExternalUploadUrlReset(&urls);
    ArrowBufferReset(&encoded);
    ArrowBufferReset(&response);
    return rc;
}
int VgiExternalPointer(const struct ArrowSchema *schema, const struct VgiExternalRef *ref,
                       struct VgiBatch *out, struct ArrowBuffer *metadata,
                       struct ArrowError *error) {
    if (!ref || !ref->location) return EINVAL;
    int rc = VgiBatchInit(out, schema, error);
    if (!rc) rc = VgiBatchFinish(out, error);
    if (!rc) rc = VgiMetadataSet(metadata, "vgi_rpc.location", ref->location);
    if (!rc && *ref->sha256) rc = VgiMetadataSet(metadata, "vgi_rpc.location.sha256", ref->sha256);
    if (rc) {
        VgiBatchReset(out);
        ArrowBufferReset(metadata);
    }
    return rc;
}

struct PayloadInput {
    const struct ArrowBuffer *buffer;
    int64_t position;
};
static int payload_read(struct ArrowIpcInputStream *stream, uint8_t *out, int64_t length,
                        int64_t *read_out, struct ArrowError *error) {
    (void)error;
    struct PayloadInput *input = stream->private_data;
    int64_t left = input->buffer->size_bytes - input->position;
    if (length < 0) return EINVAL;
    *read_out = length < left ? length : left;
    if (*read_out) memcpy(out, input->buffer->data + input->position, (size_t)*read_out);
    input->position += *read_out;
    return 0;
}
static void payload_release(struct ArrowIpcInputStream *stream) { stream->release = NULL; }
static char *metadata_text(struct ArrowStringView view) {
    if (view.size_bytes < 0 || (view.size_bytes && memchr(view.data, 0, (size_t)view.size_bytes)))
        return NULL;
    char *text = malloc((size_t)view.size_bytes + 1);
    if (text) {
        if (view.size_bytes) memcpy(text, view.data, (size_t)view.size_bytes);
        text[view.size_bytes] = 0;
    }
    return text;
}
static int schema_equal(const struct ArrowSchema *a, const struct ArrowSchema *b) {
    if (strcmp(a->format, b->format) || a->flags != b->flags ||
        strcmp(a->name ? a->name : "", b->name ? b->name : "") || a->n_children != b->n_children ||
        (!!a->dictionary != !!b->dictionary))
        return 0;
    for (int64_t i = 0; i < a->n_children; ++i)
        if (!schema_equal(a->children[i], b->children[i])) return 0;
    return !a->dictionary || schema_equal(a->dictionary, b->dictionary);
}
int VgiExternalResolveOptions(const struct VgiExternalOptions *options, int require_sha256,
                              struct VgiBatch *batch, struct ArrowBuffer *metadata,
                              struct ArrowError *error, VgiExternalLogFn receive_log,
                              void *log_state) {
    struct ArrowStringView location =
        VgiMetadataGet((const char *)metadata->data, VGI_RPC_LOCATION_KEY);
    if (!location.data) return 0;
    if (!options) {
        ArrowErrorSet(error, "External locations are not configured");
        return EINVAL;
    }
    if (batch->array.length) {
        ArrowErrorSet(error, "External pointer must have zero rows");
        return EINVAL;
    }
    struct ArrowStringView digest_view =
        VgiMetadataGet((const char *)metadata->data, VGI_RPC_LOCATION_SHA256_KEY);
    if (require_sha256 && (!digest_view.data || !digest_view.size_bytes)) {
        ArrowErrorSet(error,
                      "Request pointer is missing vgi_rpc.location.sha256 (required on "
                      "request pointers since vgi-rpc upload-URL protocol v2)");
        return EINVAL;
    }
    if (require_sha256) {
        int hex = digest_view.size_bytes == 64;
        for (int64_t i = 0; hex && i < 64; ++i) {
            char ch = digest_view.data[i];
            hex = (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
        }
        if (!hex) {
            ArrowErrorSet(error,
                          "Request pointer vgi_rpc.location.sha256 is not 64 lowercase "
                          "hex characters");
            return EINVAL;
        }
    }
    char *url = metadata_text(location);
    char *digest = metadata_text(digest_view);
    int rc = url && digest ? 0 : EINVAL;
    double start = VgiMonotonicSeconds();
    struct ArrowBuffer payload, inner_metadata, value_metadata;
    ArrowBufferInit(&payload);
    ArrowBufferInit(&inner_metadata);
    ArrowBufferInit(&value_metadata);
    if (!rc) rc = VgiExternalFetch(options, url, digest, &payload, error);
    double end = VgiMonotonicSeconds();
    struct PayloadInput input = {&payload, 0};
    struct ArrowIpcInputStream source = {payload_read, payload_release, &input};
    struct VgiIpcReader reader = {0};
    struct VgiBatch incoming = {0}, value = {0};
    if (!rc) rc = VgiIpcReaderInit(&reader, &source);
    if (!rc) rc = VgiIpcReaderSchema(&reader, &incoming.schema, error);
    if (!rc && !schema_equal(&incoming.schema, &batch->schema)) {
        ArrowErrorSet(error, "External payload schema differs from its pointer");
        rc = EINVAL;
    }
    while (!rc) {
        rc = VgiIpcReaderNext(&reader, &incoming.array, &inner_metadata, error);
        if (rc || !incoming.array.release) break;
        if (VgiMetadataGet((const char *)inner_metadata.data, VGI_RPC_LOCATION_KEY).data) {
            ArrowErrorSet(error, "Nested external pointers are not allowed");
            rc = EINVAL;
            break;
        }
        struct ArrowStringView level =
            VgiMetadataGet((const char *)inner_metadata.data, VGI_RPC_LOG_LEVEL_KEY);
        if (level.data) {
            char *level_text = metadata_text(level);
            char *message = metadata_text(
                VgiMetadataGet((const char *)inner_metadata.data, VGI_RPC_LOG_MESSAGE_KEY));
            char *extra = metadata_text(
                VgiMetadataGet((const char *)inner_metadata.data, VGI_RPC_LOG_EXTRA_KEY));
            if (!level_text || !message || !extra || incoming.array.length)
                rc = EINVAL;
            else if (!strcmp(level_text, "EXCEPTION")) {
                ArrowErrorSet(error, "External payload contains an exception");
                rc = EINVAL;
            } else if (receive_log)
                rc = receive_log(log_state, level_text, message, *extra ? extra : NULL);
            free(level_text);
            free(message);
            free(extra);
        } else if (value.array.release) {
            ArrowErrorSet(error, "External payload contains multiple data batches");
            rc = EINVAL;
        } else {
            rc = ArrowSchemaDeepCopy(&incoming.schema, &value.schema);
            if (!rc) ArrowArrayMove(&incoming.array, &value.array);
            if (!rc) ArrowBufferMove(&inner_metadata, &value_metadata);
        }
        if (incoming.array.release) ArrowArrayRelease(&incoming.array);
    }
    if (!rc && (!value.array.release || input.position != payload.size_bytes)) {
        ArrowErrorSet(error, "External payload must contain exactly one complete data stream");
        rc = EINVAL;
    }
    if (!rc) rc = VgiMetadataSet(&value_metadata, VGI_RPC_LOCATION_SOURCE_KEY, url);
    if (!rc) {
        char duration[64];
        double milliseconds = (end - start) * 1000.0;
        snprintf(duration, sizeof(duration), "%.6f", milliseconds);
        rc = VgiMetadataSet(&value_metadata, VGI_RPC_LOCATION_FETCH_MS_KEY, duration);
    }
    if (!rc) {
        VgiBatchReset(batch);
        *batch = value;
        memset(&value, 0, sizeof(value));
        ArrowBufferReset(metadata);
        ArrowBufferMove(&value_metadata, metadata);
    } else if (!error->message[0])
        ArrowErrorSet(error, "Invalid external payload");
    VgiBatchReset(&incoming);
    VgiBatchReset(&value);
    VgiIpcReaderReset(&reader);
    if (source.release) source.release(&source);
    ArrowBufferReset(&payload);
    ArrowBufferReset(&inner_metadata);
    ArrowBufferReset(&value_metadata);
    free(url);
    free(digest);
    return rc;
}

#if !defined(VGI_RPC_CLIENT_ONLY)
static int server_log(void *state, const char *level, const char *message, const char *extra) {
    return VgiRpcLogExtra((struct VgiRpcCall *)state, level, message, extra);
}

int VgiExternalResolve(struct VgiRpcCall *call, struct VgiBatch *batch,
                       struct ArrowBuffer *metadata) {
    /* Request pointers must carry the payload digest (upload protocol v2). */
    int rc = VgiExternalResolveOptions(VgiRpcServerExternal(call->server), 1, batch, metadata,
                                       &call->error, server_log, call);
    if (rc) {
        call->error_type = "ExternalLocationError";
        call->error_code = "INVALID_ARGUMENT";
    }
    return rc;
}
#endif
