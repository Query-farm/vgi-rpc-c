#define _POSIX_C_SOURCE 200809L
#include <curl/curl.h>
#include <errno.h>
#include <pthread.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#include "external_internal.h"
#include "http_codec.h"
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
    o->timeout_seconds = 30;
    o->validate_url = https_only;
}
void VgiExternalUploadUrlReset(struct VgiExternalUploadUrl *url) {
    free(url->upload_url);
    free(url->download_url);
    memset(url, 0, sizeof(*url));
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
};
static pthread_once_t curl_once = PTHREAD_ONCE_INIT;
static CURLcode curl_initialized;
static void curl_start(void) { curl_initialized = curl_global_init(CURL_GLOBAL_DEFAULT); }
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
int VgiExternalHttp(const struct VgiExternalOptions *o, const char *method, const char *url,
                    struct ArrowBufferView body, const char *encoding, struct ArrowBuffer *out,
                    struct ArrowError *error) {
    if (!o || !method || !url || !out || out->size_bytes || o->max_fetch_bytes < 1 ||
        o->max_decompressed_bytes < 1 || !o->timeout_seconds || body.size_bytes < 0)
        return EINVAL;
    pthread_once(&curl_once, curl_start);
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
        CURL *curl = curl_easy_init();
        if (!curl) {
            rc = ENOMEM;
            break;
        }
        struct Download download = {0};
        ArrowBufferInit(&download.body);
        download.limit = o->max_fetch_bytes;
        struct curl_slist *headers = NULL;
        curl_easy_setopt(curl, CURLOPT_URL, current);
        curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, "http,https");
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)o->timeout_seconds);
        curl_easy_setopt(curl, CURLOPT_HTTP_CONTENT_DECODING, 0L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, receive_body);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &download);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, receive_header);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, &download);
        if (strcmp(method, "GET")) {
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.size_bytes ? body.data.data : "");
            curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)body.size_bytes);
            headers = curl_slist_append(headers,
                                        !strcmp(method, "POST")
                                            ? "Content-Type: application/json"
                                            : "Content-Type: application/vnd.apache.arrow.stream");
            if (encoding && *encoding) {
                char field[96];
                snprintf(field, sizeof(field), "Content-Encoding: %s", encoding);
                headers = curl_slist_append(headers, field);
            }
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        }
        CURLcode result = curl_easy_perform(curl);
        long status = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
        char *redirect = NULL, *next = NULL;
        curl_easy_getinfo(curl, CURLINFO_REDIRECT_URL, &redirect);
        if (download.error) {
            rc = download.error;
            ArrowErrorSet(error, rc == EFBIG ? "External response exceeds max_fetch_bytes"
                                             : "Invalid external response");
        } else if (result != CURLE_OK) {
            rc = EIO;
            ArrowErrorSet(error, "External HTTP request failed: %s", curl_easy_strerror(result));
        } else if (status >= 300 && status < 400 && redirect && !strcmp(method, "GET")) {
            if (hop >= o->max_redirects) {
                rc = ELOOP;
                ArrowErrorSet(error, "External redirect limit exceeded");
            } else if (!(next = strdup(redirect)))
                rc = ENOMEM;
        } else if (status < 200 || status >= 300) {
            rc = EIO;
            ArrowErrorSet(error, "External HTTP request returned status %ld", status);
        } else {
            rc = VgiHttpDecode(
                download.encoding,
                (struct ArrowBufferView){{download.body.data}, download.body.size_bytes},
                o->max_decompressed_bytes, out);
            if (rc)
                ArrowErrorSet(error, rc == EFBIG
                                         ? "External response exceeds max_decompressed_bytes"
                                         : "Invalid external content encoding");
        }
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        ArrowBufferReset(&download.body);
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
    int rc = VgiExternalHttp(o, "GET", url, (struct ArrowBufferView){{NULL}, 0}, NULL, out, error);
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
            ArrowErrorSet(error, "External payload checksum mismatch");
            ArrowBufferReset(out);
        }
    }
    return rc;
}
int VgiExternalPublish(const struct VgiExternalOptions *o, const struct ArrowSchema *schema,
                       struct ArrowBufferView ipc, int include_sha256, struct VgiExternalRef *out,
                       struct ArrowError *error) {
    if (!o || !o->allocate || !out || out->location || ipc.size_bytes < 0) return EINVAL;
    struct VgiExternalUploadUrl urls = {0};
    struct ArrowBuffer encoded, response;
    ArrowBufferInit(&encoded);
    ArrowBufferInit(&response);
    int rc = VgiHttpEncode(o->compression ? o->compression : "identity", ipc, &encoded);
    if (!rc) rc = o->allocate(o->storage_state, schema, &urls, error);
    if (!rc && (!urls.upload_url || !urls.download_url)) rc = EINVAL;
    if (!rc)
        rc = VgiExternalHttp(o, "PUT", urls.upload_url,
                             (struct ArrowBufferView){{encoded.data}, encoded.size_bytes},
                             o->compression, &response, error);
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
int VgiExternalResolve(struct VgiRpcCall *call, struct VgiBatch *batch,
                       struct ArrowBuffer *metadata) {
    struct ArrowStringView location =
        VgiMetadataGet((const char *)metadata->data, VGI_RPC_LOCATION_KEY);
    if (!location.data) return 0;
    const struct VgiExternalOptions *options = VgiRpcServerExternal(call->server);
    if (!options)
        return VgiRpcFail(call, "RuntimeError", "FAILED_PRECONDITION", "",
                          "External locations are not configured");
    if (batch->array.length)
        return VgiRpcFail(call, "ValueError", "INVALID_ARGUMENT", "",
                          "External pointer must have zero rows");
    char *url = metadata_text(location);
    char *digest =
        metadata_text(VgiMetadataGet((const char *)metadata->data, VGI_RPC_LOCATION_SHA256_KEY));
    int rc = url && digest ? 0 : EINVAL;
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    struct ArrowBuffer payload, inner_metadata, value_metadata;
    ArrowBufferInit(&payload);
    ArrowBufferInit(&inner_metadata);
    ArrowBufferInit(&value_metadata);
    if (!rc) rc = VgiExternalFetch(options, url, digest, &payload, &call->error);
    clock_gettime(CLOCK_MONOTONIC, &end);
    struct PayloadInput input = {&payload, 0};
    struct ArrowIpcInputStream source = {payload_read, payload_release, &input};
    struct VgiIpcReader reader = {0};
    struct VgiBatch incoming = {0}, value = {0};
    if (!rc) rc = VgiIpcReaderInit(&reader, &source);
    if (!rc) rc = VgiIpcReaderSchema(&reader, &incoming.schema, &call->error);
    if (!rc && !schema_equal(&incoming.schema, &batch->schema)) {
        ArrowErrorSet(&call->error, "External payload schema differs from its pointer");
        rc = EINVAL;
    }
    while (!rc) {
        rc = VgiIpcReaderNext(&reader, &incoming.array, &inner_metadata, &call->error);
        if (rc || !incoming.array.release) break;
        if (VgiMetadataGet((const char *)inner_metadata.data, VGI_RPC_LOCATION_KEY).data) {
            ArrowErrorSet(&call->error, "Nested external pointers are not allowed");
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
                ArrowErrorSet(&call->error, "External payload contains an exception");
                rc = EINVAL;
            } else
                rc = VgiRpcLogExtra(call, level_text, message, *extra ? extra : NULL);
            free(level_text);
            free(message);
            free(extra);
        } else if (value.array.release) {
            ArrowErrorSet(&call->error, "External payload contains multiple data batches");
            rc = EINVAL;
        } else {
            rc = ArrowSchemaDeepCopy(&incoming.schema, &value.schema);
            if (!rc) ArrowArrayMove(&incoming.array, &value.array);
            if (!rc) ArrowBufferMove(&inner_metadata, &value_metadata);
        }
        if (incoming.array.release) ArrowArrayRelease(&incoming.array);
    }
    if (!rc && (!value.array.release || input.position != payload.size_bytes)) {
        ArrowErrorSet(&call->error,
                      "External payload must contain exactly one complete data stream");
        rc = EINVAL;
    }
    if (!rc) rc = VgiMetadataSet(&value_metadata, VGI_RPC_LOCATION_SOURCE_KEY, url);
    if (!rc) {
        char duration[64];
        double milliseconds =
            (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_nsec - start.tv_nsec) / 1000000.0;
        snprintf(duration, sizeof(duration), "%.6f", milliseconds);
        rc = VgiMetadataSet(&value_metadata, VGI_RPC_LOCATION_FETCH_MS_KEY, duration);
    }
    if (!rc) {
        VgiBatchReset(batch);
        *batch = value;
        memset(&value, 0, sizeof(value));
        ArrowBufferReset(metadata);
        ArrowBufferMove(&value_metadata, metadata);
    } else {
        call->error_type = "ExternalLocationError";
        call->error_code = "INVALID_ARGUMENT";
        if (!call->error.message[0]) ArrowErrorSet(&call->error, "Invalid external payload");
    }
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
