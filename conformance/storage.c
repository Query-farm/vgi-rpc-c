#define _POSIX_C_SOURCE 200809L
#include "storage.h"

#include <curl/curl.h>
#include <errno.h>
#include <json-c/json.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#include "external_internal.h"

struct PublishedEntry {
    char *value;
    int64_t size;
    int include_sha256;
    struct VgiExternalRef ref;
    struct PublishedEntry *next;
};

static int validate(void *state, const char *url) {
    struct ConformanceStorage *storage = state;
    CURLU *parsed = curl_url();
    if (!parsed) return ENOMEM;
    char *scheme = NULL, *host = NULL;
    int rc = curl_url_set(parsed, CURLUPART_URL, url, 0);
    if (!rc) rc = curl_url_get(parsed, CURLUPART_SCHEME, &scheme, 0);
    if (!rc) rc = curl_url_get(parsed, CURLUPART_HOST, &host, 0);
    if (!rc && strcasecmp(scheme, "http") && strcasecmp(scheme, "https")) rc = EACCES;
    if (!rc && storage->reject_localhost && !strcasecmp(host, "localhost")) rc = EACCES;
    curl_free(scheme);
    curl_free(host);
    curl_url_cleanup(parsed);
    return rc;
}
/* Ask the fake storage for a URL pair sized and hashed for one upload; any
 * headers it signs come back in "headers" and are sent on the PUT. An object
 * allocation ("purpose": "object") is the object API's put_object. */
static int alloc_request(struct ConformanceStorage *storage, int64_t size, const char *sha256,
                         int object, struct VgiExternalUploadUrl *out, struct ArrowError *error) {
    unsigned char digest[32];
    char digest_b64[sodium_base64_ENCODED_LEN(32, sodium_base64_VARIANT_ORIGINAL)];
    if (sodium_hex2bin(digest, sizeof(digest), sha256, strlen(sha256), NULL, NULL, NULL))
        return EINVAL;
    sodium_bin2base64(digest_b64, sizeof(digest_b64), digest, sizeof(digest),
                      sodium_base64_VARIANT_ORIGINAL);
    char request[200];
    int request_length =
        snprintf(request, sizeof(request), "{\"size\": %lld, \"sha256_b64\": \"%s\"%s}",
                 (long long)size, digest_b64, object ? ", \"purpose\": \"object\"" : "");
    if (request_length < 0 || (size_t)request_length >= sizeof(request)) return EINVAL;
    size_t url_size = strlen(storage->base_url) + 7;
    char *url = malloc(url_size);
    if (!url) return ENOMEM;
    snprintf(url, url_size, "%s/alloc", storage->base_url);
    struct ArrowBuffer body;
    ArrowBufferInit(&body);
    int rc = VgiExternalHttp(&storage->options, "POST", url,
                             (struct ArrowBufferView){{request}, request_length}, NULL, NULL, 0,
                             &body, error);
    struct json_object *value = NULL;
    if (!rc) {
        rc = ArrowBufferAppend(&body, "", 1);
        if (!rc) value = json_tokener_parse((const char *)body.data);
        if (!value || !json_object_is_type(value, json_type_object)) rc = EINVAL;
    }
    if (!rc) {
        struct json_object *upload = NULL, *download = NULL;
        if (!json_object_object_get_ex(value, "upload_url", &upload) ||
            !json_object_object_get_ex(value, "download_url", &download) ||
            !json_object_is_type(upload, json_type_string) ||
            !json_object_is_type(download, json_type_string))
            rc = EINVAL;
        else {
            out->upload_url = strdup(json_object_get_string(upload));
            out->download_url = strdup(json_object_get_string(download));
            out->expires_at_us = ((int64_t)time(NULL) + 3600) * 1000000;
            if (!out->upload_url || !out->download_url) rc = ENOMEM;
        }
        struct json_object *object_id = NULL;
        if (!rc && json_object_object_get_ex(value, "object_id", &object_id) &&
            json_object_is_type(object_id, json_type_string)) {
            out->object_id = strdup(json_object_get_string(object_id));
            if (!out->object_id) rc = ENOMEM;
        }
        struct json_object *headers = NULL;
        if (!rc && json_object_object_get_ex(value, "upload_headers", &headers) && headers) {
            if (!json_object_is_type(headers, json_type_object))
                rc = EINVAL;
            else {
                json_object_object_foreach(headers, name, header) {
                    if (rc) break;
                    if (!json_object_is_type(header, json_type_string))
                        rc = EINVAL;
                    else
                        rc = VgiExternalUploadUrlAddHeader(out, name,
                                                           json_object_get_string(header));
                }
            }
        }
    }
    if (value) json_object_put(value);
    if (rc) VgiExternalUploadUrlReset(out);
    ArrowBufferReset(&body);
    free(url);
    return rc;
}
static int allocate(void *state, int64_t size, const char *sha256, struct VgiExternalUploadUrl *out,
                    struct ArrowError *error) {
    return alloc_request(state, size, sha256, 0, out, error);
}
static int put_object(void *state, struct ArrowBufferView data, char **object_id,
                      struct ArrowError *error) {
    struct ConformanceStorage *storage = state;
    if (!object_id || *object_id || data.size_bytes < 1) return EINVAL;
    char sha256[65];
    struct VgiExternalUploadUrl url = {0};
    struct ArrowBuffer response;
    ArrowBufferInit(&response);
    int rc = VgiExternalSha256Hex(data.data.data, (size_t)data.size_bytes, sha256);
    if (!rc) rc = alloc_request(storage, data.size_bytes, sha256, 1, &url, error);
    if (!rc && !url.object_id) rc = EINVAL;
    if (!rc)
        rc = VgiExternalHttp(&storage->options, "PUT", url.upload_url, data, NULL, url.headers,
                             url.header_count, &response, error);
    if (!rc) {
        *object_id = url.object_id;
        url.object_id = NULL;
    }
    VgiExternalUploadUrlReset(&url);
    ArrowBufferReset(&response);
    return rc;
}
/* GET /objects/{id}: the fake storage's object API; 404 means missing. */
static int get_object(void *state, const char *object_id, int64_t max_bytes,
                      struct ArrowBuffer *out, struct ArrowError *error) {
    struct ConformanceStorage *storage = state;
    if (!object_id || !*object_id || strpbrk(object_id, "/?#%") || max_bytes < 1) return EINVAL;
    size_t url_size = strlen(storage->base_url) + strlen(object_id) + 10;
    char *url = malloc(url_size);
    if (!url) return ENOMEM;
    snprintf(url, url_size, "%s/objects/%s", storage->base_url, object_id);
    struct VgiExternalOptions options = storage->options;
    options.max_fetch_bytes = max_bytes;
    options.max_decompressed_bytes = max_bytes;
    int rc = VgiExternalHttp(&options, "GET", url, (struct ArrowBufferView){{NULL}, 0}, NULL, NULL,
                             0, out, error);
    free(url);
    return rc;
}
void ConformanceStorageInit(struct ConformanceStorage *storage) {
    memset(storage, 0, sizeof(*storage));
    VgiExternalOptionsInit(&storage->options);
    storage->options.validate_url = validate;
    storage->options.validate_state = storage;
    storage->options.allocate = allocate;
    storage->options.put_object = put_object;
    storage->options.get_object = get_object;
    storage->options.storage_state = storage;
    pthread_mutex_init(&storage->mutex, NULL);
}
void ConformanceStorageReset(struct ConformanceStorage *storage) {
    struct PublishedEntry *entry = storage->published;
    while (entry) {
        struct PublishedEntry *next = entry->next;
        VgiExternalRefReset(&entry->ref);
        free(entry->value);
        free(entry);
        entry = next;
    }
    pthread_mutex_destroy(&storage->mutex);
}
int ConformancePublished(struct ConformanceStorage *storage, struct VgiRpcCall *call,
                         struct VgiRpcReply *reply) {
    if (!storage->base_url)
        return VgiRpcFail(call, "RuntimeError", "FAILED_PRECONDITION", "",
                          "published_string requires external storage");
    struct ArrowStringView value = VgiBatchString(&call->request, "value");
    int include_sha256 = (int)VgiBatchInt(&call->request, "include_sha256", 1);
    struct ArrowSchema schema = {0};
    int rc = call->method->result(&schema, &call->error);
    if (rc) return rc;
    pthread_mutex_lock(&storage->mutex);
    struct PublishedEntry *entry = storage->published;
    while (entry && !(entry->include_sha256 == include_sha256 && entry->size == value.size_bytes &&
                      !memcmp(entry->value, value.data, (size_t)value.size_bytes)))
        entry = entry->next;
    if (!entry) {
        entry = calloc(1, sizeof(*entry));
        if (!entry)
            rc = ENOMEM;
        else {
            entry->value = malloc((size_t)value.size_bytes + 1);
            if (!entry->value)
                rc = ENOMEM;
            else {
                memcpy(entry->value, value.data, (size_t)value.size_bytes);
                entry->value[value.size_bytes] = 0;
                entry->size = value.size_bytes;
                entry->include_sha256 = include_sha256;
            }
        }
        struct VgiBatch batch = {0};
        struct ArrowBuffer bytes;
        ArrowBufferInit(&bytes);
        if (!rc) rc = VgiBatchInit(&batch, &schema, &call->error);
        if (!rc) rc = ArrowArrayAppendString(batch.array.children[0], value);
        if (!rc) rc = VgiBatchFinishRow(&batch, &call->error);
        if (!rc) rc = VgiIpcWriteBatch(&batch.schema, &batch.array, NULL, &bytes, &call->error);
        if (!rc)
            rc = VgiExternalPublish(&storage->options, &schema,
                                    (struct ArrowBufferView){{bytes.data}, bytes.size_bytes},
                                    include_sha256, &entry->ref, &call->error);
        ArrowBufferReset(&bytes);
        VgiBatchReset(&batch);
        if (!rc) {
            entry->next = storage->published;
            storage->published = entry;
        } else if (entry) {
            VgiExternalRefReset(&entry->ref);
            free(entry->value);
            free(entry);
            entry = NULL;
        }
    }
    if (!rc)
        rc =
            VgiExternalPointer(&schema, &entry->ref, &reply->value, &reply->metadata, &call->error);
    pthread_mutex_unlock(&storage->mutex);
    ArrowSchemaRelease(&schema);
    return rc;
}
