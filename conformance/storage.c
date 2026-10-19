#define _POSIX_C_SOURCE 200809L
#include "storage.h"

#include <curl/curl.h>
#include <errno.h>
#include <json-c/json.h>
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
static int allocate(void *state, const struct ArrowSchema *schema, struct VgiExternalUploadUrl *out,
                    struct ArrowError *error) {
    (void)schema;
    struct ConformanceStorage *storage = state;
    size_t size = strlen(storage->base_url) + 7;
    char *url = malloc(size);
    if (!url) return ENOMEM;
    snprintf(url, size, "%s/alloc", storage->base_url);
    struct ArrowBuffer body;
    ArrowBufferInit(&body);
    int rc = VgiExternalHttp(&storage->options, "POST", url, (struct ArrowBufferView){{"{}"}, 2},
                             NULL, &body, error);
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
    }
    if (value) json_object_put(value);
    if (rc) VgiExternalUploadUrlReset(out);
    ArrowBufferReset(&body);
    free(url);
    return rc;
}
void ConformanceStorageInit(struct ConformanceStorage *storage) {
    memset(storage, 0, sizeof(*storage));
    VgiExternalOptionsInit(&storage->options);
    storage->options.validate_url = validate;
    storage->options.validate_state = storage;
    storage->options.allocate = allocate;
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
