/* Blob-vendor client (see include/vgi_rpc/vendor.h). Ported from vgi-rpc-go's
 * vgirpc/vendor.go. */
#define _POSIX_C_SOURCE 200809L
#include "vgi_rpc/vendor.h"

#include <ctype.h>
#include <curl/curl.h>
#include <errno.h>
#include <json-c/json.h>
#include <pthread.h>
#include <sodium.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#include "external_internal.h"
#include "vgi_rpc/http.h"
#include "vgi_rpc/rpc.h"

#define VENDOR_DEFAULT_THRESHOLD (1024 * 1024)
#define VENDOR_DEFAULT_MAX_FETCH ((int64_t)256 * 1024 * 1024)
#define VENDOR_API_MAX_BODY ((int64_t)16 * 1024 * 1024)
#define VENDOR_MAX_HOSTS 64
#define VENDOR_MAX_PREFIXES 64

struct PinnedHost {
    char *host;
    int port;
};
struct VgiVendor {
    char *base;
    char *token;
    int loopback;
    struct VgiVendorOptions options;
    /* Signed-URL requests: pinned, never carrying the token. */
    struct VgiExternalOptions storage;
    pthread_mutex_t lock;
    struct PinnedHost hosts[VENDOR_MAX_HOSTS];
    size_t host_count;
    char *prefixes[VENDOR_MAX_PREFIXES];
    size_t prefix_count;
    time_t fetched_at;
    int pinned;
};

void VgiVendorOptionsInit(struct VgiVendorOptions *o) {
    memset(o, 0, sizeof(*o));
    o->timeout_seconds = 30;
    o->attempts = 3;
    o->info_ttl_seconds = 600;
}

/* ------------------------------------------------------------------------- */
/* URL helpers                                                               */
/* ------------------------------------------------------------------------- */

static int is_loopback(const char *host) {
    return !strcasecmp(host, "localhost") || !strncmp(host, "127.", 4) || !strcmp(host, "::1") ||
           !strcmp(host, "[::1]");
}
static int default_port(const char *scheme) { return strcasecmp(scheme, "https") ? 80 : 443; }
static void lower(char *s) {
    for (; s && *s; ++s) *s = (char)tolower((unsigned char)*s);
}
/* Parse scheme, host (lowercase, no brackets), effective port and userinfo. */
static int parse_url(const char *url, char **scheme, char **host, int *port, int *has_user) {
    *scheme = *host = NULL;
    *port = 0;
    *has_user = 0;
    CURLU *u = curl_url();
    if (!u) return ENOMEM;
    char *port_text = NULL, *user = NULL;
    int rc = curl_url_set(u, CURLUPART_URL, url, 0) ? EINVAL : 0;
    if (!rc && curl_url_get(u, CURLUPART_SCHEME, scheme, 0)) rc = EINVAL;
    if (!rc && curl_url_get(u, CURLUPART_HOST, host, 0)) rc = EINVAL;
    if (!rc && !curl_url_get(u, CURLUPART_USER, &user, 0) && user) *has_user = 1;
    if (!rc && !curl_url_get(u, CURLUPART_PORT, &port_text, 0) && port_text) {
        char *end = NULL;
        long value = strtol(port_text, &end, 10);
        if (*end || value < 1 || value > 65535)
            rc = EINVAL;
        else
            *port = (int)value;
    }
    curl_free(port_text);
    curl_free(user);
    curl_url_cleanup(u);
    if (!rc) {
        lower(*scheme);
        lower(*host);
        size_t n = strlen(*host);
        if (n >= 2 && (*host)[0] == '[' && (*host)[n - 1] == ']') {
            memmove(*host, *host + 1, n - 2);
            (*host)[n - 2] = 0;
        }
        if (!**host) rc = EINVAL;
        if (!*port && !rc) *port = default_port(*scheme);
    }
    if (rc) {
        curl_free(*scheme);
        curl_free(*host);
        *scheme = *host = NULL;
    }
    return rc;
}

/* ------------------------------------------------------------------------- */
/* Vendor API (JSON over HTTPS, bearer token, retries)                       */
/* ------------------------------------------------------------------------- */

struct Sink {
    struct ArrowBuffer *out;
    int64_t limit;
    int error;
};
static size_t sink(char *data, size_t size, size_t count, void *state) {
    struct Sink *s = state;
    size_t n = size * count;
    if ((int64_t)n > s->limit - s->out->size_bytes) {
        s->error = EFBIG;
        return 0;
    }
    if (ArrowBufferAppend(s->out, data, (int64_t)n)) {
        s->error = ENOMEM;
        return 0;
    }
    return n;
}
static void backoff(unsigned attempt) {
    double delay = 0.25 * (double)(1u << (attempt - 1));
    if (delay > 5) delay = 5;
    delay *= 0.5 + (double)randombytes_uniform(1000) / 2000.0;
    struct timespec ts = {(time_t)delay, (long)((delay - (double)(time_t)delay) * 1e9)};
    nanosleep(&ts, NULL);
}
/* Short vendor error code ("not_found") and message from a JSON error body. */
static void error_detail(const struct ArrowBuffer *body, char *code, size_t code_size,
                         char *message, size_t message_size) {
    *code = *message = 0;
    if (!body->size_bytes || body->size_bytes > 65536) return;
    struct json_tokener *parser = json_tokener_new();
    if (!parser) return;
    struct json_object *object =
        json_tokener_parse_ex(parser, (const char *)body->data, (int)body->size_bytes);
    json_tokener_free(parser);
    struct json_object *field = NULL;
    if (object && json_object_is_type(object, json_type_object)) {
        if (json_object_object_get_ex(object, "error", &field) &&
            json_object_is_type(field, json_type_string))
            snprintf(code, code_size, "%s", json_object_get_string(field));
        if (json_object_object_get_ex(object, "message", &field) &&
            json_object_is_type(field, json_type_string))
            snprintf(message, message_size, "%.200s", json_object_get_string(field));
    }
    if (object) json_object_put(object);
}
/* One vendor API call: `path` relative to the base URL, a JSON body (or NULL
 * for GET). On success *result is the parsed JSON object. On an HTTP error,
 * *status and code carry the vendor's answer. */
static int api_call(struct VgiVendor *v, const char *method, const char *path,
                    struct json_object *payload, const char *op, struct json_object **result,
                    long *status, char code[64], struct ArrowError *error) {
    *result = NULL;
    *status = 0;
    code[0] = 0;
    size_t url_size = strlen(v->base) + strlen(path) + 1;
    char *url = malloc(url_size);
    if (!url) return ENOMEM;
    snprintf(url, url_size, "%s%s", v->base, path);
    const char *body =
        payload ? json_object_to_json_string_ext(payload, JSON_C_TO_STRING_PLAIN) : NULL;
    size_t auth_size = strlen(v->token) + 32;
    char *auth = malloc(auth_size);
    if (!auth) {
        free(url);
        return ENOMEM;
    }
    snprintf(auth, auth_size, "Authorization: Bearer %s", v->token);
    struct ArrowBuffer response;
    ArrowBufferInit(&response);
    int rc = EIO;
    char last[64] = "transport error";
    for (unsigned attempt = 0; attempt < v->options.attempts; ++attempt) {
        if (attempt) backoff(attempt);
        ArrowBufferReset(&response);
        ArrowBufferInit(&response);
        CURL *curl = curl_easy_init();
        if (!curl) {
            rc = ENOMEM;
            break;
        }
        struct Sink s = {&response, VENDOR_API_MAX_BODY, 0};
        struct curl_slist *headers = curl_slist_append(NULL, auth);
        headers = curl_slist_append(headers, "Accept: application/json");
        if (body) {
            headers = curl_slist_append(headers, "Content-Type: application/json");
            headers = curl_slist_append(headers, "Expect:");
        }
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, "http,https");
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)v->options.timeout_seconds);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, sink);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s);
        if (body) {
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)strlen(body));
        }
        CURLcode result = curl_easy_perform(curl);
        long http_status = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_status);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        if (s.error) {
            rc = s.error;
            snprintf(last, sizeof(last), "response too large");
            break;
        }
        if (result != CURLE_OK) {
            rc = EIO;
            snprintf(last, sizeof(last), "%s",
                     result == CURLE_OPERATION_TIMEDOUT ? "timeout" : "transport error");
            continue;
        }
        *status = http_status;
        if (http_status == 429 || http_status >= 500) {
            rc = EIO;
            snprintf(last, sizeof(last), "HTTP %ld", http_status);
            continue;
        }
        rc = 0;
        break;
    }
    free(auth);
    if (rc) {
        ArrowErrorSet(error, "blob vendor %s failed after %u attempts (%s): %s", op,
                      v->options.attempts, last, url);
        free(url);
        ArrowBufferReset(&response);
        return rc;
    }
    if (*status < 200 || *status >= 300) {
        char message[256];
        error_detail(&response, code, 64, message, sizeof(message));
        ArrowErrorSet(error, "blob vendor %s failed: HTTP %ld%s%s%s%s", op, *status,
                      *code ? ": " : "", code, *message ? ": " : "", message);
        free(url);
        ArrowBufferReset(&response);
        return *status == 404 ? ENOENT : EIO;
    }
    free(url);
    struct json_object *object = NULL;
    if (response.size_bytes && response.size_bytes <= INT32_MAX) {
        struct json_tokener *parser = json_tokener_new();
        if (parser) {
            object = json_tokener_parse_ex(parser, (const char *)response.data,
                                           (int)response.size_bytes);
            json_tokener_free(parser);
        }
    }
    ArrowBufferReset(&response);
    if (!object || !json_object_is_type(object, json_type_object)) {
        if (object) json_object_put(object);
        ArrowErrorSet(error, "blob vendor %s returned a non-object JSON body", op);
        return EIO;
    }
    *result = object;
    return 0;
}
static const char *json_text(struct json_object *object, const char *key) {
    struct json_object *field = NULL;
    if (!json_object_object_get_ex(object, key, &field) ||
        !json_object_is_type(field, json_type_string))
        return NULL;
    return json_object_get_string(field);
}
/* days_from_civil (Howard Hinnant): proleptic Gregorian date to Unix days. */
static int64_t days_from_civil(int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468;
}
/* ISO-8601 UTC ("2026-11-22T10:00:00Z", fractional seconds and +00:00 allowed)
 * or epoch seconds (milliseconds above 1e11) to microseconds. */
static int vendor_time(struct json_object *object, const char *key, int64_t *out_us) {
    struct json_object *field = NULL;
    if (!json_object_object_get_ex(object, key, &field) || !field) return ENOENT;
    if (json_object_is_type(field, json_type_int) || json_object_is_type(field, json_type_double)) {
        double seconds = json_object_get_double(field);
        if (seconds > 1e11) seconds /= 1000;
        *out_us = (int64_t)(seconds * 1e6);
        return 0;
    }
    if (!json_object_is_type(field, json_type_string)) return EINVAL;
    int year, month, day, hour, minute, second;
    if (sscanf(json_object_get_string(field), "%4d-%2d-%2dT%2d:%2d:%2d", &year, &month, &day, &hour,
               &minute, &second) != 6 ||
        month < 1 || month > 12 || day < 1 || day > 31)
        return EINVAL;
    int64_t days = days_from_civil(year, (unsigned)month, (unsigned)day);
    *out_us = ((days * 86400) + hour * 3600 + minute * 60 + second) * 1000000;
    return 0;
}

/* ------------------------------------------------------------------------- */
/* Host pinning (/v1/info)                                                   */
/* ------------------------------------------------------------------------- */

static void clear_pins(struct VgiVendor *v) {
    for (size_t i = 0; i < v->host_count; ++i) free(v->hosts[i].host);
    for (size_t i = 0; i < v->prefix_count; ++i) free(v->prefixes[i]);
    v->host_count = v->prefix_count = 0;
}
static int add_host(struct VgiVendor *v, const char *entry) {
    if (v->host_count == VENDOR_MAX_HOSTS) return E2BIG;
    char *scheme = NULL, *host = NULL;
    int port = 0, user = 0;
    size_t size = strlen(entry) + 16;
    char *url = malloc(size);
    if (!url) return ENOMEM;
    snprintf(url, size, "https://%s", entry);
    int explicit_port = 0;
    {
        const char *colon = strrchr(entry, ':'), *bracket = strrchr(entry, ']');
        explicit_port = colon && (!bracket || colon > bracket);
    }
    int rc = strpbrk(entry, "/?#@ ") ? EINVAL : parse_url(url, &scheme, &host, &port, &user);
    free(url);
    if (!rc) {
        v->hosts[v->host_count].host = strdup(host);
        /* Without a port the entry matches either scheme's default port. */
        v->hosts[v->host_count].port = explicit_port ? port : 0;
        if (!v->hosts[v->host_count].host)
            rc = ENOMEM;
        else
            ++v->host_count;
    }
    curl_free(scheme);
    curl_free(host);
    return rc;
}
static int add_hosts(struct VgiVendor *v, struct json_object *info, const char *key) {
    struct json_object *list = NULL;
    if (!json_object_object_get_ex(info, key, &list) || !list) return 0;
    if (!json_object_is_type(list, json_type_array)) return EINVAL;
    for (size_t i = 0; i < json_object_array_length(list); ++i) {
        struct json_object *item = json_object_array_get_idx(list, i);
        if (!json_object_is_type(item, json_type_string) || !json_object_get_string_len(item))
            return EINVAL;
        int rc = add_host(v, json_object_get_string(item));
        if (rc) return rc;
    }
    return 0;
}
/* Refresh the pin when it is older than info_ttl_seconds. Keeps the last good
 * pin through a vendor blip. Caller holds the lock. */
static int refresh_pins(struct VgiVendor *v, struct ArrowError *error) {
    time_t now = time(NULL);
    if (v->pinned && now - v->fetched_at < (time_t)v->options.info_ttl_seconds) return 0;
    struct json_object *info = NULL;
    long status = 0;
    char code[64];
    struct ArrowError local = {{0}};
    int rc = api_call(v, "GET", "/v1/info", NULL, "info", &info, &status, code, &local);
    if (!rc) {
        struct VgiVendor scratch;
        memset(&scratch, 0, sizeof(scratch));
        rc = add_hosts(&scratch, info, "upload_hosts");
        if (!rc) rc = add_hosts(&scratch, info, "download_hosts");
        struct json_object *list = NULL;
        if (!rc && json_object_object_get_ex(info, "url_prefixes", &list) && list) {
            if (!json_object_is_type(list, json_type_array)) rc = EINVAL;
            for (size_t i = 0; !rc && i < json_object_array_length(list); ++i) {
                struct json_object *item = json_object_array_get_idx(list, i);
                if (!json_object_is_type(item, json_type_string)) {
                    rc = EINVAL;
                    break;
                }
                if (!json_object_get_string_len(item)) continue;
                if (scratch.prefix_count == VENDOR_MAX_PREFIXES) {
                    rc = E2BIG;
                    break;
                }
                if (!(scratch.prefixes[scratch.prefix_count++] =
                          strdup(json_object_get_string(item))))
                    rc = ENOMEM;
            }
        }
        if (!rc && !scratch.host_count) rc = EINVAL;
        if (!rc) {
            clear_pins(v);
            memcpy(v->hosts, scratch.hosts, sizeof(v->hosts));
            memcpy(v->prefixes, scratch.prefixes, sizeof(v->prefixes));
            v->host_count = scratch.host_count;
            v->prefix_count = scratch.prefix_count;
            v->fetched_at = now;
            v->pinned = 1;
        } else {
            clear_pins(&scratch);
            snprintf(local.message, sizeof(local.message), "blob vendor info has malformed hosts");
        }
        json_object_put(info);
    }
    if (rc && v->pinned) {
        fprintf(stderr, "vgi-rpc-c: blob vendor info refresh failed; keeping cached hosts\n");
        v->fetched_at = now;
        return 0;
    }
    if (rc) ArrowErrorSet(error, "cannot pin storage hosts: %s", local.message);
    return rc;
}
int VgiVendorValidateUrl(struct VgiVendor *v, const char *url, struct ArrowError *error) {
    if (!v || !url) return EINVAL;
    char *scheme = NULL, *host = NULL;
    int port = 0, user = 0;
    if (parse_url(url, &scheme, &host, &port, &user)) {
        ArrowErrorSet(error, "URL is malformed");
        return EINVAL;
    }
    int rc = 0;
    if (strcmp(scheme, "https") && !(v->loopback && !strcmp(scheme, "http"))) {
        ArrowErrorSet(error, "URL scheme %s not allowed", scheme);
        rc = EACCES;
    } else if (user) {
        ArrowErrorSet(error, "URL userinfo is not allowed");
        rc = EACCES;
    }
    pthread_mutex_lock(&v->lock);
    if (!rc) rc = refresh_pins(v, error);
    if (!rc) {
        int allowed = 0;
        for (size_t i = 0; !allowed && i < v->host_count; ++i) {
            int expected = v->hosts[i].port ? v->hosts[i].port : default_port(scheme);
            allowed = !strcmp(v->hosts[i].host, host) && expected == port;
        }
        if (!allowed) {
            ArrowErrorSet(error, "URL host is not a pinned storage host: %s://%s:%d", scheme, host,
                          port);
            rc = EACCES;
        }
    }
    if (!rc && v->prefix_count) {
        size_t bare = strcspn(url, "?#");
        int matched = 0, dotdot = 0;
        for (size_t i = 0; !matched && i < v->prefix_count; ++i) {
            size_t n = strlen(v->prefixes[i]);
            matched = n <= bare && !strncmp(url, v->prefixes[i], n);
        }
        const char *path = strstr(url, "://");
        path = path ? strchr(path + 3, '/') : NULL;
        for (const char *p = path; p && p < url + bare; p = strchr(p + 1, '/'))
            if (!strncmp(p, "/..", 3) && (p + 3 >= url + bare || p[3] == '/' || p[3] == '?'))
                dotdot = 1;
        if (!matched || dotdot) {
            ArrowErrorSet(error, "URL is outside the pinned storage prefixes: %s://%s:%d", scheme,
                          host, port);
            rc = EACCES;
        }
    }
    pthread_mutex_unlock(&v->lock);
    curl_free(scheme);
    curl_free(host);
    return rc;
}
static int validate_callback(void *state, const char *url) {
    struct ArrowError error = {{0}};
    int rc = VgiVendorValidateUrl(state, url, &error);
    if (rc) fprintf(stderr, "vgi-rpc-c: blob vendor URL refused: %s\n", error.message);
    return rc;
}

/* ------------------------------------------------------------------------- */
/* Grants, objects                                                           */
/* ------------------------------------------------------------------------- */

static int hex_to_b64(const char *sha256_hex, char out[48]) {
    unsigned char digest[32];
    size_t n = 0;
    if (!sha256_hex || strlen(sha256_hex) != 64 ||
        sodium_hex2bin(digest, sizeof(digest), sha256_hex, 64, NULL, &n, NULL) || n != 32)
        return EINVAL;
    sodium_bin2base64(out, 48, digest, sizeof(digest), sodium_base64_VARIANT_ORIGINAL);
    return 0;
}
/* POST /v1/blobs/put (purpose rpc) for one object of `size` bytes, with an
 * optional advisory origin. Fills upload/download URLs, required headers
 * (minus Content-Length, which must equal size), object id and expiry. */
static int put_grant(struct VgiVendor *v, int64_t size, const char *sha256_hex, const char *origin,
                     struct VgiExternalUploadUrl *out, struct ArrowError *error) {
    char digest[48];
    if (size < 1 || hex_to_b64(sha256_hex, digest)) {
        ArrowErrorSet(error, "blob vendor put needs a positive size and a 64-hex SHA-256");
        return EINVAL;
    }
    struct json_object *payload = json_object_new_object(), *response = NULL;
    if (!payload) return ENOMEM;
    json_object_object_add(payload, "size", json_object_new_int64(size));
    json_object_object_add(payload, "sha256_b64", json_object_new_string(digest));
    json_object_object_add(payload, "purpose", json_object_new_string("rpc"));
    if (origin) json_object_object_add(payload, "origin", json_object_new_string(origin));
    long status = 0;
    char code[64];
    int rc = api_call(v, "POST", "/v1/blobs/put", payload, "put", &response, &status, code, error);
    json_object_put(payload);
    if (rc) return rc == ENOENT ? EIO : rc;
    const char *method = json_text(response, "upload_method");
    const char *object_id = json_text(response, "object_id"),
               *upload = json_text(response, "upload_url"),
               *download = json_text(response, "download_url");
    struct json_object *headers = NULL;
    if (method && strcmp(method, "PUT")) {
        ArrowErrorSet(error, "blob vendor put asked for unsupported upload_method %s", method);
        rc = EIO;
    } else if (!object_id || !*object_id || !upload || !*upload) {
        ArrowErrorSet(error, "blob vendor put response is missing object_id or upload_url");
        rc = EIO;
    } else if (!download || !*download) {
        ArrowErrorSet(error, "blob vendor put response for an rpc object has no download_url");
        rc = EIO;
    } else if (!json_object_object_get_ex(response, "required_headers", &headers) ||
               !json_object_is_type(headers, json_type_object)) {
        ArrowErrorSet(error, "blob vendor put response has malformed required_headers");
        rc = EIO;
    }
    if (!rc) {
        out->object_id = strdup(object_id);
        out->upload_url = strdup(upload);
        out->download_url = strdup(download);
        if (!out->object_id || !out->upload_url || !out->download_url) rc = ENOMEM;
    }
    if (!rc) {
        json_object_object_foreach(headers, name, value) {
            if (rc) break;
            if (!json_object_is_type(value, json_type_string)) {
                ArrowErrorSet(error, "blob vendor put response has malformed required_headers");
                rc = EIO;
                break;
            }
            if (!strcasecmp(name, "Content-Length")) {
                char expected[32];
                snprintf(expected, sizeof(expected), "%lld", (long long)size);
                if (strcmp(json_object_get_string(value), expected)) {
                    ArrowErrorSet(error,
                                  "blob vendor granted Content-Length %s for a %lld-byte object",
                                  json_object_get_string(value), (long long)size);
                    rc = EIO;
                }
                continue;
            }
            rc = VgiExternalUploadUrlAddHeader(out, name, json_object_get_string(value));
        }
    }
    if (!rc) {
        int64_t expires = 0, download_expires = 0;
        if (vendor_time(response, "expires_at", &expires))
            expires = ((int64_t)time(NULL) + 3600) * 1000000;
        if (!vendor_time(response, "download_expires_at", &download_expires) &&
            download_expires < expires)
            expires = download_expires;
        out->expires_at_us = expires;
    }
    json_object_put(response);
    if (!rc) rc = VgiVendorValidateUrl(v, out->upload_url, error);
    if (!rc) rc = VgiVendorValidateUrl(v, out->download_url, error);
    if (rc) VgiExternalUploadUrlReset(out);
    return rc;
}
static int allocate_result(void *state, int64_t size, const char *sha256_hex,
                           struct VgiExternalUploadUrl *out, struct ArrowError *error) {
    return put_grant(state, size, sha256_hex, NULL, out, error);
}
static int allocate_client(void *state, int64_t size, const char *sha256_hex,
                           struct VgiExternalUploadUrl *out, struct ArrowError *error) {
    return put_grant(state, size, sha256_hex, "client_upload", out, error);
}
static int put_object(void *state, struct ArrowBufferView data, char **object_id,
                      struct ArrowError *error) {
    struct VgiVendor *v = state;
    char sha256[65];
    struct VgiExternalUploadUrl grant = {0};
    int rc = VgiExternalSha256Hex(data.data.data, (size_t)data.size_bytes, sha256);
    if (!rc) rc = put_grant(v, data.size_bytes, sha256, "state_ref", &grant, error);
    struct ArrowBuffer response;
    ArrowBufferInit(&response);
    if (!rc)
        rc = VgiExternalHttp(&v->storage, "PUT", grant.upload_url, data, NULL, grant.headers,
                             grant.header_count, &response, error);
    ArrowBufferReset(&response);
    if (!rc) {
        *object_id = grant.object_id;
        grant.object_id = NULL;
    }
    VgiExternalUploadUrlReset(&grant);
    return rc;
}
static int get_object(void *state, const char *object_id, int64_t max_bytes,
                      struct ArrowBuffer *out, struct ArrowError *error) {
    struct VgiVendor *v = state;
    if (max_bytes <= 0) max_bytes = VENDOR_DEFAULT_MAX_FETCH;
    struct json_object *payload = json_object_new_object(), *response = NULL;
    if (!payload) return ENOMEM;
    json_object_object_add(payload, "object_id", json_object_new_string(object_id));
    long status = 0;
    char code[64];
    int rc = api_call(v, "POST", "/v1/blobs/get", payload, "get", &response, &status, code, error);
    json_object_put(payload);
    if (rc) {
        if (rc == ENOENT || !strcmp(code, "not_found") || !strcmp(code, "not_uploaded")) {
            ArrowErrorSet(error, "blob vendor object is gone (%s)", *code ? code : "404");
            return ENOENT;
        }
        return rc;
    }
    const char *url = json_text(response, "download_url"),
               *encoding = json_text(response, "content_encoding");
    struct json_object *size_field = NULL;
    int64_t size = -1;
    if (json_object_object_get_ex(response, "size", &size_field) &&
        json_object_is_type(size_field, json_type_int))
        size = json_object_get_int64(size_field);
    char *download = url ? strdup(url) : NULL;
    if (!url || !*url || size < 0) {
        ArrowErrorSet(error, "blob vendor get response is missing download_url or size");
        rc = EIO;
    } else if (!download)
        rc = ENOMEM;
    else if (size > max_bytes) {
        ArrowErrorSet(error, "blob vendor object is %lld bytes, over max_fetch_bytes (%lld)",
                      (long long)size, (long long)max_bytes);
        rc = EFBIG;
    } else if (encoding && *encoding && strcmp(encoding, "identity")) {
        ArrowErrorSet(error, "blob vendor object has unsupported content_encoding %s", encoding);
        rc = EINVAL;
    }
    json_object_put(response);
    if (!rc) {
        struct VgiExternalOptions options = v->storage;
        options.max_fetch_bytes = max_bytes;
        options.max_decompressed_bytes = max_bytes;
        rc = VgiExternalHttp(&options, "GET", download, (struct ArrowBufferView){{NULL}, 0}, NULL,
                             NULL, 0, out, error);
        if (rc == ENOENT) ArrowErrorSet(error, "blob vendor object is gone (storage 404)");
    }
    free(download);
    return rc;
}

/* ------------------------------------------------------------------------- */
/* Lifecycle and environment                                                 */
/* ------------------------------------------------------------------------- */

int VgiVendorNew(const char *url, const char *token, const struct VgiVendorOptions *options,
                 struct VgiVendor **out, struct ArrowError *error) {
    if (!url || !out) return EINVAL;
    *out = NULL;
    char *scheme = NULL, *host = NULL;
    int port = 0, user = 0;
    if (parse_url(url, &scheme, &host, &port, &user) ||
        (strcmp(scheme, "https") && strcmp(scheme, "http")) || user) {
        curl_free(scheme);
        curl_free(host);
        ArrowErrorSet(error, "blob vendor URL must be an absolute https URL");
        return EINVAL;
    }
    int loopback = !strcmp(scheme, "http");
    int refused = loopback && !is_loopback(host);
    curl_free(scheme);
    curl_free(host);
    if (refused) {
        /* The bearer token would cross the network in plain text. */
        ArrowErrorSet(error, "blob vendor URL must use https (http is allowed only for loopback)");
        return EINVAL;
    }
    if (!token || !*token) {
        ArrowErrorSet(error, "blob vendor token is empty");
        return EINVAL;
    }
    struct VgiVendor *v = calloc(1, sizeof(*v));
    if (!v) return ENOMEM;
    if (options)
        v->options = *options;
    else
        VgiVendorOptionsInit(&v->options);
    if (!v->options.timeout_seconds) v->options.timeout_seconds = 30;
    if (!v->options.attempts) v->options.attempts = 3;
    if (!v->options.info_ttl_seconds) v->options.info_ttl_seconds = 600;
    v->base = strdup(url);
    v->token = strdup(token);
    v->loopback = loopback;
    if (!v->base || !v->token || pthread_mutex_init(&v->lock, NULL)) {
        free(v->base);
        free(v->token);
        free(v);
        return ENOMEM;
    }
    size_t n = strlen(v->base);
    while (n && v->base[n - 1] == '/') v->base[--n] = 0;
    VgiExternalOptionsInit(&v->storage);
    v->storage.timeout_seconds = v->options.timeout_seconds * 4;
    v->storage.max_redirects = 0;
    v->storage.validate_url = validate_callback;
    v->storage.validate_state = v;
    *out = v;
    return 0;
}
void VgiVendorFree(struct VgiVendor *v) {
    if (!v) return;
    clear_pins(v);
    pthread_mutex_destroy(&v->lock);
    sodium_memzero(v->token, strlen(v->token));
    free(v->token);
    free(v->base);
    free(v);
}
void VgiVendorBindExternal(struct VgiVendor *v, struct VgiExternalOptions *external) {
    external->allocate = allocate_result;
    external->allocate_client_upload = allocate_client;
    external->put_object = put_object;
    external->get_object = get_object;
    external->storage_state = v;
    external->validate_url = validate_callback;
    external->validate_state = v;
}
static int positive_env(const char *name, int64_t *out, struct ArrowError *error) {
    *out = 0;
    const char *raw = getenv(name);
    if (!raw) return 0;
    while (isspace((unsigned char)*raw)) ++raw;
    if (!*raw) return 0;
    char *end = NULL;
    errno = 0;
    long long value = strtoll(raw, &end, 10);
    while (end && isspace((unsigned char)*end)) ++end;
    if (errno || !end || *end || value <= 0) {
        ArrowErrorSet(error, "%s must be a positive integer, got '%s'", name, raw);
        return EINVAL;
    }
    *out = value;
    return 0;
}
int VgiExternalOptionsFromEnv(struct VgiExternalOptions *external, struct VgiVendor **vendor,
                              struct ArrowError *error) {
    if (!external || !vendor) return EINVAL;
    *vendor = NULL;
    const char *url = getenv(VGI_BLOB_VENDOR_URL_ENV);
    if (!url || !*url) return 0;
    const char *token = getenv(VGI_BLOB_VENDOR_TOKEN_ENV);
    if (!token || !*token) token = getenv(VGI_BLOB_VENDOR_TOKEN_FALLBACK_ENV);
    if (!token || !*token) {
        ArrowErrorSet(error, "%s is set but neither %s nor %s is", VGI_BLOB_VENDOR_URL_ENV,
                      VGI_BLOB_VENDOR_TOKEN_ENV, VGI_BLOB_VENDOR_TOKEN_FALLBACK_ENV);
        return EINVAL;
    }
    int64_t threshold = 0, fetch = 0, upload = 0;
    int rc = positive_env(VGI_RPC_EXTERNALIZE_THRESHOLD_ENV, &threshold, error);
    if (!rc) rc = positive_env(VGI_RPC_MAX_FETCH_BYTES_ENV, &fetch, error);
    if (!rc) rc = positive_env(VGI_RPC_MAX_UPLOAD_BYTES_ENV, &upload, error);
    if (rc) return rc;
    /* Fix 1: the advertised upload cap never exceeds what the server fetches. */
    if (fetch && upload > fetch) {
        ArrowErrorSet(error,
                      "%s (%lld) exceeds %s (%lld): clients would upload requests the server then "
                      "refuses to fetch",
                      VGI_RPC_MAX_UPLOAD_BYTES_ENV, (long long)upload, VGI_RPC_MAX_FETCH_BYTES_ENV,
                      (long long)fetch);
        return EINVAL;
    }
    if (!fetch) fetch = upload > VENDOR_DEFAULT_MAX_FETCH ? upload : VENDOR_DEFAULT_MAX_FETCH;
    const char *compression = getenv(VGI_RPC_EXTERNAL_COMPRESSION_ENV);
    const char *codec = NULL;
    if (compression && *compression && strcasecmp(compression, "none") &&
        strcasecmp(compression, "off") && strcasecmp(compression, "identity")) {
        if (strcasecmp(compression, "zstd") && strncasecmp(compression, "zstd:", 5)) {
            ArrowErrorSet(error, "%s must be none or zstd, got '%s'",
                          VGI_RPC_EXTERNAL_COMPRESSION_ENV, compression);
            return EINVAL;
        }
        codec = "zstd";
    }
    struct VgiVendor *v = NULL;
    rc = VgiVendorNew(url, token, NULL, &v, error);
    if (rc) return rc;
    VgiExternalOptionsInit(external);
    external->threshold_bytes = threshold ? threshold : VENDOR_DEFAULT_THRESHOLD;
    external->max_fetch_bytes = fetch;
    external->max_decompressed_bytes = fetch;
    external->max_upload_bytes = upload;
    external->compression = codec;
    VgiVendorBindExternal(v, external);
    *vendor = v;
    return 0;
}
int VgiHttpConfigureFromEnv(struct VgiRpcServer *server, struct VgiHttpOptions *options,
                            struct VgiVendor **vendor, struct ArrowError *error) {
    if (!server || !options || !vendor) return EINVAL;
    *vendor = NULL;
    int64_t max_request = 0;
    int rc = positive_env(VGI_RPC_MAX_REQUEST_BYTES_ENV, &max_request, error);
    if (rc) return rc;
    struct VgiExternalOptions external;
    rc = VgiExternalOptionsFromEnv(&external, vendor, error);
    if (rc) return rc;
    if (max_request) options->max_request_bytes = max_request;
    if (*vendor) {
        rc = VgiRpcServerSetExternal(server, &external);
        if (rc) {
            ArrowErrorSet(error, "cannot configure blob-vendor externalization");
            VgiVendorFree(*vendor);
            *vendor = NULL;
        }
    }
    return rc;
}
