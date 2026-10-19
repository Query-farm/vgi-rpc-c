#define _POSIX_C_SOURCE 200809L
#include <curl/curl.h>
#include <errno.h>
#include <inttypes.h>
#include <json-c/json.h>
#include <pthread.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "telemetry_internal.h"

static _Thread_local struct VgiTraceContext current;
struct Record {
    struct json_object *span, *metrics;
    struct Record *next;
};
struct VgiTelemetry {
    struct VgiTelemetryOptions options;
    char *endpoint, *service, *ca;
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    pthread_t thread;
    struct Record *head, *tail;
    struct VgiTelemetryStats stats;
    int stopping, inflight;
    uint64_t metric_start;
};
struct VgiTraceSpan {
    struct VgiTelemetry *telemetry;
    struct VgiTraceContext context;
    char parent[17], error[128];
    char *protocol, *method, *type, *transport;
    uint64_t start;
    struct timespec monotonic;
    int client;
};
static uint64_t ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return (uint64_t)t.tv_sec * 1000000000 + (uint64_t)t.tv_nsec;
}
static struct timespec deadline(unsigned ms) {
    uint64_t n = ns() + (uint64_t)ms * 1000000;
    return (struct timespec){(time_t)(n / 1000000000), (long)(n % 1000000000)};
}
static int hex(const char *p, size_t n, int nonzero) {
    int any = 0;
    for (size_t i = 0; i < n; ++i) {
        if (!((p[i] >= '0' && p[i] <= '9') || (p[i] >= 'a' && p[i] <= 'f'))) return 0;
        any |= p[i] != '0';
    }
    return !nonzero || any;
}
static int state_valid(const char *state) {
    if (!state || !*state) return 1;
    size_t len = strlen(state);
    if (len > 512) return 0;
    char copy[513];
    memcpy(copy, state, len + 1);
    char *keys[32], *p = copy;
    size_t count = 0;
    while (p) {
        char *next = strchr(p, ',');
        if (next) *next++ = 0;
        while (*p == ' ' || *p == '\t') ++p;
        char *end = p + strlen(p);
        while (end > p && (end[-1] == ' ' || end[-1] == '\t')) *--end = 0;
        char *eq = strchr(p, '=');
        if (!eq || eq == p || !eq[1] || count == 32) return 0;
        *eq = 0;
        size_t n = strlen(p);
        char *at = strchr(p, '@');
        if (n > 256 || (at && (strchr(at + 1, '@') || at == p || at - p > 241 ||
                               strlen(at + 1) > 14 || !at[1] || !((at[1] >= 'a' && at[1] <= 'z')))))
            return 0;
        if (!((p[0] >= 'a' && p[0] <= 'z') || (at && p[0] >= '0' && p[0] <= '9'))) return 0;
        for (char *q = p; *q; ++q)
            if (!((*q >= 'a' && *q <= 'z') || (*q >= '0' && *q <= '9') || strchr("_-/ *@", *q)))
                return 0;
        if (strchr(p, ' ') || strchr(p, '/')) return 0;
        for (size_t i = 0; i < count; ++i)
            if (!strcmp(keys[i], p)) return 0;
        keys[count++] = p;
        if (strlen(eq + 1) > 256) return 0;
        for (char *q = eq + 1; *q; ++q)
            if (*q < 0x20 || *q > 0x7e || *q == '=') return 0;
        p = next;
    }
    return 1;
}
int VgiTraceContextParse(const char *parent, const char *state, struct VgiTraceContext *out) {
    if (!out) return EINVAL;
    memset(out, 0, sizeof(*out));
    size_t n = parent ? strlen(parent) : 0;
    if (n < 55 || !hex(parent, 2, 0) || !strncmp(parent, "ff", 2) || parent[2] != '-' ||
        parent[35] != '-' || parent[52] != '-' || !hex(parent + 3, 32, 1) ||
        !hex(parent + 36, 16, 1) || !hex(parent + 53, 2, 0) ||
        (!strncmp(parent, "00", 2) ? n != 55 : n > 55 && parent[55] != '-'))
        return EINVAL;
    memcpy(out->traceparent, parent, 55);
    memcpy(out->traceparent, "00", 2);
    if (state_valid(state) && state)
        snprintf(out->tracestate, sizeof(out->tracestate), "%s", state);
    return 0;
}
void VgiTraceContextSwap(const struct VgiTraceContext *ctx, struct VgiTraceContext *previous) {
    if (previous) *previous = current;
    current = ctx ? *ctx : (struct VgiTraceContext){0};
}
void VgiTraceContextCurrent(struct VgiTraceContext *out) {
    if (out) *out = current;
}
static char *metadata_text(const char *md, const char *key) {
    struct ArrowStringView v = VgiMetadataGet(md, key);
    if (!v.data || v.size_bytes > 1024 || memchr(v.data, 0, (size_t)v.size_bytes)) return NULL;
    char *s = malloc((size_t)v.size_bytes + 1);
    if (s) {
        memcpy(s, v.data, (size_t)v.size_bytes);
        s[v.size_bytes] = 0;
    }
    return s;
}
static struct VgiTraceContext metadata_context(const char *md) {
    char *parent = metadata_text(md, "traceparent"), *state = metadata_text(md, "tracestate");
    struct VgiTraceContext context;
    VgiTraceContextParse(parent, state, &context);
    free(parent);
    free(state);
    return context;
}
static void identifiers(struct VgiTraceSpan *s, struct VgiTraceContext parent) {
    uint8_t data[16];
    char id[33];
    randombytes_buf(data, sizeof(data));
    sodium_bin2hex(id, sizeof(id), data, sizeof(data));
    if (parent.traceparent[0]) {
        memcpy(id, parent.traceparent + 3, 32);
        memcpy(s->parent, parent.traceparent + 36, 16);
    }
    randombytes_buf(data, 8);
    char span[17];
    sodium_bin2hex(span, sizeof(span), data, 8);
    snprintf(s->context.traceparent, sizeof(s->context.traceparent), "00-%s-%s-%s", id, span,
             parent.traceparent[0] ? parent.traceparent + 53 : "01");
    snprintf(s->context.tracestate, sizeof(s->context.tracestate), "%s", parent.tracestate);
}
struct VgiTraceSpan *VgiTraceStart(struct VgiTelemetry *t, const char *protocol, const char *method,
                                   const char *type, const char *transport, int client,
                                   const char *metadata, const char *parent, const char *state) {
    if (!t) return NULL;
    struct VgiTraceSpan *s = calloc(1, sizeof(*s));
    if (!s) return NULL;
    s->telemetry = t;
    s->client = client;
    s->start = ns();
    clock_gettime(CLOCK_MONOTONIC, &s->monotonic);
    s->protocol = strdup(protocol ? protocol : "");
    s->method = strdup(method ? method : "");
    s->type = strdup(type ? type : "");
    s->transport = strdup(transport ? transport : "");
    if (!s->protocol || !s->method || !s->type || !s->transport) {
        free(s->protocol);
        free(s->method);
        free(s->type);
        free(s->transport);
        free(s);
        return NULL;
    }
    struct VgiTraceContext context = metadata_context(metadata);
    if (parent)
        VgiTraceContextParse(parent, state, &context);
    else if (!context.traceparent[0] && client)
        context = current;
    identifiers(s, context);
    return s;
}
const struct VgiTraceContext *VgiTraceSpanContext(struct VgiTraceSpan *s) {
    return s ? &s->context : NULL;
}
void VgiTraceReparent(struct VgiTraceSpan *s, const char *md) {
    if (!s) return;
    struct VgiTraceContext p = metadata_context(md);
    if (!p.traceparent[0]) return;
    memcpy(s->context.traceparent + 3, p.traceparent + 3, 32);
    memcpy(s->context.traceparent + 53, p.traceparent + 53, 2);
    memcpy(s->parent, p.traceparent + 36, 16);
    snprintf(s->context.tracestate, sizeof(s->context.tracestate), "%s", p.tracestate);
}
void VgiTraceError(struct VgiTraceSpan *s, const char *type) {
    if (s) snprintf(s->error, sizeof(s->error), "%s", type ? type : "RpcError");
}
int VgiTraceInject(struct VgiTraceSpan *s, struct ArrowBuffer *metadata) {
    const struct VgiTraceContext *c = s ? &s->context : &current;
    if (!c->traceparent[0]) return 0;
    if (!s && VgiMetadataGet((const char *)metadata->data, "traceparent").data) return 0;
    int rc = VgiMetadataSet(metadata, "traceparent", c->traceparent);
    if (!rc && c->tracestate[0]) rc = VgiMetadataSet(metadata, "tracestate", c->tracestate);
    return rc;
}
static void string(struct json_object *o, const char *k, const char *v) {
    json_object_object_add(o, k, json_object_new_string(v));
}
static void integer(struct json_object *o, const char *k, uint64_t v) {
    char b[32];
    snprintf(b, sizeof(b), "%" PRIu64, v);
    string(o, k, b);
}
static void attr(struct json_object *a, const char *key, const char *value) {
    struct json_object *entry = json_object_new_object(), *v = json_object_new_object();
    string(entry, "key", key);
    string(v, "stringValue", value);
    json_object_object_add(entry, "value", v);
    json_object_array_add(a, entry);
}
static struct json_object *attributes(struct VgiTraceSpan *s, int error) {
    struct json_object *a = json_object_new_array();
    attr(a, "rpc.system", "vgi_rpc");
    attr(a, "rpc.service", s->protocol);
    attr(a, "rpc.method", s->method);
    attr(a, "rpc.vgi_rpc.method_type", s->type);
    attr(a, "network.transport", s->transport);
    attr(a, "status", error ? "error" : "ok");
    return a;
}
static struct json_object *resource(struct VgiTelemetry *t, const char *scope_key,
                                    const char *data_key, struct json_object *data) {
    struct json_object *r = json_object_new_object(), *res = json_object_new_object(),
                       *a = json_object_new_array();
    attr(a, "service.name", t->service);
    json_object_object_add(res, "attributes", a);
    json_object_object_add(r, "resource", res);
    struct json_object *scopes = json_object_new_array(), *group = json_object_new_object(),
                       *scope = json_object_new_object();
    string(scope, "name", "vgi_rpc");
    string(scope, "version", "0.1.0");
    json_object_object_add(group, "scope", scope);
    json_object_object_add(group, data_key, data);
    json_object_array_add(scopes, group);
    json_object_object_add(r, scope_key, scopes);
    return r;
}
static struct json_object *metric(struct VgiTraceSpan *s, int histogram, uint64_t end,
                                  double duration, int error) {
    struct json_object *m = json_object_new_object(), *kind = json_object_new_object(),
                       *points = json_object_new_array(), *p = json_object_new_object();
    char name[64];
    snprintf(name, sizeof(name), "rpc.%s.%s", s->client ? "client" : "server",
             histogram ? "duration" : "requests");
    string(m, "name", name);
    string(m, "unit", histogram ? "s" : "{request}");
    integer(p, "startTimeUnixNano", s->start);
    integer(p, "timeUnixNano", end);
    json_object_object_add(p, "attributes", attributes(s, error));
    if (histogram) {
        integer(p, "count", 1);
        json_object_object_add(p, "sum", json_object_new_double(duration));
        struct json_object *counts = json_object_new_array(), *bounds = json_object_new_array();
        const double limits[] = {.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10};
        int bucket = 0;
        for (size_t i = 0; i < sizeof(limits) / sizeof(limits[0]); ++i) {
            json_object_array_add(bounds, json_object_new_double(limits[i]));
            if (duration > limits[i]) ++bucket;
        }
        for (int i = 0; i <= (int)(sizeof(limits) / sizeof(limits[0])); ++i)
            json_object_array_add(counts, json_object_new_string(i == bucket ? "1" : "0"));
        json_object_object_add(p, "bucketCounts", counts);
        json_object_object_add(p, "explicitBounds", bounds);
        json_object_object_add(p, "min", json_object_new_double(duration));
        json_object_object_add(p, "max", json_object_new_double(duration));
    } else {
        integer(p, "asInt", 1);
        json_object_object_add(kind, "isMonotonic", json_object_new_boolean(1));
    }
    json_object_array_add(points, p);
    json_object_object_add(kind, "dataPoints", points);
    json_object_object_add(kind, "aggregationTemporality", json_object_new_int(1));
    json_object_object_add(m, histogram ? "histogram" : "sum", kind);
    return m;
}
void VgiTraceEnd(struct VgiTraceSpan *s, int failed) {
    if (!s) return;
    struct VgiTelemetry *t = s->telemetry;
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    double duration = (double)(now.tv_sec - s->monotonic.tv_sec) +
                      (double)(now.tv_nsec - s->monotonic.tv_nsec) / 1e9;
    uint64_t end = s->start + (uint64_t)(duration * 1e9);
    int error = failed || s->error[0];
    struct Record *record = calloc(1, sizeof(*record));
    if (record && t->options.enable_tracing &&
        (strtol(s->context.traceparent + 53, NULL, 16) & 1)) {
        struct json_object *span = json_object_new_object(), *list = json_object_new_array();
        char trace[33], id[17];
        memcpy(trace, s->context.traceparent + 3, 32);
        trace[32] = 0;
        memcpy(id, s->context.traceparent + 36, 16);
        id[16] = 0;
        string(span, "traceId", trace);
        string(span, "spanId", id);
        if (s->parent[0]) string(span, "parentSpanId", s->parent);
        if (s->context.tracestate[0]) string(span, "traceState", s->context.tracestate);
        char name[512];
        snprintf(name, sizeof(name), "%s/%s", s->protocol, s->method);
        string(span, "name", name);
        json_object_object_add(span, "kind", json_object_new_int(s->client ? 3 : 2));
        integer(span, "startTimeUnixNano", s->start);
        integer(span, "endTimeUnixNano", end);
        struct json_object *a = attributes(s, error);
        if (s->error[0]) attr(a, "error.type", s->error);
        json_object_object_add(span, "attributes", a);
        struct json_object *status = json_object_new_object();
        json_object_object_add(status, "code", json_object_new_int(error ? 2 : 1));
        json_object_object_add(span, "status", status);
        json_object_array_add(list, span);
        record->span = resource(t, "scopeSpans", "spans", list);
    }
    if (record && t->options.enable_metrics) {
        struct json_object *list = json_object_new_array();
        json_object_array_add(list, metric(s, 0, end, duration, error));
        json_object_array_add(list, metric(s, 1, end, duration, error));
        record->metrics = resource(t, "scopeMetrics", "metrics", list);
    }
    pthread_mutex_lock(&t->mutex);
    if (!record || t->stopping || t->stats.queued >= t->options.max_queue) {
        t->stats.dropped++;
        if (record) {
            json_object_put(record->span);
            json_object_put(record->metrics);
            free(record);
        }
    } else if (!record->span && !record->metrics)
        free(record);
    else {
        if (t->tail)
            t->tail->next = record;
        else
            t->head = record;
        t->tail = record;
        t->stats.queued++;
        t->stats.recorded++;
        pthread_cond_signal(&t->changed);
    }
    pthread_mutex_unlock(&t->mutex);
    free(s->protocol);
    free(s->method);
    free(s->type);
    free(s->transport);
    free(s);
}
static size_t response_body(char *data, size_t size, size_t count, void *state) {
    struct ArrowBuffer *body = state;
    if (count && size > SIZE_MAX / count) return 0;
    size_t n = size * count;
    if (n > 65536 || body->size_bytes > 65536 - (int64_t)n ||
        ArrowBufferAppend(body, data, (int64_t)n))
        return 0;
    return n;
}
static pthread_once_t curl_once = PTHREAD_ONCE_INIT;
static void curl_init(void) { (void)curl_global_init(CURL_GLOBAL_DEFAULT); }
static int send_json(struct VgiTelemetry *t, const char *signal, struct json_object *payload) {
    const char *json = json_object_to_json_string_ext(payload, JSON_C_TO_STRING_PLAIN);
    if (t->options.export_json)
        return t->options.export_json(t->options.export_state, signal, json, strlen(json));
    size_t n = strlen(t->endpoint) + strlen(signal) + 16;
    char *url = malloc(n);
    if (!url) return ENOMEM;
    snprintf(url, n, "%s/v1/%s", t->endpoint, signal);
    CURL *curl = curl_easy_init();
    if (!curl) {
        free(url);
        return ENOMEM;
    }
    struct curl_slist *headers = curl_slist_append(NULL, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)strlen(json));
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)t->options.timeout_ms);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, "http,https");
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
    if (t->ca) curl_easy_setopt(curl, CURLOPT_CAINFO, t->ca);
    struct ArrowBuffer response;
    ArrowBufferInit(&response);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, response_body);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    CURLcode code;
    long status = 0;
    for (int attempt = 0;; ++attempt) {
        response.size_bytes = 0;
        code = curl_easy_perform(curl);
        status = 0;
        if (!code) curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
        int retryable = code == CURLE_COULDNT_CONNECT || code == CURLE_OPERATION_TIMEDOUT ||
                        code == CURLE_SEND_ERROR || code == CURLE_RECV_ERROR || status == 429 ||
                        status == 502 || status == 503 || status == 504;
        if (!retryable || attempt == 2) break;
        struct timespec wait = {0, 50000000L << attempt};
        nanosleep(&wait, NULL);
    }
    int rc = code || status < 200 || status >= 300 ? EIO : 0;
    if (!rc && response.size_bytes) {
        struct json_tokener *parser = json_tokener_new();
        struct json_object *result =
            parser ? json_tokener_parse_ex(parser, (const char *)response.data,
                                           (int)response.size_bytes)
                   : NULL;
        if (!result || !json_object_is_type(result, json_type_object)) rc = EIO;
        struct json_object *partial =
            result ? json_object_object_get(result, "partialSuccess") : NULL;
        if (partial &&
            (json_object_get_int64(json_object_object_get(partial, "rejectedSpans")) ||
             json_object_get_int64(json_object_object_get(partial, "rejectedDataPoints"))))
            rc = EIO;
        json_object_put(result);
        if (parser) json_tokener_free(parser);
    }
    ArrowBufferReset(&response);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    free(url);
    return rc;
}
static struct json_object *aggregate_metrics(struct VgiTelemetry *t,
                                             struct json_object *resources) {
    struct json_object *groups = json_object_new_object();
    uint64_t end = ns();
    for (size_t i = 0; i < json_object_array_length(resources); ++i) {
        struct json_object *r = json_object_array_get_idx(resources, i);
        struct json_object *scope =
            json_object_array_get_idx(json_object_object_get(r, "scopeMetrics"), 0);
        struct json_object *metrics = json_object_object_get(scope, "metrics");
        for (size_t j = 0; j < json_object_array_length(metrics); ++j) {
            struct json_object *m = json_object_array_get_idx(metrics, j);
            int histogram = json_object_object_get(m, "histogram") != NULL;
            const char *kind = histogram ? "histogram" : "sum";
            struct json_object *p = json_object_array_get_idx(
                json_object_object_get(json_object_object_get(m, kind), "dataPoints"), 0);
            const char *name = json_object_get_string(json_object_object_get(m, "name"));
            const char *attrs = json_object_to_json_string_ext(
                json_object_object_get(p, "attributes"), JSON_C_TO_STRING_PLAIN);
            size_t size = strlen(name) + strlen(attrs) + 2;
            char *key = malloc(size);
            if (!key) continue;
            snprintf(key, size, "%s:%s", name, attrs);
            struct json_object *existing = json_object_object_get(groups, key);
            if (!existing) {
                integer(p, "startTimeUnixNano", t->metric_start);
                integer(p, "timeUnixNano", end);
                json_object_object_add(groups, key, json_object_get(m));
            } else {
                struct json_object *dest = json_object_array_get_idx(
                    json_object_object_get(json_object_object_get(existing, kind), "dataPoints"),
                    0);
                const char *count = histogram ? "count" : "asInt";
                integer(dest, count,
                        json_object_get_uint64(json_object_object_get(dest, count)) +
                            json_object_get_uint64(json_object_object_get(p, count)));
                if (histogram) {
                    double sum = json_object_get_double(json_object_object_get(dest, "sum")) +
                                 json_object_get_double(json_object_object_get(p, "sum"));
                    json_object_object_add(dest, "sum", json_object_new_double(sum));
                    double lo = json_object_get_double(json_object_object_get(dest, "min")),
                           hi = json_object_get_double(json_object_object_get(dest, "max"));
                    double min = json_object_get_double(json_object_object_get(p, "min")),
                           max = json_object_get_double(json_object_object_get(p, "max"));
                    json_object_object_add(dest, "min",
                                           json_object_new_double(lo < min ? lo : min));
                    json_object_object_add(dest, "max",
                                           json_object_new_double(hi > max ? hi : max));
                    struct json_object *a = json_object_object_get(dest, "bucketCounts"),
                                       *b = json_object_object_get(p, "bucketCounts");
                    for (size_t k = 0; k < json_object_array_length(a); ++k) {
                        uint64_t n = json_object_get_uint64(json_object_array_get_idx(a, k)) +
                                     json_object_get_uint64(json_object_array_get_idx(b, k));
                        char value[32];
                        snprintf(value, sizeof(value), "%" PRIu64, n);
                        json_object_array_put_idx(a, k, json_object_new_string(value));
                    }
                }
            }
            free(key);
        }
    }
    struct json_object *list = json_object_new_array(), *output = json_object_new_array();
    json_object_object_foreach(groups, key, value) {
        (void)key;
        json_object_array_add(list, json_object_get(value));
    }
    if (json_object_array_length(list))
        json_object_array_add(output, resource(t, "scopeMetrics", "metrics", list));
    else
        json_object_put(list);
    json_object_put(groups);
    json_object_put(resources);
    t->metric_start = end;
    return output;
}
static void *export_loop(void *state) {
    struct VgiTelemetry *t = state;
    pthread_mutex_lock(&t->mutex);
    for (;;) {
        while (!t->head && !t->stopping) pthread_cond_wait(&t->changed, &t->mutex);
        if (!t->head && t->stopping) break;
        struct json_object *spans = json_object_new_array(), *metrics = json_object_new_array();
        for (int i = 0; i < 64 && t->head; ++i) {
            struct Record *r = t->head;
            t->head = r->next;
            t->stats.queued--;
            if (r->span) json_object_array_add(spans, r->span);
            if (r->metrics) json_object_array_add(metrics, r->metrics);
            free(r);
        }
        if (!t->head) t->tail = NULL;
        t->inflight = 1;
        pthread_mutex_unlock(&t->mutex);
        int failures = 0;
        if (json_object_array_length(spans)) {
            struct json_object *o = json_object_new_object();
            json_object_object_add(o, "resourceSpans", spans);
            failures += !!send_json(t, "traces", o);
            json_object_put(o);
        } else
            json_object_put(spans);
        if (json_object_array_length(metrics)) {
            metrics = aggregate_metrics(t, metrics);
            struct json_object *o = json_object_new_object();
            json_object_object_add(o, "resourceMetrics", metrics);
            failures += !!send_json(t, "metrics", o);
            json_object_put(o);
        } else
            json_object_put(metrics);
        pthread_mutex_lock(&t->mutex);
        t->stats.export_failures += (uint64_t)failures;
        t->inflight = 0;
        pthread_cond_broadcast(&t->changed);
    }
    pthread_mutex_unlock(&t->mutex);
    return NULL;
}
void VgiTelemetryOptionsInit(struct VgiTelemetryOptions *o) {
    memset(o, 0, sizeof(*o));
    o->endpoint = getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
    o->service_name = getenv("OTEL_SERVICE_NAME");
    o->timeout_ms = 2000;
    o->max_queue = 1024;
    o->enable_tracing = o->enable_metrics = 1;
}
int VgiTelemetryNew(const struct VgiTelemetryOptions *o, struct VgiTelemetry **out,
                    struct ArrowError *error) {
    if (!o || !out || *out || !o->max_queue || !o->timeout_ms) return EINVAL;
    if (sodium_init() < 0) return EIO;
    pthread_once(&curl_once, curl_init);
    struct VgiTelemetry *t = calloc(1, sizeof(*t));
    if (!t) return ENOMEM;
    int rc = pthread_mutex_init(&t->mutex, NULL);
    if (rc) {
        free(t);
        return rc;
    }
    rc = pthread_cond_init(&t->changed, NULL);
    if (rc) {
        pthread_mutex_destroy(&t->mutex);
        free(t);
        return rc;
    }
    t->options = *o;
    t->metric_start = ns();
    t->endpoint = strdup(o->endpoint ? o->endpoint : "http://localhost:4318");
    t->service = strdup(o->service_name ? o->service_name : "vgi-rpc-c");
    t->ca = o->tls_ca_file ? strdup(o->tls_ca_file) : NULL;
    if (!t->endpoint || !t->service || (o->tls_ca_file && !t->ca)) rc = ENOMEM;
    if (!rc) {
        size_t n = strlen(t->endpoint);
        while (n && t->endpoint[n - 1] == '/') t->endpoint[--n] = 0;
        if (!o->export_json && strncmp(t->endpoint, "http://", 7) &&
            strncmp(t->endpoint, "https://", 8))
            rc = EINVAL;
    }
    if (!rc) rc = pthread_create(&t->thread, NULL, export_loop, t);
    if (rc) {
        free(t->endpoint);
        free(t->service);
        free(t->ca);
        pthread_cond_destroy(&t->changed);
        pthread_mutex_destroy(&t->mutex);
        free(t);
        ArrowErrorSet(error, "Cannot initialize telemetry: %s", strerror(rc));
    } else
        *out = t;
    return rc;
}
int VgiTelemetryFlush(struct VgiTelemetry *t, unsigned timeout_ms) {
    if (!t) return EINVAL;
    struct timespec end = deadline(timeout_ms);
    int rc = 0;
    pthread_mutex_lock(&t->mutex);
    while ((t->head || t->inflight) && !rc)
        rc = pthread_cond_timedwait(&t->changed, &t->mutex, &end);
    if (!rc && t->stats.export_failures) rc = EIO;
    pthread_mutex_unlock(&t->mutex);
    return rc;
}
void VgiTelemetryGetStats(struct VgiTelemetry *t, struct VgiTelemetryStats *out) {
    if (!t || !out) return;
    pthread_mutex_lock(&t->mutex);
    *out = t->stats;
    pthread_mutex_unlock(&t->mutex);
}
void VgiTelemetryFree(struct VgiTelemetry *t) {
    if (!t) return;
    pthread_mutex_lock(&t->mutex);
    t->stopping = 1;
    pthread_cond_signal(&t->changed);
    pthread_mutex_unlock(&t->mutex);
    pthread_join(t->thread, NULL);
    free(t->endpoint);
    free(t->service);
    free(t->ca);
    pthread_cond_destroy(&t->changed);
    pthread_mutex_destroy(&t->mutex);
    free(t);
}
