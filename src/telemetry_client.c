#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "telemetry_internal.h"

static _Thread_local struct VgiTraceContext current;

static int hex(const char *value, size_t length, int nonzero) {
    int any = 0;
    for (size_t i = 0; i < length; ++i) {
        if (!((value[i] >= '0' && value[i] <= '9') || (value[i] >= 'a' && value[i] <= 'f')))
            return 0;
        any |= value[i] != '0';
    }
    return !nonzero || any;
}

static int state_valid(const char *state) {
    if (!state || !*state) return 1;
    size_t length = strlen(state);
    if (length > 512) return 0;
    for (size_t i = 0; i < length; ++i)
        if ((unsigned char)state[i] < 0x20 || (unsigned char)state[i] > 0x7e) return 0;
    return 1;
}

void VgiTelemetryOptionsInit(struct VgiTelemetryOptions *options) {
    memset(options, 0, sizeof(*options));
    options->endpoint = getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
    options->service_name = getenv("OTEL_SERVICE_NAME");
    options->timeout_ms = 2000;
    options->max_queue = 1024;
    options->enable_tracing = options->enable_metrics = 1;
}

int VgiTelemetryNew(const struct VgiTelemetryOptions *options, struct VgiTelemetry **out,
                    struct ArrowError *error) {
    (void)options;
    (void)out;
    ArrowErrorSet(error, "Telemetry export is not part of the client-only runtime");
    return ENOTSUP;
}

int VgiTelemetryFlush(struct VgiTelemetry *telemetry, unsigned timeout_ms) {
    (void)telemetry;
    (void)timeout_ms;
    return ENOTSUP;
}

void VgiTelemetryGetStats(struct VgiTelemetry *telemetry, struct VgiTelemetryStats *out) {
    (void)telemetry;
    if (out) memset(out, 0, sizeof(*out));
}

void VgiTelemetryFree(struct VgiTelemetry *telemetry) { (void)telemetry; }

int VgiTraceContextParse(const char *parent, const char *state, struct VgiTraceContext *out) {
    if (!out) return EINVAL;
    memset(out, 0, sizeof(*out));
    size_t length = parent ? strlen(parent) : 0;
    if (length < 55 || !hex(parent, 2, 0) || !strncmp(parent, "ff", 2) || parent[2] != '-' ||
        parent[35] != '-' || parent[52] != '-' || !hex(parent + 3, 32, 1) ||
        !hex(parent + 36, 16, 1) || !hex(parent + 53, 2, 0) ||
        (!strncmp(parent, "00", 2) ? length != 55 : length > 55 && parent[55] != '-'))
        return EINVAL;
    memcpy(out->traceparent, parent, 55);
    memcpy(out->traceparent, "00", 2);
    if (state_valid(state) && state)
        snprintf(out->tracestate, sizeof(out->tracestate), "%s", state);
    return 0;
}

void VgiTraceContextSwap(const struct VgiTraceContext *context, struct VgiTraceContext *previous) {
    if (previous) *previous = current;
    current = context ? *context : (struct VgiTraceContext){0};
}

void VgiTraceContextCurrent(struct VgiTraceContext *out) {
    if (out) *out = current;
}

struct VgiTraceSpan *VgiTraceStart(struct VgiTelemetry *telemetry, const char *protocol,
                                   const char *method, const char *method_type,
                                   const char *transport, int client, const char *metadata,
                                   const char *parent, const char *state) {
    (void)telemetry;
    (void)protocol;
    (void)method;
    (void)method_type;
    (void)transport;
    (void)client;
    (void)metadata;
    (void)parent;
    (void)state;
    return NULL;
}

const struct VgiTraceContext *VgiTraceSpanContext(struct VgiTraceSpan *span) {
    (void)span;
    return NULL;
}

void VgiTraceReparent(struct VgiTraceSpan *span, const char *metadata) {
    (void)span;
    (void)metadata;
}

void VgiTraceError(struct VgiTraceSpan *span, const char *error_type) {
    (void)span;
    (void)error_type;
}

void VgiTraceEnd(struct VgiTraceSpan *span, int failed) {
    (void)span;
    (void)failed;
}

int VgiTraceInject(struct VgiTraceSpan *span, struct ArrowBuffer *metadata) {
    (void)span;
    if (!current.traceparent[0] || VgiMetadataGet((const char *)metadata->data, "traceparent").data)
        return 0;
    int rc = VgiMetadataSet(metadata, "traceparent", current.traceparent);
    if (!rc && current.tracestate[0])
        rc = VgiMetadataSet(metadata, "tracestate", current.tracestate);
    return rc;
}
