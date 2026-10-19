#ifndef VGI_RPC_TELEMETRY_H
#define VGI_RPC_TELEMETRY_H
#include <stddef.h>
#include <stdint.h>

#include "nanoarrow/nanoarrow.h"
#ifdef __cplusplus
extern "C" {
#endif
struct VgiTelemetry;
struct VgiTraceContext {
    char traceparent[56], tracestate[513];
};
struct VgiTelemetryOptions {
    const char *endpoint; /* OTLP/HTTP base URL; /v1/traces and /v1/metrics appended. */
    const char *service_name;
    const char *tls_ca_file;
    unsigned timeout_ms;
    size_t max_queue;
    int enable_tracing, enable_metrics;
    /* Optional host exporter: receives OTLP/JSON on the export thread.
     * Return zero on success. Payload borrowed for the callback only.
     * Callback state must outlive the telemetry instance. */
    int (*export_json)(void *, const char *signal, const char *json, size_t length);
    void *export_state;
};
struct VgiTelemetryStats {
    uint64_t recorded, dropped, export_failures;
    size_t queued;
};
void VgiTelemetryOptionsInit(struct VgiTelemetryOptions *);
int VgiTelemetryNew(const struct VgiTelemetryOptions *, struct VgiTelemetry **,
                    struct ArrowError *);
/* Shared by clients and servers; configure before serving. Must outlive them.
 * Export failures never change RPC results; inspect stats/Flush for delivery. */
int VgiTelemetryFlush(struct VgiTelemetry *, unsigned timeout_ms);
void VgiTelemetryGetStats(struct VgiTelemetry *, struct VgiTelemetryStats *);
void VgiTelemetryFree(struct VgiTelemetry *);
/* W3C context for integrating an application's existing tracer. Swap is thread-
 * local; restore the returned previous context after a synchronous scope. */
int VgiTraceContextParse(const char *traceparent, const char *tracestate, struct VgiTraceContext *);
void VgiTraceContextSwap(const struct VgiTraceContext *, struct VgiTraceContext *previous);
void VgiTraceContextCurrent(struct VgiTraceContext *);
#ifdef __cplusplus
}
#endif
#endif
