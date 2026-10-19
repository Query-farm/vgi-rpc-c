#ifndef VGI_TELEMETRY_INTERNAL_H
#define VGI_TELEMETRY_INTERNAL_H
#include "vgi_rpc/rpc.h"
#include "vgi_rpc/telemetry.h"
struct VgiTraceSpan;
struct VgiTraceSpan *VgiTraceStart(struct VgiTelemetry *, const char *protocol, const char *method,
                                   const char *method_type, const char *transport, int client,
                                   const char *metadata, const char *parent, const char *state);
const struct VgiTraceContext *VgiTraceSpanContext(struct VgiTraceSpan *);
void VgiTraceReparent(struct VgiTraceSpan *, const char *metadata);
void VgiTraceError(struct VgiTraceSpan *, const char *error_type);
void VgiTraceEnd(struct VgiTraceSpan *, int failed);
int VgiTraceInject(struct VgiTraceSpan *, struct ArrowBuffer *metadata);
struct VgiTelemetry *VgiRpcServerTelemetry(struct VgiRpcServer *);
#endif
