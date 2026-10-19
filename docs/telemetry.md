# OpenTelemetry

The C runtime instruments client calls and server dispatch with W3C trace context,
OTLP/HTTP JSON spans, request counters, duration histograms, and in-band log
correlation. It uses the existing C json-c/libcurl dependencies. It does not link
the OpenTelemetry C++ SDK.

```c
#include <vgi_rpc/telemetry.h>
#include <vgi_rpc/rpc.h>

struct VgiTelemetryOptions options;
VgiTelemetryOptionsInit(&options);
options.endpoint = "http://127.0.0.1:4318";
options.service_name = "my-c-service";
struct VgiTelemetry *telemetry = NULL;
struct ArrowError error = {{0}};
int rc = VgiTelemetryNew(&options, &telemetry, &error);
struct VgiRpcServer *server = VgiRpcServerNew();
if (!rc) rc = VgiRpcServerSetTelemetry(server, telemetry);
/* Register protocols, serve requests, then stop/join all serving threads. */
VgiRpcServerFree(server);
if (!rc) rc = VgiTelemetryFlush(telemetry, 5000);
VgiTelemetryFree(telemetry);
```

For clients, assign the same or another instance to `VgiClientOptions.telemetry`.
Telemetry objects are thread-safe and borrowed: they must outlive all attached
servers, clients, pools, and outstanding leases. `VgiWorkerSetTelemetry` exposes
the same option in the separate `vgi-c` SDK. Its example worker enables telemetry
when `VGI_RPC_OTEL_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

`OptionsInit` reads `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_SERVICE_NAME`.
Defaults are `http://localhost:4318`, service `vgi-rpc-c`, a 2-second export
timeout, and a 1,024-record queue. `/v1/traces` and `/v1/metrics` are appended to
the base URL. HTTPS verifies certificate chains and hostnames; `tls_ca_file`
selects a trust bundle. A custom `export_json` callback can instead hand the
OTLP payloads to an application's configured exporter. Callback payloads are
borrowed and callbacks run on the export thread.

## Context and span lifetime

Incoming `traceparent` and `tracestate` are extracted from Arrow metadata or HTTP
headers. HTTP headers take precedence. Invalid parents start a new trace, and
invalid tracestate is discarded. A valid unsampled parent stays unsampled;
metrics are still recorded. Root spans are sampled. Trace/span IDs use libsodium
randomness. All scopes restore the preceding thread-local context.

Client spans cover complete unary calls or logical streams. Byte-stream server
spans cover complete dispatch, including streaming. HTTP server spans cover each
request/continuation, allowing stateless streams to move across worker processes.
Server callbacks run inside the server context: downstream C client calls inherit
it automatically. Client requests inject their context into IPC metadata and
HTTP headers. `VgiTraceContextParse`, `Current`, and `Swap` let an application
bridge an existing tracer's context; restore the previous context after a scope.

RPC errors and client cancellation set error status; logs emitted during dispatch include
`trace_id` and `span_id` in their JSON extras. Spans carry RPC system, service,
method, method type, transport, and status. Payload values, credentials, arbitrary
claims, and error-message text are not exported as attributes.

## Metrics and delivery

`rpc.server.requests` / `rpc.client.requests` are monotonic delta sums.
`rpc.server.duration` / `rpc.client.duration` are delta histograms in seconds,
with explicit bounds from 5 ms through 10 seconds. Measurements are aggregated
by method and status within an export batch. Identifiers and caller identity are
excluded from metric dimensions.

RPC threads enqueue completed records without waiting for the collector.
The export thread sends batches of up to 64 records. A full queue drops telemetry
and increments `dropped`, preserving RPC availability. Transient HTTP/network
failures receive two bounded retries; permanent and partial-rejection responses
increment `export_failures`. `Flush` waits up to its deadline and returns `EIO`
if any export has failed. `Free` drains the queue and joins the exporter, so stop
RPC producers first; a slow/unavailable collector can extend shutdown by the
bounded export attempts for queued batches.

The collector tests decode actual HTTP bodies with the official OpenTelemetry
protobuf schemas, applying the OTLP JSON hex-ID rule. They check parentage,
root isolation, unsampled calls, errors, metrics, and log correlation on stdio
and HTTP. Native tests also check malformed context, bounded queues, flush
timeouts, and exporter failures that must not affect RPC results.
Collector response tests cover transient retries, permanent failures, and partial
rejections without retrying rejected data.

The wire encodings follow [W3C Trace Context](https://www.w3.org/TR/trace-context/)
and [OTLP JSON](https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding).
