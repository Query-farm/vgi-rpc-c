# Native client API

`VgiClientConnect` opens an owned subprocess, Unix socket, TCP socket, or HTTP
connection. Set `VgiClientOptions.protocol` and, when declared by the service,
`protocol_version`. For stdio use a NULL-terminated argv array; for other
transports use `target`. The [compiled client example](../examples/client.c)
shows both subprocess and HTTP calls.

Build requests against the declared Arrow schema. Null samples and empty batches
still need exact field types, order, dictionary index types, and nullability.
The client accepts `VgiAnnotatedBatch` values and preserves their custom metadata.
Missing dispatch metadata is supplied from the method and connection options;
explicit caller metadata remains intact.

## Calls and streams

`VgiClientUnary` returns one annotated batch. `VgiClientDescribe` performs the
two Reflection v1 calls and returns decoded `ProtocolList` and
`ServiceDescription` batches, selecting the first application protocol.

`VgiClientStreamOpen` takes explicit producer/exchange and header flags. Use
`StreamTick` for producers and `StreamExchange` for exchanges. Outputs are
caller-owned; reset each before reusing its storage. `done` signals EOS.
A real zero-row application batch is delivered as data. HTTP init responses
can buffer several batches, which ticks consume before making another request.
The client retains the call token and sends it with every continuation and cancel.

`VgiClientStreamClose(client, 1)` cancels. Passing zero releases a stream without
sending cancellation. Raw connections complete and drain their stream framing
so the connection can be reused. `VgiClientFree` closes owned descriptors and
terminates an owned subprocess; abandoned raw streams do not require a peer drain.
The API permits one active call/stream per client and is not thread-safe.
Use [worker pool leases](pooling.md) to give concurrent callers exclusive clients.
Subprocess cleanup first gives the worker a short EOF grace period, then uses
SIGTERM and bounded SIGKILL/reaping if necessary.

Set `shared_memory_size` to enable the [shared-memory side channel](shared-memory.md)
on stdio or Unix sockets. `shared_memory_threshold` defaults to 128 KiB.
Attach `options.telemetry` to emit [OpenTelemetry spans and metrics](telemetry.md).

## Errors and logs

A nonzero return code indicates failure. `VgiClientLastError` exposes type,
message, remote traceback, canonical code, reason kind, and JSON error details.
These strings belong to the client and remain valid until its next operation.
Unknown typed error details are retained. The driver does not reinterpret errors.

`on_log` receives borrowed level, message, and JSON-extra strings. Inline logs and
logs bundled into external payloads reach this callback once, in wire order.
The library bounds subprocess/socket I/O and HTTP requests using
`timeout_seconds`. Raw writes suppress only their own SIGPIPE, preserving the
application's signal handler and mask.

## HTTP

`headers` supplies default authentication or application headers. The client
reads capabilities with `OPTIONS /health` at connection time. Set
`compression_level=0` to disable zstd request compression. Response decoding
supports identity, gzip, and zstd. `accepted_max_response_bytes` requests a
server response budget; zero leaves the server default in effect.

HTTPS uses system trust, verifies certificates and hostnames, and requires at
least TLS 1.2. `tls_ca_file` selects an additional deployment-specific trust
bundle; `tls_certificate_file` and `tls_key_file` configure client credentials.
There is no option that disables peer or hostname verification. Keep TLS path
strings and callback state alive for the lifetime of the client.

Session scopes use `VgiClientSessionBegin` and `SessionEnd`. Scopes nest and
restore the enclosing scope when popped. The client retains the server's token
and echoed routing headers. `SessionDetach` keeps a session alive after its
scope closes; otherwise end performs best-effort deletion. Tokens and echo
header arrays returned by accessors are borrowed from the active scope.

For external results, supply `options.external`. Default external URL policy
permits HTTPS; set a custom validator for other policies. NULL explicitly
permits HTTP and HTTPS and is used by local conformance fixtures. Oversized HTTP
requests can obtain upload URLs and publish their original IPC body before
sending a pointer. Fetching always occurs in this native library, including on
raw byte-stream transports. See [external payloads](hosting.md#external-payloads).
