# HTTP hosting

Initialize `VgiHttpOptions` with `VgiHttpOptionsInit`, configure it, then call
`VgiRpcServerServeHttp`. The default listener is loopback, the mount prefix is
`/vgi`, and port zero selects an available port. The server prints `PORT:<port>`
after binding. RPC endpoints are `/{prefix}/{protocol}/{method}` with `/init`
and `/exchange` suffixes for streams. Health and capability discovery are mounted
at the configured prefix.

| Setting | Default |
| --- | --- |
| Request body limit | 64 MiB |
| Response wire limit | 256 MiB |
| Externalized response limit | 1 GiB |
| Retained streams / sticky sessions | 4,096 each |
| Stream / sticky-session TTL | 300 seconds |
| Concurrent HTTP requests | 64 |
| Compression | Enabled |

HTTP serving uses libevent. Terminate public TLS at a reverse proxy; the native
client supports HTTPS directly. Configure `token_key` as a shared 64-digit hex
key when stateless continuations can land on different workers. The default is a
random process key. Stateless callbacks are described in [the server guide](server.md#streams).

## Authentication and identity

`bearer_tokens` and `optional_bearer_tokens` configure token-to-principal mappings.
For dynamic credentials, supply `authenticate` or `authenticate_request`.
Identity values are copied into the call, including claims and authentication
time. Invalid supplied credentials are rejected even when anonymous requests
are otherwise permitted. Framework Identity and rotated grant-key helpers are
available in `identity.h` and `credentials.h`.

Proxy proofs support off, allow, and require modes, origin binding, key rotation,
clock skew, and a bounded nonce replay cache. Set the proof options explicitly
for a proxy deployment. Authentication and proof checks run before application
dispatch. Standardized failures expose reason codes without credentials.

Sticky sessions are opt-in. `VgiRpcSessionOpen` transfers owned application state
only on success. `SessionGet` borrows it for the request; close, expiration, or
server cleanup releases it. Tokens bind identity and worker. Draining refuses
new opens while existing bound sessions can still operate. Applications can
expose an authenticated control route for drain configuration. Process shutdown
stops accepting requests and cleans up sessions; it does not provide a separate
network grace period for idle sticky sessions.

`access_log` writes JSON lines with request/stream correlation, timing, identity,
status, and field summaries. Argument values and credentials are omitted. CORS
origin configuration advertises the RPC headers and methods.

## External payloads

`VgiRpcServerSetExternal` installs a `VgiExternalOptions` policy and storage
allocator callback before serving. The allocator returns separately owned PUT
and GET URLs plus an expiry. `VgiExternalPublish` encodes/compresses and uploads
an IPC payload; `VgiExternalPointer` builds the zero-row pointer batch.

HTTP output externalization observes an Arrow-buffer threshold and per-response
upload/wire caps. Logs and their data batch share one uploaded payload.
The upload-control endpoint vends method-bound upload/download URL pairs.
Pre-published references can be returned on every transport. Automatic output
threshold externalization is currently an HTTP server feature; raw workers can
return pre-published references explicitly.

Resolution on client and server validates the URL on every redirect hop, bounds
encoded and decompressed bytes, checks the optional SHA-256 digest, and verifies
the inner schema and complete IPC framing. Nested pointers and multiple data
batches are rejected. Inner application metadata and log records are preserved.
Storage signing/credentials belong to the allocator supplied by the application;
there are no bundled S3/GCS credential-provider SDKs.
