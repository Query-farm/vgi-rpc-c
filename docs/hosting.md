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
available in `identity.h` and `credentials.h`. The framework protocol is
`vgi_rpc.Identity.v2`: a grant's `expires_at` is a non-null
`timestamp[us, tz=UTC]`, so a `mint_grant` hook sets
`VgiIssuedGrant.expires_at_us` (microseconds since the Unix epoch). The older
`expires_at` field, in Unix seconds, is still accepted when `expires_at_us` is
zero.

### JWT access tokens

`<vgi_rpc/jwt.h>` verifies JWT bearer tokens (RFC 7519, RFC 9068) against a
JWKS. Create a `VgiJwtVerifier` from `VgiJwtOptions` (issuer, audience and JWKS
URI are required) and attach it with `VgiJwtInstall`, which sets `authenticate`,
`authenticate_early` and `release_identity`. The verifier is thread-safe and
must outlive the server.

- Algorithms come from a comma-separated allow-list (default `ES256`; ES384,
  ES512, RS256/384/512 and PS256/384/512 may be named). `none` and `HS*` are
  refused when the verifier is created. Keys come only from `jwks_uri`, never
  from token headers, and a token with a `crit` header is refused.
- `required_typ` (for example `at+jwt`) compares case-insensitively with an
  optional `application/` prefix. `iss` and `aud` match exactly;
  `single_audience` refuses a token naming several audiences. `require_exp`
  makes `exp` mandatory. `exp`, `nbf` and `iat` must be numbers and are checked
  with `leeway_seconds` of skew. The principal claim (`sub` by default) must be
  a non-empty string.
- The claim set reaches the call as `auth_claims_json`, exactly as signed, so
  claims keep their JSON types.
- `jwks_max_age_seconds` bounds how long fetched keys are used; a shorter
  `Cache-Control: max-age` wins. When it passes the keys are dropped: if the
  re-fetch fails, requests get `503` with `Retry-After`, and stale keys are
  never used. An unknown `kid` triggers a re-fetch at most once per
  `jwks_refresh_min_interval_seconds`. Each fetch has a timeout (10 s by
  default), follows no redirects and accepts at most 1 MiB.
- Input limits apply before any allocation proportional to the input: tokens
  over 16 KiB, JSON nested deeper than 16 levels, embedded NUL bytes, invalid
  base64url and trailing data are all refused.

Verification uses OpenSSL 3 (`libcrypto`), which the full server links.

### Authentication before the body

`require_authentication` makes every request authenticate except `HEAD /health`
and CORS preflights (`OPTIONS` with `Access-Control-Request-Method`). That
includes `GET /health`, other `OPTIONS` requests and unknown paths, which are
answered only after authentication succeeds. Authentication then runs before the
body is read, size-checked or decoded: an input filter inspects each request
head, and a request that `authenticate_early` predicts will be refused, or whose
declared body exceeds `max_request_bytes`, is dispatched with its body withheld.
It is authenticated first and gets `401` (or `503`), or `413` once
authenticated. The rest of that connection is discarded and the reply closes it.
`auth_challenge` replaces the `WWW-Authenticate` value on `401` responses.
Orchard hosted mode in vgi-c is built on these options.

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
allocator callback before serving. The allocator is called with the exact byte
length and lowercase hex SHA-256 of one upload and returns separately owned PUT
and GET URLs, an expiry, and the headers the PUT must carry. Backends that can
sign the length and checksum (S3/R2: `content-length` and
`x-amz-checksum-sha256`) must, so storage itself rejects a different body; the
returned headers never include `Content-Length`. `VgiExternalPublish`
encodes/compresses, hashes, and uploads an IPC payload; `VgiExternalPointer`
builds the zero-row pointer batch.

HTTP output externalization observes an Arrow-buffer threshold and per-response
upload/wire caps. Logs and their data batch share one uploaded payload.
The upload-control endpoint (`POST {prefix}/__upload_url__/init`, advertised as
`VGI-Upload-URL-Support: 2`) takes `uploads: list<struct<size: int64,
sha256: utf8>>` (1-100 entries) and answers one row per entry with
`upload_url`, `download_url`, `expires_at`, and `upload_headers`
(`map<utf8, utf8>`). The retired `count` shape, empty or oversized lists,
`size < 1`, malformed digests, and sizes above `VGI-Max-Upload-Bytes` are
refused with HTTP 400 before any URL is minted.
Pre-published references can be returned on every transport. Automatic output
threshold externalization is currently an HTTP server feature; raw workers can
return pre-published references explicitly.

Resolution on client and server validates the URL on every redirect hop, bounds
encoded and decompressed bytes, checks the SHA-256 digest (required on request
pointers, which are refused without `vgi_rpc.location.sha256`), and verifies
the inner schema and complete IPC framing. Nested pointers and multiple data
batches are rejected. Inner application metadata and log records are preserved.
Storage signing/credentials belong to the allocator supplied by the application;
there are no bundled S3/GCS credential-provider SDKs.
