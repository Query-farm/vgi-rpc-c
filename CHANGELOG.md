# Changelog

All notable changes to `vgi-rpc-c` are listed here. The API may change between
0.x minor releases.

## [Unreleased]

Upload URL protocol v2 (wire-incompatible; pairs with vgi protocol 3.4.0).

- `__upload_url__/init` takes `uploads: list<struct<size, sha256>>` instead of
  `count` and answers `upload_headers` per URL; the server advertises
  `VGI-Upload-URL-Support: 2` and refuses the retired shape.
- `VgiExternalOptions.allocate` receives the upload's size and SHA-256 instead
  of a schema, and may return headers for the PUT
  (`VgiExternalUploadUrl.headers`, `VgiExternalUploadUrlAddHeader`).
- `VgiClientUploadUrls` takes `VgiUploadRequest` entries; the client refuses
  to upload to a v1 server, PUTs the request body verbatim with the returned
  headers, and names its digest on the pointer.
- Request pointers without `vgi_rpc.location.sha256` are refused; a digest
  mismatch reports `sha256 mismatch`.

Large values (vgi-rpc `STATE_REF_V1.md`, protocol 3.4). This runtime keeps
HTTP stream state in process, so it follows the single-replica rules (§5.4).

- `<vgi_rpc/state_ref.h>`: `VgiRpcStateRef` / `VgiRpcStateRefResolve`, held
  in memory on every transport. A ref created outside a stream's init handler
  is `state_ref_misuse`. Inline encode and decode are provided, and a presented
  stored, unknown or badly coded ref is `invalid_state_ref` (HTTP 400).
  `VgiRpcCall.handling_stream_init` marks the init handler.
- `__upload_url__/init` rows gain `object_id` and `ref`, a sealed argument
  reference bound to the grant's principal. `VgiExternalUploadUrl.object_id`
  is required for grants. `VgiRpcResolveArgumentRef` and
  `VgiRpcResolveArgumentsLocation` resolve references through the object
  cache.
- `VgiExternalOptions` gains `put_object` and `get_object`, the object store
  contract. External fetches now report HTTP 404 as `ENOENT`.
- A process-wide object cache: keyed by object id, single-flight, bounded by
  `VGI_RPC_OBJECT_CACHE_BYTES`, with a one-day age rule
  (`VgiObjectCacheGetStats`, `VgiObjectCacheAge`).
- The token codec follows the standard: `0x00` raw, `0x01` zstd (whenever it
  is smaller), and every other tag refused, explicitly on every reader.
- `stream_ttl_seconds` defaults to 21600 (6 h maximum stream duration). Idle
  in-process streams are evicted after the new `stream_idle_seconds`, default
  600.
- A `max_request_bytes` below 16384 logs a startup warning.

Blob vendor (the hosted storage service; vgi docs/testing/blob-test-catalog.md).

- `<vgi_rpc/vendor.h>`: a blob-vendor client ported from vgi-rpc-go's
  `vendor.go`. `VgiVendorNew` takes the vendor URL (https; http only for a
  loopback host) and bearer token; `VgiVendorBindExternal` makes it every
  storage role of `VgiExternalOptions`: result externalization (`allocate`),
  client upload grants (`allocate_client_upload`, advisory origin
  `client_upload`), and the object store (`put_object`, origin `state_ref`;
  `get_object` through `/v1/blobs/get`, where `not_found`, `not_uploaded` and a
  storage 404 are `ENOENT`). Signed URLs are pinned to the hosts and prefixes of
  `/v1/info` (cached 10 minutes, kept through a vendor blip). Vendor calls
  retry transport errors, 429 and 5xx; the token never reaches a signed-URL
  request or an error message.
- Raw blobs for callers that keep their own references (value-level storage
  externalization): `VgiVendorPutBlob` (purpose `rpc` or `fs`, optional scope
  and idempotency key), `VgiVendorGetBlob`, `VgiVendorDeleteBlobs`,
  `VgiVendorDeleteScope`, and `VgiVendorFromEnv`.
- `VgiExternalOptionsFromEnv` and `VgiHttpConfigureFromEnv` read
  `VGI_BLOB_VENDOR_URL`, `VGI_BLOB_VENDOR_TOKEN` (fallback `VGI_CF_DO_TOKEN`),
  `VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES` (default 1 MiB),
  `VGI_RPC_EXTERNAL_COMPRESSION`, `VGI_RPC_MAX_REQUEST_BYTES`, and the fix-1
  caps `VGI_RPC_MAX_FETCH_BYTES` (default 256 MiB, raised to the upload cap)
  and `VGI_RPC_MAX_UPLOAD_BYTES` (refused above an explicit fetch cap).
- `VgiExternalOptions` gains `allocate_client_upload` (grants use it when set)
  and `max_upload_bytes` (advertised as `VGI-Max-Upload-Bytes` and enforced at
  `__upload_url__/init`; zero means `max_fetch_bytes`, and it can never exceed
  it).
- `VgiHttpOptions` gains `capability_header_names`, `capability_header_values`
  and `capability_header_count`: application capability headers (vgi's
  `VGI-Arguments-Ref-Bytes`) sent on every response and exposed to CORS
  clients. Malformed names or values stop the server from starting.

JWT verification and authentication before the body (for Orchard hosted mode
in vgi-c).

- `<vgi_rpc/jwt.h>`: `VgiJwtVerifier` verifies JWT bearer tokens against a
  JWKS (ES256 by default; ES384/512, RS256/384/512, PS256/384/512 by name;
  never `none` or HS*), with `typ`, exact `iss`/`aud`, single audience,
  required `exp`, leeway, a hard key-set maximum age that fails closed (503),
  `Cache-Control: max-age`, and a rate-limited unknown-`kid` re-fetch. The full
  server now links OpenSSL 3 `libcrypto`.
- `VgiHttpOptions` gains `require_authentication` (every request but
  `HEAD /health` and CORS preflights authenticates, before its body is read,
  size-checked or decoded), `authenticate_early`, `release_identity` and
  `auth_challenge`.

## [0.1.0] — 2026-11-04

First release. C11 clients and servers for the vgi-rpc protocol, tested against
the Python reference at v0.52.3. See the
[capabilities and conformance](docs/conformance.md) page for the tested scope.

- Installing also installs nanoarrow's headers, libraries, and CMake package, which
  can conflict with another nanoarrow in the same prefix.
- Native Iroh connectors are not implemented.
