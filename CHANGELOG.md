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

## [0.1.0] — 2026-11-04

First release. C11 clients and servers for the vgi-rpc protocol, tested against
the Python reference at v0.52.3. See the
[capabilities and conformance](docs/conformance.md) page for the tested scope.

- Installing also installs nanoarrow's headers, libraries, and CMake package, which
  can conflict with another nanoarrow in the same prefix.
- Native Iroh connectors are not implemented.
