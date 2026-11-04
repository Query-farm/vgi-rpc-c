# Changelog

All notable changes to `vgi-rpc-c` are listed here. The API may change between
0.x minor releases.

## [0.1.0] — 2026-11-04

First release. C11 clients and servers for the vgi-rpc protocol, tested against
the Python reference at v0.52.3. See the
[capabilities and conformance](docs/conformance.md) page for the tested scope.

- Installing also installs nanoarrow's headers, libraries, and CMake package, which
  can conflict with another nanoarrow in the same prefix.
- Native Iroh connectors are not implemented.
