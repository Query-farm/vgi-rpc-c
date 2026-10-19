---
description: Build Arrow-native RPC services and clients in C11 with nanoarrow.
hide:
  - toc
---

<div class="hero" markdown>

<p class="eyebrow">C11 · NANOARROW · ARROW IPC</p>

# vgi-rpc for C

Build services around Arrow arrays.
Call them from any vgi-rpc implementation.

[Get started](getting-started.md){ .md-button .md-button--primary }
[Explore the API](api/index.md){ .md-button }

</div>

`vgi-rpc-c` implements the shared Arrow IPC RPC protocol with C11 and nanoarrow.
Its public API accepts Arrow C Data arrays and schemas. Application code owns
its schema declarations and builds batches against them; the runtime owns RPC
framing, transport I/O, continuation tokens, and error/log envelopes.

## From build to first call

```sh
git clone https://github.com/Query-farm/vgi-rpc-c.git
cd vgi-rpc-c
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
./build/vgi-rpc-example-client ./build/vgi-rpc-calculator
# 2 + 3 = 5
```

Install the [native dependencies](getting-started.md#install-dependencies) first.
The runtime and its Arrow dependencies compile as C; Python is used only for
development tooling and interoperability tests.

## Choose your next step

| Task | Guide | Public header |
| --- | --- | --- |
| Build, install, or embed the library | [Building](building.md) | CMake `vgi_rpc::vgi_rpc` |
| Register a unary or streaming service | [Server](server.md) | `vgi_rpc/rpc.h` |
| Make calls or consume streams | [Client](client.md) | `vgi_rpc/client.h` |
| Transfer batches through shared memory | [Shared memory](shared-memory.md) | `vgi_rpc/shm.h` |
| Reuse bounded subprocess workers | [Pooling](pooling.md) | `vgi_rpc/pool.h` |
| Export traces and metrics | [OpenTelemetry](telemetry.md) | `vgi_rpc/telemetry.h` |
| Configure HTTP and identity | [Hosting](hosting.md) | `vgi_rpc/http.h`, `identity.h`, `proof.h` |
| Publish or fetch external batches | [Hosting](hosting.md#external-payloads) | `vgi_rpc/external.h` |
| Read and write Arrow IPC | [Ownership](server.md#ownership) | `vgi_rpc/ipc.h`, `stream.h`, `batch.h` |
| Reproduce compatibility checks | [Conformance](conformance.md) | Shared Python test harness |

Start with the compiled [calculator server](examples.md#server) and
[client](examples.md#client), or browse the [public API reference](api/index.md).
For the canonical protocol, use the
[wire specification](https://github.com/Query-farm/vgi-rpc-python/blob/main/docs/WIRE_PROTOCOL.md).
The framework's general documentation is at [vgi-rpc.query.farm](https://vgi-rpc.query.farm).

## What the library provides

Unary calls, producer streams, and bidirectional exchanges run over subprocess
pipes, Unix sockets, TCP, and HTTP. HTTPS clients verify certificates through
libcurl. Local callers can use negotiated shared memory and bounded worker pools;
HTTP services can use authenticated continuations, identity, external payloads,
and response limits. OpenTelemetry exports client/server traces and metrics.

See [conformance and scope](conformance.md) for tested behavior and current gaps,
including native Iroh connectors and bundled OAuth/cloud credential-provider SDKs.

The separate [vgi-c](https://github.com/Query-farm/vgi-c) SDK adds the VGI
application protocol. Catalogs, DuckDB function execution, COPY, pruning, and
attach tickets belong there rather than in this RPC library.

Built by [Query.Farm](https://query.farm). Available under the
[Query Farm Source-Available License, Version 1.0](https://github.com/Query-farm/vgi-rpc-c/blob/main/LICENSE).
