# vgi-rpc for C

`vgi-rpc-c` implements the shared Arrow IPC RPC protocol with C11 and nanoarrow.
Its public API accepts Arrow C Data arrays and schemas. Application code owns
its schema declarations and builds batches against them; the runtime owns RPC
framing, transport I/O, continuation tokens, and error/log envelopes.

| Task | Guide | Public header |
| --- | --- | --- |
| Build, install, or embed the library | [Building](building.md) | CMake `vgi_rpc::vgi_rpc` |
| Register a unary or streaming service | [Server](server.md) | `vgi_rpc/rpc.h` |
| Make calls or consume streams | [Client](client.md) | `vgi_rpc/client.h` |
| Configure HTTP and identity | [Hosting](hosting.md) | `vgi_rpc/http.h`, `identity.h`, `proof.h` |
| Publish or fetch external batches | [Hosting](hosting.md#external-payloads) | `vgi_rpc/external.h` |
| Read and write Arrow IPC | [Ownership](server.md#ownership) | `vgi_rpc/ipc.h`, `stream.h`, `batch.h` |
| Reproduce compatibility checks | [Conformance](conformance.md) | Shared Python test harness |

Start with the compiled [calculator server](../examples/calculator.c) and
[client](../examples/client.c). For the canonical protocol, use the
[wire specification](https://github.com/Query-farm/vgi-rpc-python/blob/main/docs/WIRE_PROTOCOL.md).
The framework's general documentation is at [vgi-rpc.query.farm](https://vgi-rpc.query.farm).

The separate [vgi-c](https://github.com/Query-farm/vgi-c) SDK adds the VGI
application protocol. Catalogs, DuckDB function execution, COPY, pruning, and
attach tickets belong there rather than in this RPC library.
