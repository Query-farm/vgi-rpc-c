# Public C API

These pages display the declarations and comments from the public headers at site build time. Use the guides for lifecycle and ownership details, then consult these declarations for exact types, callbacks, and options.

The API uses Arrow C Data structures supplied by nanoarrow. Initialize option structs with their `Init` function, zero-initialize output wrappers, and release owned results as described in the [ownership guide](../server.md#ownership).

| Area | Public headers |
| --- | --- |
| [Server and methods](server.md) | `rpc.h`, `method.h` |
| [Native client](client.md) | `client.h` |
| [Arrow batches and IPC](arrow.md) | `batch.h`, `ipc.h`, `stream.h`, `schema.h` |
| [HTTP and identity](http.md) | `http.h`, `identity.h`, `credentials.h`, `proof.h` |
| [External payloads](external.md) | `external.h` |
| [Shared memory and worker pools](local.md) | `shm.h`, `pool.h` |
| [OpenTelemetry](telemetry.md) | `telemetry.h` |

The generated protocol declarations in `include/vgi_rpc/generated/protocol.h` are produced from the canonical Python models. See [schema generation](../building.md#schema-generation) for regeneration and validation.
