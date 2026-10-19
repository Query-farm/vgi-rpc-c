# Shared-memory transfer

Subprocess and Unix-socket clients can carry Arrow batch payloads through a
POSIX shared-memory segment. Control messages remain Arrow IPC over the original
connection. The C server accepts a client-advertised segment on these local
transports. HTTP and TCP do not attach segments.

```c
struct VgiClientOptions options;
VgiClientOptionsInit(&options);
options.protocol = "Calculator";
options.protocol_version = "1.0.0";
const char *command[] = {"./build/vgi-rpc-calculator", NULL};
options.argv = command;
options.shared_memory_size = 64 * 1024 * 1024;
/* Default threshold: 128 KiB. Zero forces every eligible nonempty batch. */
options.shared_memory_threshold = 128 * 1024;
```

Setting a size enables a `__transport_options__` handshake once per connection.
The client creates and advertises a segment only after the peer confirms
`vgi_rpc.transport.shm=true`. Unsupported peers and application errors during the
handshake fall back to inline IPC; a broken connection fails to connect.
The C server answers this reserved method before application routing and version
checks, without adding it to protocol reflection. Each subsequent request advertises
the segment name and size. The server caches attachments for the connection,
also supporting peers that advertise only once; a replacement advertisement swaps
the mapping and disconnect releases it. Direct `VgiRpcServerServeOne` calls attach
and detach per call. Responses use SHM only when the current request advertises a
segment or carries a pointer, preserving inline-only catalog/bind calls on pooled
connections. Requests, unary results, and stream data can use zero-row pointer
batches containing `vgi_rpc.shm_offset` and `vgi_rpc.shm_length`. Stream headers,
logs, empty application batches, and small or non-fitting batches stay inline.
`VGI_RPC_SHM_MIN_BATCH_BYTES` sets the server threshold; clients have the explicit
option above. Ordinary clients with size zero continue using inline IPC.

The 64 KiB `VGIS` v1 header and sorted first-fit allocation table match the
Python reference. Top-level dictionary batches use the reference's dictionary/
record-batch message representation; other batches use a complete IPC stream.
The resolver checks the header, allocation bounds, schema, and complete IPC
framing before handing data to the application. The receiving side frees the
allocation after decoding. Segment names, ownership, and private permissions are
checked on attachment. Only the creator unlinks the segment.

Returned C batches own their buffers. They survive subsequent calls, pool returns,
segment reuse, and client destruction. This implementation avoids transferring
large payloads through the pipe/socket, but still serializes and copies IPC data
into/out of the mapping; it does not expose zero-copy mmap-backed application arrays.
One lockstep connection owns each segment; do not share it among simultaneous
connections or mutate its allocator while the peer is using it.

`VgiClientSharedMemoryStats` reports batch/byte counts and inline fallbacks, so
applications and tests can confirm that the mapping actually carried payloads.
`VgiShmCreate`, `Attach`, and `Free` are available for explicit segment ownership.
The client normally manages these automatically, including pooled workers.

The native tests force mapped transfers, retain a result across reuse/unlink,
exercise exhaustion fallback, and reject malformed headers/pointers. Independent
Python peers test nested, dictionary, decimal, timestamp, null, and empty batches
in both directions, including over Unix sockets.
