# Server API

A `VgiRpcServer` hosts one or more `VgiRpcProtocol` registrations. Each protocol
names its version, hash, method table, dispatch callback, and application state.
Each `VgiMethod` supplies schema factories for parameters, result, optional
payload, and optional stream header. `stream_kind` describes producers and
exchanges for reflection; dynamically selected streams can declare UNKNOWN.

Register protocols before serving. Registrations copy protocol strings; the
method table and application state must outlive the server. Use
`VgiRpcServerAddManagedProtocol` to transfer state with a cleanup callback.
The [calculator](examples.md#server) is a complete unary service.

## Unary handlers

The dispatcher receives a `VgiRpcCall` with a validated, viewable request batch,
custom metadata, method, transport kind, and authenticated identity. Construct
`reply->value` against the declared result schema, append values, then finish
the batch. A void method still returns a valid empty batch.

Use `VgiRpcFail` for typed errors and `VgiRpcSetErrorDetails` for structured
wire details. `VgiRpcLog` and `VgiRpcLogExtra` emit in-band records. Logs and errors
are serialized by the runtime; handlers should keep stdout exclusively for RPC
when serving standard input/output.

`VgiRpcReplyPayload` wraps a batch as the binary Arrow payload used by protocol
methods that return structured payloads inside an envelope.

## Streams

An initializer fills `reply->stream` with an output schema, optional header,
`is_producer`, a `next` callback, state, and a release callback. `next` receives
an empty producer tick or an exchange batch. Input is borrowed; returned output
is moved to the runtime. Set `finished` for EOS. A callback may return its final
batch and set `finished` in the same turn. `on_cancel` is invoked for a cancel
request, and `release` disposes the state exactly once.

HTTP stateless streams additionally provide:

- `stream.serialize`: append the advancing state to an initialized Arrow buffer.
- `protocol.resume`: reconstruct the stream from the original call, authenticated
  serialized state, and frozen output schema. Do not repeat initializer side
  effects, emit the header again, or emit initialization logs.

The runtime seals the original request and resolved schema in a call token once,
then rotates a separate cursor after every turn. Cursor authentication precedes
call-token resolution, and the embedded call identifiers must match. Both tokens
bind the route and caller identity. Stateless streams use no process-local call
cache. Set `require_stateless_streams` to reject methods missing these callbacks.

Without serialization/resume callbacks, HTTP retains stream objects in a bounded
process-local table. That mode requires requests to return to the same process.
Byte-stream transports naturally retain stream state on their live connection.

### Stream-state references (protocol 3.4)

`<vgi_rpc/state_ref.h>` is the C shape of vgi-rpc's `StateRef`
(`STATE_REF_V1.md`): `VgiRpcStateRef(call, value, &ref)` wraps bytes the stream
will not change, and `VgiRpcStateRefResolve(call, ref, &view)` reads them back
on any turn. Create refs only in the stream method's own handler, while it
handles the init request; on any later turn, including the first producer turn,
`VgiRpcStateRef` fails with `state_ref_misuse` (`FAILED_PRECONDITION`). Keep the
ref in the stream state and copy it between turns.

This runtime keeps HTTP stream state in process for protocol 3.4
(`STATE_REF_V1.md` §5.4), so a ref is held in memory on every transport and is
never stored, sealed per turn or fetched. Worker code stays portable to the
runtimes that serialize state. `VgiStateRefEncode` and `VgiRpcStateRefDecode`
handle the inline wire form (`0x01 || codec || body`). A presented stored-form
ref, an unknown form byte or a bad codec is `invalid_state_ref`, answered with
HTTP 400.

The stateless mode above predates `StateRef`. Its call token carries the whole
original request, so a large request is echoed on every continuation. Against a
request cap, the turn budget, `floor((max_request_bytes - 4096) / 1.34)` minus
the call token's sealed size, is what is left for the cursor. A
multi-megabyte request leaves no budget, and its continuations exceed the cap.
VGI workers do not use this mode in 3.4. When a stateless mode is adopted, it
must carry large values as `StateRef`s rather than in the call token.

## Ownership

Zero-initialize Arrow schemas, arrays, streams, `VgiBatch`, replies, and
`VgiAnnotatedBatch` outputs. Initialize `ArrowBuffer` with `ArrowBufferInit`
before calling nanoarrow metadata builders. `VgiAnnotatedBatchReset` accepts an
empty zeroed wrapper and releases its batch and metadata.

Release a `VgiBatch` with `VgiBatchReset`, a standalone schema/array with its
Arrow release function, and a buffer with `ArrowBufferReset`. `VgiBatchView`
constructs a borrowed array view; rebuild it after changing or replacing the
array. Successful IPC reads own their data after the input bytes are released.

`VgiIpcReadBatch` accepts exactly one complete IPC stream with one data batch.
Use `VgiIpcReader` / `VgiIpcWriter` for incremental streams with dictionaries and
per-batch metadata. Their init calls move the supplied I/O stream on success.

## Lifecycle

`VgiRpcServerServe` handles persistent FILE streams. `ServeSocket` hosts Unix or
TCP connections, and `ServeHttp` hosts HTTP. An optional start hook runs once,
with the actual transport kind, before dispatch. Free the server after serving
returns. Protocol registration and transport configuration are frozen while
serving.
