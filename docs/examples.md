# Calculator server and client

These are the complete example programs compiled by the default CMake build.
The website includes their source at build time, so the examples stay in sync
with the repository. Follow [getting started](getting-started.md) to run them.

## Server

The `params` and `result` factories declare the method's Arrow schemas. Each
field is a non-nullable float64. The `add` handler reads the two arguments from
the request's array view and builds one result row. The method table connects
those factories and the dispatcher to the `Calculator` protocol.

The default entry point serves stdin/stdout. Passing `--http` selects the HTTP
host while keeping the protocol registration and handler identical.

```c title="examples/calculator.c" linenums="1"
--8<-- "examples/calculator.c"
```

## Client

The client chooses a subprocess or HTTP connection from its command line,
builds a request with the exact parameter schema, and calls `VgiClientUnary`.
The returned batch belongs to the caller. Both input and output are reset
before the connection is freed.

```c title="examples/client.c" linenums="1"
--8<-- "examples/client.c"
```

## Extend the example

- Add a method and matching schemas using the [server guide](server.md).
- Add producer or exchange callbacks using [stream lifecycle rules](server.md#streams).
- Reuse subprocesses with [exclusive worker pool leases](pooling.md).
- Configure [authentication and external payloads](hosting.md) for HTTP deployments.

Source: [calculator.c](https://github.com/Query-farm/vgi-rpc-c/blob/main/examples/calculator.c)
and [client.c](https://github.com/Query-farm/vgi-rpc-c/blob/main/examples/client.c).
