# Arrow batches and IPC

[Ownership rules](../server.md#ownership) explain how to initialize, move, and release batches, schemas, buffers, and streams.

Declarations below are included directly from the public headers.

## Batch construction and access

[`vgi_rpc/batch.h` on GitHub](https://github.com/Query-farm/vgi-rpc-c/blob/main/include/vgi_rpc/batch.h)

```c title="vgi_rpc/batch.h" linenums="1"
--8<-- "include/vgi_rpc/batch.h"
```

## Complete IPC messages

[`vgi_rpc/ipc.h` on GitHub](https://github.com/Query-farm/vgi-rpc-c/blob/main/include/vgi_rpc/ipc.h)

```c title="vgi_rpc/ipc.h" linenums="1"
--8<-- "include/vgi_rpc/ipc.h"
```

## Incremental IPC streams

[`vgi_rpc/stream.h` on GitHub](https://github.com/Query-farm/vgi-rpc-c/blob/main/include/vgi_rpc/stream.h)

```c title="vgi_rpc/stream.h" linenums="1"
--8<-- "include/vgi_rpc/stream.h"
```

## Schema factories

[`vgi_rpc/schema.h` on GitHub](https://github.com/Query-farm/vgi-rpc-c/blob/main/include/vgi_rpc/schema.h)

```c title="vgi_rpc/schema.h" linenums="1"
--8<-- "include/vgi_rpc/schema.h"
```
