# Shared memory and worker pools

Read the [shared-memory guide](../shared-memory.md) for negotiation and buffer lifetime, and the [pool guide](../pooling.md) for exclusive leases and worker replacement.

Declarations below are included directly from the public headers.

## Shared-memory regions

[`vgi_rpc/shm.h` on GitHub](https://github.com/Query-farm/vgi-rpc-c/blob/main/include/vgi_rpc/shm.h)

```c title="vgi_rpc/shm.h" linenums="1"
--8<-- "include/vgi_rpc/shm.h"
```

## Worker pools

[`vgi_rpc/pool.h` on GitHub](https://github.com/Query-farm/vgi-rpc-c/blob/main/include/vgi_rpc/pool.h)

```c title="vgi_rpc/pool.h" linenums="1"
--8<-- "include/vgi_rpc/pool.h"
```
