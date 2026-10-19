# Subprocess worker pools

`VgiWorkerPool` is a thread-safe pool for one subprocess command and client
configuration. Create separate pools for different commands or configurations.
Each lease owns exclusive access to one client until released. Pooling does not
make an individual `VgiClient` thread-safe.

```c
#include <vgi_rpc/pool.h>

struct VgiWorkerPoolOptions options;
VgiWorkerPoolOptionsInit(&options);
const char *command[] = {"./build/vgi-rpc-calculator", NULL};
options.client.argv = command;
options.client.protocol = "Calculator";
options.client.protocol_version = "1.0.0";
options.max_workers = 4;
options.max_idle = 4;
options.acquire_timeout_ms = 5000;

struct ArrowError error = {{0}};
struct VgiWorkerPool *pool = NULL;
int rc = VgiWorkerPoolNew(&options, &pool, &error);
struct VgiWorkerLease *lease = NULL;
if (!rc) rc = VgiWorkerPoolAcquire(pool, &lease, &error);
if (!rc) {
    struct VgiClient *client = VgiWorkerLeaseClient(lease);
    /* Build a request and call VgiClientUnary(client, ...). */
    (void)client;
    VgiWorkerLeaseRelease(lease);
}
VgiWorkerPoolFree(pool);
```

Defaults are four total/idle workers, a 30-second acquisition timeout, and a
60-second idle timeout. At capacity, acquisition waits on a condition variable
and returns `ETIMEDOUT` when its deadline expires. A background reaper evicts
expired or exited idle workers. Healthy idle workers are reused in LIFO order.
`VgiWorkerPoolGetStats` exposes spawns, reuse, discard, eviction, timeout, and
active/idle/waiter counts.

Application RPC errors can leave a worker reusable after framing has been
drained. Transport errors, exited peers, unexpected unread bytes, and abandoned
streams cause the worker to be discarded. `VgiWorkerLeaseDiscard` also forces
discard. The next acquisition spawns a replacement; failed RPCs are never
automatically replayed because they may have had side effects.

`VgiWorkerPoolClose` rejects new acquisitions, wakes waiters with `ECANCELED`,
and disposes idle workers. `Free` additionally joins the reaper and releases the
pool owner's reference. Outstanding leases remain valid until returned; the
last return releases the remaining pool storage. Begin no new pool operations
after `Free`, and return each lease exactly once. Never free the leased client
directly. Callback state, external policy, and a configured telemetry instance
must outlive the pool and all outstanding leases.

The pool copies argv and routing strings. Configure `options.client` to use
shared memory or telemetry. The pool does not alter the process environment or
working directory; workers inherit the spawning application's environment and cwd.
