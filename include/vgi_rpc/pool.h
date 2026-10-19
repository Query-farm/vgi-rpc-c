#ifndef VGI_RPC_POOL_H
#define VGI_RPC_POOL_H
#include "vgi_rpc/client.h"
#ifdef __cplusplus
extern "C" {
#endif
struct VgiWorkerPool;
struct VgiWorkerLease;
struct VgiWorkerPoolOptions {
    /* One pool per subprocess command/configuration. Strings and argv are copied;
     * callback state and external-storage policy must outlive all leases. */
    struct VgiClientOptions client;
    size_t max_workers, max_idle;
    unsigned idle_timeout_ms, acquire_timeout_ms;
};
struct VgiWorkerPoolStats {
    uint64_t spawns, reuses, discards, idle_evictions, timeouts;
    size_t active, idle, waiting;
};
void VgiWorkerPoolOptionsInit(struct VgiWorkerPoolOptions *);
int VgiWorkerPoolNew(const struct VgiWorkerPoolOptions *, struct VgiWorkerPool **,
                     struct ArrowError *);
/* Thread-safe; a lease grants exclusive use of its client. Never free that client.
 * Exhausted pools wait up to acquire_timeout_ms. No RPC is automatically retried. */
int VgiWorkerPoolAcquire(struct VgiWorkerPool *, struct VgiWorkerLease **, struct ArrowError *);
struct VgiClient *VgiWorkerLeaseClient(struct VgiWorkerLease *);
/* Application RPC errors may be reused after framing is drained. Broken peers,
 * abandoned streams, and explicitly discarded leases are terminated and reaped. */
void VgiWorkerLeaseRelease(struct VgiWorkerLease *);
void VgiWorkerLeaseDiscard(struct VgiWorkerLease *);
void VgiWorkerPoolGetStats(struct VgiWorkerPool *, struct VgiWorkerPoolStats *);
void VgiWorkerPoolClose(struct VgiWorkerPool *); /* Wake waiters; reject new borrows. */
/* Stop the idle reaper. Outstanding leases remain valid and keep the pool alive.
 * Do not begin any new pool operation after Free; existing waiters may finish. */
void VgiWorkerPoolFree(struct VgiWorkerPool *);
#ifdef __cplusplus
}
#endif
#endif
