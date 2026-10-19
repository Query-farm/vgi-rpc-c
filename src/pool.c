#define _POSIX_C_SOURCE 200809L
#include "vgi_rpc/pool.h"

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "client_internal.h"

struct VgiWorkerLease {
    struct VgiWorkerPool *pool;
    struct VgiClient *client;
    struct timespec returned;
    struct VgiWorkerLease *next;
};
struct VgiWorkerPool {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    pthread_t reaper;
    struct VgiWorkerPoolOptions options;
    struct VgiWorkerPoolStats stats;
    struct VgiWorkerLease *idle;
    char **argv;
    char *protocol, *version;
    size_t references;
    int closed;
};
static struct timespec after(unsigned ms) {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    t.tv_sec += ms / 1000;
    t.tv_nsec += (long)(ms % 1000) * 1000000;
    if (t.tv_nsec >= 1000000000) {
        ++t.tv_sec;
        t.tv_nsec -= 1000000000;
    }
    return t;
}
static int expired(struct timespec start, unsigned ms) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    double elapsed =
        (double)(now.tv_sec - start.tv_sec) * 1000 + (double)(now.tv_nsec - start.tv_nsec) / 1e6;
    return elapsed >= ms;
}
static void destroy(struct VgiWorkerPool *p) {
    if (p->argv)
        for (size_t i = 0; p->argv[i]; ++i) free(p->argv[i]);
    free(p->argv);
    free(p->protocol);
    free(p->version);
    pthread_cond_destroy(&p->changed);
    pthread_mutex_destroy(&p->mutex);
    free(p);
}
/* Called with the pool locked. */
static void prune(struct VgiWorkerPool *p) {
    struct VgiWorkerLease **next = &p->idle;
    while (*next) {
        struct VgiWorkerLease *item = *next;
        int timed_out = expired(item->returned, p->options.idle_timeout_ms);
        if (p->closed || timed_out || !VgiClientReusable(item->client)) {
            *next = item->next;
            p->stats.idle--;
            p->stats.discards++;
            if (timed_out) p->stats.idle_evictions++;
            VgiClientFree(item->client);
            free(item);
            pthread_cond_broadcast(&p->changed);
        } else
            next = &item->next;
    }
}
static void *reap(void *state) {
    struct VgiWorkerPool *p = state;
    pthread_mutex_lock(&p->mutex);
    while (!p->closed) {
        prune(p);
        unsigned ms = p->options.idle_timeout_ms < 1000 ? p->options.idle_timeout_ms : 1000;
        struct timespec deadline = after(ms);
        pthread_cond_timedwait(&p->changed, &p->mutex, &deadline);
    }
    pthread_mutex_unlock(&p->mutex);
    return NULL;
}
void VgiWorkerPoolOptionsInit(struct VgiWorkerPoolOptions *o) {
    memset(o, 0, sizeof(*o));
    VgiClientOptionsInit(&o->client);
    o->max_workers = o->max_idle = 4;
    o->idle_timeout_ms = 60000;
    o->acquire_timeout_ms = 30000;
}
int VgiWorkerPoolNew(const struct VgiWorkerPoolOptions *o, struct VgiWorkerPool **out,
                     struct ArrowError *error) {
    if (!o || !out || *out || !o->max_workers || o->max_idle > o->max_workers ||
        !o->idle_timeout_ms || o->client.transport != VGI_CLIENT_STDIO || !o->client.argv ||
        !o->client.argv[0] || !o->client.protocol)
        return EINVAL;
    struct VgiWorkerPool *p = calloc(1, sizeof(*p));
    if (!p) return ENOMEM;
    int rc = pthread_mutex_init(&p->mutex, NULL);
    if (rc) {
        free(p);
        return rc;
    }
    rc = pthread_cond_init(&p->changed, NULL);
    if (rc) {
        pthread_mutex_destroy(&p->mutex);
        free(p);
        return rc;
    }
    p->options = *o;
    p->references = 1;
    size_t count = 0;
    while (o->client.argv[count]) ++count;
    p->argv = calloc(count + 1, sizeof(char *));
    p->protocol = strdup(o->client.protocol);
    p->version = strdup(o->client.protocol_version ? o->client.protocol_version : "");
    if (!p->argv || !p->protocol || !p->version) rc = ENOMEM;
    for (size_t i = 0; !rc && i < count; ++i)
        if (!(p->argv[i] = strdup(o->client.argv[i]))) rc = ENOMEM;
    p->options.client.argv = (const char *const *)p->argv;
    p->options.client.protocol = p->protocol;
    p->options.client.protocol_version = p->version;
    if (!rc) rc = pthread_create(&p->reaper, NULL, reap, p);
    if (rc) {
        destroy(p);
        ArrowErrorSet(error, "Cannot create worker pool: %s", strerror(rc));
    } else
        *out = p;
    return rc;
}
int VgiWorkerPoolAcquire(struct VgiWorkerPool *p, struct VgiWorkerLease **out,
                         struct ArrowError *error) {
    if (!p || !out || *out) return EINVAL;
    struct VgiWorkerLease *lease = NULL;
    int rc = 0;
    struct timespec deadline = after(p->options.acquire_timeout_ms);
    pthread_mutex_lock(&p->mutex);
    p->references++;
    while (!p->closed) {
        prune(p);
        if (p->idle) {
            lease = p->idle;
            p->idle = lease->next;
            lease->next = NULL;
            p->stats.idle--;
            p->stats.active++;
            p->stats.reuses++;
            break;
        }
        if (p->stats.active < p->options.max_workers) {
            lease = calloc(1, sizeof(*lease));
            if (!lease) {
                rc = ENOMEM;
                break;
            }
            lease->pool = p;
            p->stats.active++;
            pthread_mutex_unlock(&p->mutex);
            rc = VgiClientConnect(&p->options.client, &lease->client, error);
            pthread_mutex_lock(&p->mutex);
            if (rc) {
                p->stats.active--;
                free(lease);
                lease = NULL;
            } else
                p->stats.spawns++;
            if (!rc && p->closed) {
                p->stats.active--;
                p->stats.discards++;
                VgiClientFree(lease->client);
                free(lease);
                lease = NULL;
                rc = ECANCELED;
            }
            break;
        }
        p->stats.waiting++;
        rc = pthread_cond_timedwait(&p->changed, &p->mutex, &deadline);
        p->stats.waiting--;
        if (rc) {
            if (rc == ETIMEDOUT) p->stats.timeouts++;
            break;
        }
    }
    if (!lease && !rc) rc = ECANCELED;
    int dead = 0;
    if (rc) {
        dead = --p->references == 0;
        pthread_cond_broadcast(&p->changed);
    }
    pthread_mutex_unlock(&p->mutex);
    if (dead) destroy(p);
    if (rc)
        ArrowErrorSet(error, "Cannot acquire worker: %s", strerror(rc));
    else
        *out = lease;
    return rc;
}
struct VgiClient *VgiWorkerLeaseClient(struct VgiWorkerLease *lease) {
    return lease ? lease->client : NULL;
}
static void release(struct VgiWorkerLease *lease, int discard) {
    if (!lease) return;
    struct VgiWorkerPool *p = lease->pool;
    pthread_mutex_lock(&p->mutex);
    p->stats.active--;
    if (discard || p->closed || p->stats.idle >= p->options.max_idle ||
        !VgiClientReusable(lease->client)) {
        p->stats.discards++;
        VgiClientFree(lease->client);
        free(lease);
    } else {
        clock_gettime(CLOCK_MONOTONIC, &lease->returned);
        lease->next = p->idle;
        p->idle = lease;
        p->stats.idle++;
    }
    int dead = --p->references == 0;
    pthread_cond_broadcast(&p->changed);
    pthread_mutex_unlock(&p->mutex);
    if (dead) destroy(p);
}
void VgiWorkerLeaseRelease(struct VgiWorkerLease *lease) { release(lease, 0); }
void VgiWorkerLeaseDiscard(struct VgiWorkerLease *lease) { release(lease, 1); }
void VgiWorkerPoolGetStats(struct VgiWorkerPool *p, struct VgiWorkerPoolStats *out) {
    if (!p || !out) return;
    pthread_mutex_lock(&p->mutex);
    *out = p->stats;
    pthread_mutex_unlock(&p->mutex);
}
void VgiWorkerPoolClose(struct VgiWorkerPool *p) {
    if (!p) return;
    pthread_mutex_lock(&p->mutex);
    p->closed = 1;
    prune(p);
    pthread_cond_broadcast(&p->changed);
    pthread_mutex_unlock(&p->mutex);
}
void VgiWorkerPoolFree(struct VgiWorkerPool *p) {
    if (!p) return;
    VgiWorkerPoolClose(p);
    pthread_join(p->reaper, NULL);
    pthread_mutex_lock(&p->mutex);
    int dead = --p->references == 0;
    pthread_mutex_unlock(&p->mutex);
    if (dead) destroy(p);
}
