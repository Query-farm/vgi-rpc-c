/* The process-wide object cache (vgi-rpc STATE_REF_V1.md §11).
 *
 * Keyed by object id, never by a digest: grants mint objects for claimed
 * hashes without seeing the bytes, so a digest-keyed cache would hand one
 * principal's value to another who merely knows its hash. Single-flight,
 * byte-bounded LRU, and an entry fetched more than a day ago is a miss. */
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "object_cache_internal.h"
#include "vgi_rpc/state_ref.h"

#define ONE_GIB (1024LL * 1024 * 1024)
#define MAX_AGE_SECONDS 86400.0

struct VgiObjectBytes {
    _Atomic int64_t refs;
    int64_t size;
    uint8_t data[];
};
struct Entry {
    char *id;
    struct VgiObjectBytes *bytes;
    double fetched_at;
    struct Entry *prev, *next; /* LRU: head is oldest */
};
struct Flight {
    char *id;
    int done, rc, waiters;
    char message[1024];
    struct VgiObjectBytes *bytes;
    struct Flight *next;
};
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t landed = PTHREAD_COND_INITIALIZER;
static struct Entry *head, *tail;
static struct Flight *flights;
static int configured;
static int64_t max_bytes, resident, entry_count, hits, misses, evictions, fetches;

static double now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}
struct VgiObjectBytes *VgiObjectBytesNew(int64_t size) {
    if (size < 0 || (uint64_t)size > SIZE_MAX - sizeof(struct VgiObjectBytes)) return NULL;
    struct VgiObjectBytes *b = malloc(sizeof(*b) + (size_t)size);
    if (!b) return NULL;
    atomic_init(&b->refs, 1);
    b->size = size;
    return b;
}
uint8_t *VgiObjectBytesData(struct VgiObjectBytes *b) { return b->data; }
int64_t VgiObjectBytesSize(const struct VgiObjectBytes *b) { return b->size; }
static struct VgiObjectBytes *retain(struct VgiObjectBytes *b) {
    if (b) atomic_fetch_add(&b->refs, 1);
    return b;
}
void VgiObjectBytesRelease(struct VgiObjectBytes *b) {
    if (b && atomic_fetch_sub(&b->refs, 1) == 1) free(b);
}

static int64_t read_limit(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    char text[64] = {0};
    int64_t value = 0;
    if (fgets(text, sizeof(text), f) && strncmp(text, "max", 3)) {
        char *end = NULL;
        long long parsed = strtoll(text, &end, 10);
        if (end != text && parsed > 0) value = parsed;
    }
    fclose(f);
    return value;
}
static int64_t physical_memory(void) {
    long pages = sysconf(_SC_PHYS_PAGES), page = sysconf(_SC_PAGESIZE);
    return pages > 0 && page > 0 ? (int64_t)pages * page : 0;
}
static int64_t default_bytes(void) {
    int64_t physical = physical_memory();
    int64_t memory = read_limit("/sys/fs/cgroup/memory.max");
    if (!memory) memory = read_limit("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    /* cgroup v1 reports an effectively unlimited value as a huge number. */
    if (memory && physical && memory > physical) memory = 0;
    if (!memory) memory = physical;
    if (!memory) return ONE_GIB;
    int64_t forty = memory / 100 * 40;
    return forty < ONE_GIB ? forty : ONE_GIB;
}
static int parse_env(int64_t *out, struct ArrowError *error) {
    const char *raw = getenv("VGI_RPC_OBJECT_CACHE_BYTES");
    if (!raw || !*raw) {
        *out = default_bytes();
        return 0;
    }
    char *end = NULL;
    errno = 0;
    long long value = strtoll(raw, &end, 10);
    if (errno || end == raw || *end || value < 0) {
        if (error)
            ArrowErrorSet(error,
                          "VGI_RPC_OBJECT_CACHE_BYTES must be a non-negative integer, got %s", raw);
        return EINVAL;
    }
    *out = value;
    return 0;
}
static void unlink_entry(struct Entry *e) {
    if (e->prev)
        e->prev->next = e->next;
    else
        head = e->next;
    if (e->next)
        e->next->prev = e->prev;
    else
        tail = e->prev;
    e->prev = e->next = NULL;
}
static void drop_entry(struct Entry *e) {
    unlink_entry(e);
    resident -= e->bytes->size;
    --entry_count;
    ++evictions;
    VgiObjectBytesRelease(e->bytes);
    free(e->id);
    free(e);
}
static void trim_locked(void) {
    while (head && resident > max_bytes) drop_entry(head);
}
static void ensure_configured_locked(void) {
    if (configured) return;
    if (parse_env(&max_bytes, NULL)) max_bytes = default_bytes();
    configured = 1;
}
int VgiObjectCacheConfigure(struct ArrowError *error) {
    int64_t value = 0;
    int rc = parse_env(&value, error);
    if (rc) return rc;
    pthread_mutex_lock(&lock);
    if (!configured) {
        max_bytes = value;
        configured = 1;
    }
    pthread_mutex_unlock(&lock);
    return 0;
}
void VgiObjectCacheSetMaxBytes(int64_t bytes) {
    pthread_mutex_lock(&lock);
    max_bytes = bytes < 0 ? 0 : bytes;
    configured = 1;
    trim_locked();
    pthread_mutex_unlock(&lock);
}
void VgiObjectCacheGetStats(struct VgiObjectCacheStats *out) {
    pthread_mutex_lock(&lock);
    ensure_configured_locked();
    *out = (struct VgiObjectCacheStats){resident, entry_count, max_bytes, hits,
                                        misses,   evictions,   fetches};
    pthread_mutex_unlock(&lock);
}
int64_t VgiObjectCacheAge(double seconds) {
    pthread_mutex_lock(&lock);
    int64_t n = 0;
    for (struct Entry *e = head; e; e = e->next, ++n) e->fetched_at -= seconds;
    pthread_mutex_unlock(&lock);
    return n;
}
static struct Entry *find_locked(const char *id) {
    for (struct Entry *e = tail; e; e = e->prev)
        if (!strcmp(e->id, id)) return e;
    return NULL;
}
static void insert_locked(const char *id, struct VgiObjectBytes *bytes) {
    if (bytes->size > max_bytes) {
        fprintf(stderr,
                "vgi-rpc-c: object_cache: a %" PRId64
                "-byte value exceeds the cache bound (%" PRId64
                " bytes, VGI_RPC_OBJECT_CACHE_BYTES); resolved but not retained\n",
                bytes->size, max_bytes);
        return;
    }
    struct Entry *previous = find_locked(id);
    if (previous) {
        unlink_entry(previous);
        resident -= previous->bytes->size;
        --entry_count;
        VgiObjectBytesRelease(previous->bytes);
        free(previous->id);
        free(previous);
    }
    struct Entry *e = calloc(1, sizeof(*e));
    if (!e) return;
    e->id = strdup(id);
    if (!e->id) {
        free(e);
        return;
    }
    e->bytes = retain(bytes);
    e->fetched_at = now();
    e->prev = tail;
    if (tail)
        tail->next = e;
    else
        head = e;
    tail = e;
    resident += bytes->size;
    ++entry_count;
    trim_locked();
}
static void flight_release_locked(struct Flight *f) {
    if (--f->waiters) return;
    struct Flight **p = &flights;
    while (*p && *p != f) p = &(*p)->next;
    if (*p) *p = f->next;
    VgiObjectBytesRelease(f->bytes);
    free(f->id);
    free(f);
}
int VgiObjectCacheGetOrFetch(const char *id, VgiObjectFetch fetch, void *state,
                             struct VgiObjectBytes **out, int *hit, struct ArrowError *error) {
    if (!id || !fetch || !out || *out) return EINVAL;
    if (hit) *hit = 0;
    pthread_mutex_lock(&lock);
    ensure_configured_locked();
    struct Entry *e = find_locked(id);
    if (e && now() - e->fetched_at <= MAX_AGE_SECONDS) {
        unlink_entry(e);
        e->prev = tail;
        if (tail)
            tail->next = e;
        else
            head = e;
        tail = e;
        ++hits;
        *out = retain(e->bytes);
        if (hit) *hit = 1;
        pthread_mutex_unlock(&lock);
        return 0;
    }
    /* Aged out: the object may already be gone, so never serve it. */
    if (e) drop_entry(e);
    ++misses;
    struct Flight *f = flights;
    while (f && strcmp(f->id, id)) f = f->next;
    if (f) {
        ++f->waiters;
        while (!f->done) pthread_cond_wait(&landed, &lock);
        int rc = f->rc;
        if (!rc)
            *out = retain(f->bytes);
        else if (error)
            ArrowErrorSet(error, "%s", f->message);
        flight_release_locked(f);
        pthread_mutex_unlock(&lock);
        return rc;
    }
    f = calloc(1, sizeof(*f));
    if (f) f->id = strdup(id);
    if (!f || !f->id) {
        free(f);
        pthread_mutex_unlock(&lock);
        return ENOMEM;
    }
    f->waiters = 1;
    f->next = flights;
    flights = f;
    ++fetches;
    pthread_mutex_unlock(&lock);

    struct ArrowError local = {{0}};
    struct VgiObjectBytes *bytes = NULL;
    int rc = fetch(state, &bytes, &local);
    if (!rc && !bytes) rc = EINVAL;

    pthread_mutex_lock(&lock);
    f->rc = rc;
    if (!rc) {
        f->bytes = bytes;
        insert_locked(id, bytes);
        *out = retain(bytes);
    } else {
        VgiObjectBytesRelease(bytes);
        snprintf(f->message, sizeof(f->message), "%s", local.message);
        if (error) ArrowErrorSet(error, "%s", local.message);
    }
    f->done = 1;
    pthread_cond_broadcast(&landed);
    flight_release_locked(f);
    pthread_mutex_unlock(&lock);
    return rc;
}
