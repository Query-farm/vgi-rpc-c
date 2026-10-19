#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "client_internal.h"
#include "protocol.h"
#include "shm_internal.h"
#include "vgi_rpc/pool.h"

#define CHECK(x)                                                    \
    do {                                                            \
        if (!(x)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); \
            abort();                                                \
        }                                                           \
    } while (0)
static void pause_ms(unsigned ms) {
    struct timespec t = {ms / 1000, (long)(ms % 1000) * 1000000};
    nanosleep(&t, NULL);
}
static void params(struct VgiAnnotatedBatch *b, const char *value) {
    struct ArrowSchema s = {0};
    struct ArrowError e = {{0}};
    CHECK(!VgiSchemaConformanceEchoStringParams(&s, &e));
    CHECK(!VgiBatchInit(&b->batch, &s, &e));
    CHECK(!VgiBatchSetString(&b->batch, "value", value));
    CHECK(!VgiBatchFinishRow(&b->batch, &e));
    ArrowSchemaRelease(&s);
}
static void check_value(struct VgiAnnotatedBatch *b, const char *expected) {
    struct ArrowError e = {{0}};
    CHECK(!VgiBatchView(&b->batch, &e));
    struct ArrowStringView v = ArrowArrayViewGetStringUnsafe(VgiBatchGet(&b->batch, "result"), 0);
    CHECK(v.size_bytes == (int64_t)strlen(expected) &&
          !memcmp(v.data, expected, (size_t)v.size_bytes));
}
static void echo(struct VgiClient *c, const char *value) {
    struct VgiAnnotatedBatch in = {0}, out = {0};
    params(&in, value);
    int rc = VgiClientUnary(c, "echo_string", &in, &out);
    if (rc) fprintf(stderr, "echo: %s\n", VgiClientLastError(c)->message);
    CHECK(!rc);
    check_value(&out, value);
    VgiAnnotatedBatchReset(&in);
    VgiAnnotatedBatchReset(&out);
}
static void stream(struct VgiClient *c, int abandon) {
    struct VgiAnnotatedBatch in = {0}, header = {0};
    struct ArrowSchema s = {0};
    struct ArrowError e = {{0}};
    CHECK(!VgiSchemaConformanceProduceNParams(&s, &e));
    CHECK(!VgiBatchInit(&in.batch, &s, &e));
    CHECK(!ArrowArrayAppendInt(in.batch.array.children[0], 2));
    CHECK(!VgiBatchFinishRow(&in.batch, &e));
    CHECK(!VgiClientStreamOpen(c, "produce_n", &in, 0, 0, &header));
    if (!abandon) {
        int done = 0, count = 0;
        while (!done) {
            struct VgiAnnotatedBatch out = {0};
            CHECK(!VgiClientStreamTick(c, NULL, &out, &done));
            count += out.batch.array.release != NULL;
            VgiAnnotatedBatchReset(&out);
        }
        CHECK(count == 2);
    }
    VgiAnnotatedBatchReset(&in);
    VgiAnnotatedBatchReset(&header);
    ArrowSchemaRelease(&s);
}
struct Waiter {
    struct VgiWorkerPool *pool;
    int rc, work;
};
static void *borrow_thread(void *state) {
    struct Waiter *w = state;
    struct VgiWorkerLease *lease = NULL;
    struct ArrowError e = {{0}};
    w->rc = VgiWorkerPoolAcquire(w->pool, &lease, &e);
    if (!w->rc) {
        if (w->work)
            for (int i = 0; i < 8; ++i) echo(VgiWorkerLeaseClient(lease), "concurrent");
        VgiWorkerLeaseRelease(lease);
    }
    return NULL;
}
static void pool_tests(const char *worker) {
    const char *argv[] = {worker, NULL};
    struct ArrowError e = {{0}};
    struct VgiWorkerPoolOptions o;
    VgiWorkerPoolOptionsInit(&o);
    o.client.argv = argv;
    o.client.protocol = VGI_CONFORMANCE_NAME;
    o.client.protocol_version = VGI_CONFORMANCE_VERSION;
    o.client.shared_memory_size = 1024 * 1024;
    o.client.shared_memory_threshold = 0;
    o.max_workers = o.max_idle = 1;
    o.acquire_timeout_ms = 30;
    o.idle_timeout_ms = 5000;
    struct VgiWorkerPool *p = NULL;
    CHECK(!VgiWorkerPoolNew(&o, &p, &e));
    struct VgiWorkerLease *a = NULL, *b = NULL;
    CHECK(!VgiWorkerPoolAcquire(p, &a, &e));
    struct VgiClient *c = VgiWorkerLeaseClient(a);
    echo(c, "pooled");
    CHECK(VgiWorkerPoolAcquire(p, &b, &e) == ETIMEDOUT && !b);
    VgiWorkerLeaseRelease(a);
    a = NULL;
    CHECK(!VgiWorkerPoolAcquire(p, &a, &e));
    CHECK(VgiWorkerLeaseClient(a) == c);
    struct VgiAnnotatedBatch in = {0}, out = {0};
    params(&in, "error");
    CHECK(VgiClientUnary(c, "unknown_method", &in, &out) == ECANCELED);
    VgiAnnotatedBatchReset(&in);
    VgiAnnotatedBatchReset(&out);
    VgiWorkerLeaseRelease(a);
    a = NULL;
    CHECK(!VgiWorkerPoolAcquire(p, &a, &e));
    CHECK(VgiWorkerLeaseClient(a) == c);
    echo(c, "after application error");
    stream(c, 1);
    VgiWorkerLeaseRelease(a);
    a = NULL;
    CHECK(!VgiWorkerPoolAcquire(p, &a, &e));
    c = VgiWorkerLeaseClient(a);
    echo(c, "replacement");
    CHECK(!kill(c->child, SIGKILL));
    pause_ms(15);
    VgiWorkerLeaseRelease(a);
    a = NULL;
    CHECK(!VgiWorkerPoolAcquire(p, &a, &e));
    echo(VgiWorkerLeaseClient(a), "after crash");
    struct VgiWorkerPoolStats stats;
    VgiWorkerPoolGetStats(p, &stats);
    CHECK(stats.spawns == 3 && stats.reuses == 2 && stats.timeouts == 1 && stats.discards == 2);
    struct Waiter waiter = {p, -1, 0};
    pthread_t thread;
    CHECK(!pthread_create(&thread, NULL, borrow_thread, &waiter));
    for (int i = 0; i < 100; ++i) {
        VgiWorkerPoolGetStats(p, &stats);
        if (stats.waiting) break;
        pause_ms(1);
    }
    CHECK(stats.waiting == 1);
    VgiWorkerPoolClose(p);
    CHECK(!pthread_join(thread, NULL));
    CHECK(waiter.rc == ECANCELED);
    VgiWorkerPoolFree(p);
    echo(VgiWorkerLeaseClient(a), "lease survives pool owner");
    VgiWorkerLeaseRelease(a);
    p = NULL;
    o.idle_timeout_ms = 30;
    CHECK(!VgiWorkerPoolNew(&o, &p, &e));
    a = NULL;
    CHECK(!VgiWorkerPoolAcquire(p, &a, &e));
    echo(VgiWorkerLeaseClient(a), "idle");
    VgiWorkerLeaseRelease(a);
    for (int i = 0; i < 200; ++i) {
        VgiWorkerPoolGetStats(p, &stats);
        if (stats.idle_evictions) break;
        pause_ms(5);
    }
    CHECK(stats.idle == 0 && stats.idle_evictions == 1);
    VgiWorkerPoolFree(p);
    p = NULL;
    o.max_workers = o.max_idle = 2;
    o.idle_timeout_ms = 5000;
    o.acquire_timeout_ms = 5000;
    CHECK(!VgiWorkerPoolNew(&o, &p, &e));
    pthread_t threads[8];
    struct Waiter workers[8];
    for (int i = 0; i < 8; ++i) {
        workers[i] = (struct Waiter){p, -1, 1};
        CHECK(!pthread_create(&threads[i], NULL, borrow_thread, &workers[i]));
    }
    for (int i = 0; i < 8; ++i) {
        CHECK(!pthread_join(threads[i], NULL));
        CHECK(!workers[i].rc);
    }
    VgiWorkerPoolGetStats(p, &stats);
    CHECK(stats.spawns <= 2 && stats.reuses >= 6 && stats.active == 0);
    VgiWorkerPoolFree(p);
}
static void shm_tests(const char *worker) {
    const char *argv[] = {worker, NULL};
    struct ArrowError e = {{0}};
    struct VgiClientOptions o;
    VgiClientOptionsInit(&o);
    o.argv = argv;
    o.protocol = VGI_CONFORMANCE_NAME;
    o.protocol_version = VGI_CONFORMANCE_VERSION;
    o.shared_memory_size = 1024 * 1024;
    o.shared_memory_threshold = 0;
    struct VgiClient *c = NULL;
    CHECK(!VgiClientConnect(&o, &c, &e));
    char *large = malloc(256 * 1024 + 1);
    CHECK(large);
    memset(large, 'q', 256 * 1024);
    large[256 * 1024] = 0;
    struct VgiAnnotatedBatch in = {0}, out = {0};
    params(&in, large);
    int rc = VgiClientUnary(c, "echo_string", &in, &out);
    if (rc) fprintf(stderr, "SHM call failed: %s (%d)\n", VgiClientLastError(c)->message, rc);
    CHECK(!rc);
    check_value(&out, large);
    for (int i = 0; i < 5; ++i) echo(c, "reuses allocation");
    struct VgiShmStats stats;
    VgiClientSharedMemoryStats(c, &stats);
    CHECK(stats.batches_written >= 6 && stats.batches_read >= 6 &&
          stats.bytes_written > 256 * 1024);
    VgiClientFree(c);
    check_value(&out, large); /* Owned buffers survive unlink. */
    VgiAnnotatedBatchReset(&in);
    VgiAnnotatedBatchReset(&out);
    free(large);
    c = NULL;
    o.shared_memory_size = 65536 + 4096;
    CHECK(!VgiClientConnect(&o, &c, &e));
    char too_large[65536];
    memset(too_large, 'f', sizeof(too_large) - 1);
    too_large[sizeof(too_large) - 1] = 0;
    echo(c, too_large);
    VgiClientSharedMemoryStats(c, &stats);
    CHECK(stats.inline_fallbacks == 1);
    VgiClientFree(c);
}
static void shm_validation(void) {
    struct ArrowError e = {{0}};
    struct VgiShm *s = NULL, *other = NULL;
    CHECK(!VgiShmCreate(1024 * 1024, &s, &e));
    CHECK(VgiShmAttach("../invalid", 1024 * 1024, &other, &e) == EINVAL);
    char path[256];
    snprintf(path, sizeof(path), "/%s", VgiShmName(s));
    int fd = shm_open(path, O_RDWR, 0600);
    CHECK(fd >= 0);
    size_t size = (size_t)VgiShmSize(s);
    uint8_t *data = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    CHECK(data != MAP_FAILED);
    close(fd);
    memset(data + 16, 255, 4);
    CHECK(VgiShmAttach(VgiShmName(s), size, &other, &e) == EINVAL && !other);
    memset(data + 16, 0, 4);
    CHECK(!VgiShmAttach(VgiShmName(s), size, &other, &e));
    VgiShmFree(other);
    struct VgiBatch pointer = {0};
    struct ArrowSchema schema = {0};
    struct ArrowBuffer metadata;
    ArrowBufferInit(&metadata);
    CHECK(!VgiSchemaRpcEmpty(&schema, &e));
    CHECK(!VgiBatchInit(&pointer, &schema, &e));
    CHECK(!VgiBatchFinish(&pointer, &e));
    CHECK(!VgiMetadataSet(&metadata, "vgi_rpc.shm_offset", "-1"));
    CHECK(!VgiMetadataSet(&metadata, "vgi_rpc.shm_length", "16"));
    CHECK(VgiShmResolve(s, &pointer, &metadata, &e) == EINVAL);
    CHECK(!VgiMetadataSet(&metadata, "vgi_rpc.shm_offset", "65536"));
    CHECK(VgiShmResolve(s, &pointer, &metadata, &e) == EINVAL); /* No allocation. */
    data[16] = 1;
    memset(data + 24, 0, 16);
    data[26] = 1;
    data[32] = 16;
    CHECK(!VgiMetadataSet(&metadata, "vgi_rpc.shm_length", "17"));
    CHECK(VgiShmResolve(s, &pointer, &metadata, &e) == EINVAL); /* Outside allocation. */
    CHECK(!VgiMetadataSet(&metadata, "vgi_rpc.shm_length", "16"));
    CHECK(VgiShmResolve(s, &pointer, &metadata, &e));
    CHECK(data[16] == 0); /* Malformed IPC released. */
    VgiBatchReset(&pointer);
    ArrowSchemaRelease(&schema);
    ArrowBufferReset(&metadata);
    munmap(data, size);
    VgiShmFree(s);
    CHECK(shm_open(path, O_RDWR, 0600) < 0 && errno == ENOENT);
}
struct ExportGate {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    int entered, released, fail;
};
static int gated_export(void *state, const char *signal, const char *json, size_t length) {
    struct ExportGate *gate = state;
    CHECK(!strcmp(signal, "traces") && length && strstr(json, "resourceSpans"));
    pthread_mutex_lock(&gate->mutex);
    gate->entered = 1;
    pthread_cond_broadcast(&gate->changed);
    while (!gate->released) pthread_cond_wait(&gate->changed, &gate->mutex);
    int fail = gate->fail;
    pthread_mutex_unlock(&gate->mutex);
    return fail;
}
static void telemetry_validation(const char *worker) {
    struct VgiTraceContext c;
    const char *valid = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    CHECK(!VgiTraceContextParse(valid, "vendor=first,vendor=duplicate", &c) && !c.tracestate[0]);
    CHECK(!VgiTraceContextParse(valid, "a=v,b=two", &c) && !strcmp(c.tracestate, "a=v,b=two"));
    const char *bad[] = {"",
                         "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
                         "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
                         "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                         "00-4BF92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                         "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra"};
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); ++i)
        CHECK(VgiTraceContextParse(bad[i], NULL, &c) && !c.traceparent[0]);
    struct ExportGate gate = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, 0, 0};
    struct VgiTelemetryOptions ot;
    VgiTelemetryOptionsInit(&ot);
    ot.export_json = gated_export;
    ot.export_state = &gate;
    ot.max_queue = 2;
    ot.enable_metrics = 0;
    struct VgiTelemetry *t = NULL;
    struct ArrowError e = {{0}};
    CHECK(!VgiTelemetryNew(&ot, &t, &e));
    struct VgiClientOptions o;
    VgiClientOptionsInit(&o);
    const char *argv[] = {worker, NULL};
    o.argv = argv;
    o.protocol = VGI_CONFORMANCE_NAME;
    o.protocol_version = VGI_CONFORMANCE_VERSION;
    o.telemetry = t;
    struct VgiClient *client = NULL;
    CHECK(!VgiClientConnect(&o, &client, &e));
    echo(client, "first export");
    pthread_mutex_lock(&gate.mutex);
    while (!gate.entered) pthread_cond_wait(&gate.changed, &gate.mutex);
    pthread_mutex_unlock(&gate.mutex);
    for (int i = 0; i < 10; ++i) echo(client, "collector stalled; RPC still works");
    struct VgiTelemetryStats stats;
    VgiTelemetryGetStats(t, &stats);
    CHECK(stats.queued <= 2 && stats.dropped >= 8);
    CHECK(VgiTelemetryFlush(t, 1) == ETIMEDOUT);
    pthread_mutex_lock(&gate.mutex);
    gate.released = 1;
    pthread_cond_broadcast(&gate.changed);
    pthread_mutex_unlock(&gate.mutex);
    CHECK(!VgiTelemetryFlush(t, 1000));
    pthread_mutex_lock(&gate.mutex);
    gate.fail = 1;
    pthread_mutex_unlock(&gate.mutex);
    echo(client, "export failure does not fail RPC");
    CHECK(VgiTelemetryFlush(t, 1000) == EIO);
    VgiTelemetryGetStats(t, &stats);
    CHECK(stats.export_failures > 0);
    VgiClientFree(client);
    VgiTelemetryFree(t);
    pthread_cond_destroy(&gate.changed);
    pthread_mutex_destroy(&gate.mutex);
}
static int logs;
static void log_received(void *state, const char *level, const char *message, const char *extra) {
    (void)state;
    (void)level;
    (void)message;
    CHECK(strstr(extra, "trace_id") && strstr(extra, "span_id"));
    ++logs;
}
static void telemetry_tests(const char *endpoint, const char *target, int http) {
    struct ArrowError e = {{0}};
    struct VgiTelemetryOptions ot;
    VgiTelemetryOptionsInit(&ot);
    ot.endpoint = endpoint;
    ot.service_name = "vgi-rpc-c-client";
    struct VgiTelemetry *t = NULL;
    CHECK(!VgiTelemetryNew(&ot, &t, &e));
    struct VgiClientOptions o;
    VgiClientOptionsInit(&o);
    o.protocol = VGI_CONFORMANCE_NAME;
    o.telemetry = t;
    o.protocol_version = VGI_CONFORMANCE_VERSION;
    o.on_log = log_received;
    const char *argv[] = {target, NULL};
    o.argv = argv;
    if (http) {
        o.transport = VGI_CLIENT_HTTP;
        o.target = target;
    }
    struct VgiClient *c = NULL;
    CHECK(!VgiClientConnect(&o, &c, &e));
    struct VgiTraceContext context, previous;
    CHECK(!VgiTraceContextParse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                                "vendor=test", &context));
    VgiTraceContextSwap(&context, &previous);
    echo(c, "traced");
    struct VgiAnnotatedBatch in = {0}, out = {0};
    params(&in, "logged");
    CHECK(!VgiClientUnary(c, "echo_with_info_log", &in, &out));
    CHECK(logs == 1);
    VgiAnnotatedBatchReset(&out);
    CHECK(VgiClientUnary(c, "unknown_method", &in, &out));
    VgiAnnotatedBatchReset(&in);
    VgiAnnotatedBatchReset(&out);
    stream(c, 0);
    VgiTraceContextSwap(&previous, NULL);
    echo(c, "new root");
    CHECK(!VgiTraceContextParse("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00", NULL,
                                &context));
    VgiTraceContextSwap(&context, &previous);
    echo(c, "unsampled");
    VgiTraceContextSwap(&previous, NULL);
    VgiClientFree(c);
    CHECK(!VgiTelemetryFlush(t, 5000));
    VgiTelemetryFree(t);
}
static void telemetry_delivery(const char *endpoint, const char *worker, int failures) {
    struct ArrowError e = {{0}};
    struct VgiTelemetryOptions ot;
    VgiTelemetryOptionsInit(&ot);
    ot.endpoint = endpoint;
    ot.timeout_ms = 1000;
    struct VgiTelemetry *t = NULL;
    CHECK(!VgiTelemetryNew(&ot, &t, &e));
    struct VgiClientOptions o;
    VgiClientOptionsInit(&o);
    const char *argv[] = {worker, NULL};
    o.argv = argv;
    o.protocol = VGI_CONFORMANCE_NAME;
    o.protocol_version = VGI_CONFORMANCE_VERSION;
    o.telemetry = t;
    struct VgiClient *client = NULL;
    CHECK(!VgiClientConnect(&o, &client, &e));
    echo(client, "RPC survives collector failure");
    VgiClientFree(client);
    CHECK(VgiTelemetryFlush(t, 5000) == (failures ? EIO : 0));
    struct VgiTelemetryStats stats;
    VgiTelemetryGetStats(t, &stats);
    CHECK(stats.recorded == 1 && stats.export_failures == (uint64_t)failures);
    VgiTelemetryFree(t);
}
int main(int argc, char **argv) {
    CHECK(argc >= 2);
    setenv("VGI_RPC_SHM_MIN_BATCH_BYTES", "0", 1);
    if (!strcmp(argv[1], "--telemetry")) {
        CHECK(argc == 5);
        telemetry_tests(argv[2], argv[4], !strcmp(argv[3], "http"));
    } else if (!strcmp(argv[1], "--telemetry-delivery")) {
        CHECK(argc == 5);
        telemetry_delivery(argv[2], argv[3], atoi(argv[4]));
    } else {
        shm_tests(argv[1]);
        shm_validation();
        pool_tests(argv[1]);
        telemetry_validation(argv[1]);
    }
    puts("Runtime feature tests passed");
    return 0;
}
