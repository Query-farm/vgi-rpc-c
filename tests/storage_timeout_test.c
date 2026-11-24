/* Storage HTTP timeouts: a slow transfer that keeps moving must finish in one
 * attempt however long it takes, and a transfer where no bytes move must fail
 * within the stall timeout.
 *
 * A loopback server trickles a GET body, drains a PUT body slowly through a
 * small receive buffer, or goes silent. The stall timeout is 3 s
 * (VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS=3); each slow transfer takes about
 * 6.4 s, so the old whole-request deadline (timeout_seconds as CURLOPT_TIMEOUT)
 * would have cut it off. The PUT is 16 MiB so the client's kernel send buffer
 * (up to 4 MiB on loopback) drains in well under the stall timeout. POSIX
 * only. */
#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "external_internal.h"

#define CHECK(x)                                                                  \
    do {                                                                          \
        if (!(x)) {                                                               \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #x); \
            exit(1);                                                              \
        }                                                                         \
    } while (0)

#define SLOW_GET_BYTES (512 * 1024)
#define SLOW_PUT_BYTES (16 * 1024 * 1024)

static int listener;
static int port;
static atomic_int slow_get, slow_put, stall_get, stall_put, flaky;
static atomic_long put_received;

static double now(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}
static void sleep_ms(long ms) {
    struct timespec t = {ms / 1000, (ms % 1000) * 1000000L};
    while (nanosleep(&t, &t) && errno == EINTR) {
    }
}
static int send_all(int fd, const char *data, size_t n) {
    while (n) {
        ssize_t w = send(fd, data, n, 0);
        if (w <= 0) return -1;
        data += w;
        n -= (size_t)w;
    }
    return 0;
}
static void respond(int fd, const char *status, const char *body) {
    char head[256];
    snprintf(head, sizeof(head), "HTTP/1.1 %s\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n",
             status, strlen(body));
    send_all(fd, head, strlen(head));
    send_all(fd, body, strlen(body));
}

static void *serve(void *arg) {
    int fd = (int)(intptr_t)arg;
    char head[8192];
    size_t used = 0;
    char *end = NULL;
    while (!end && used < sizeof(head) - 1) {
        ssize_t r = recv(fd, head + used, sizeof(head) - 1 - used, 0);
        if (r <= 0) goto done;
        used += (size_t)r;
        head[used] = 0;
        end = strstr(head, "\r\n\r\n");
    }
    if (!end) goto done;
    char method[8] = {0}, path[64] = {0};
    sscanf(head, "%7s %63s", method, path);
    long length = 0;
    for (const char *p = head; p < end; ++p)
        if (!strncasecmp(p, "\r\nContent-Length:", 17)) length = strtol(p + 17, NULL, 10);
    long have = (long)(used - (size_t)(end + 4 - head));

    if (!strcmp(path, "/slow-get")) {
        atomic_fetch_add(&slow_get, 1);
        char line[128];
        snprintf(line, sizeof(line), "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n",
                 SLOW_GET_BYTES);
        send_all(fd, line, strlen(line));
        /* 512 KiB in 8 KiB slices every 100 ms: about 6.4 s. */
        static char chunk[SLOW_GET_BYTES / 64];
        memset(chunk, 'g', sizeof(chunk));
        for (size_t sent = 0; sent < SLOW_GET_BYTES;) {
            size_t n =
                SLOW_GET_BYTES - sent < sizeof(chunk) ? SLOW_GET_BYTES - sent : sizeof(chunk);
            if (send_all(fd, chunk, n)) break;
            sent += n;
            sleep_ms(100);
        }
    } else if (!strcmp(path, "/slow-put")) {
        atomic_fetch_add(&slow_put, 1);
        /* Drain 256 KiB every 100 ms (about 6.4 s for 16 MiB); the small
         * receive buffer makes the client's sends wait on us. */
        char buffer[64 * 1024];
        long tick = 0;
        while (have < length) {
            ssize_t r = recv(fd, buffer, sizeof(buffer), 0);
            if (r <= 0) goto done;
            have += r;
            tick += r;
            if (tick >= 256 * 1024) {
                tick = 0;
                sleep_ms(100);
            }
        }
        atomic_store(&put_received, have);
        respond(fd, "200 OK", "");
    } else if (!strcmp(path, "/stall-get")) {
        atomic_fetch_add(&stall_get, 1);
        const char *line = "HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\npartial";
        send_all(fd, line, strlen(line));
        char c;
        while (recv(fd, &c, 1, 0) > 0) { /* hold until the client gives up */
        }
    } else if (!strcmp(path, "/stall-put")) {
        atomic_fetch_add(&stall_put, 1);
        /* Take the whole body, then never answer. (A server that reads
         * nothing is not a clean stall: the kernel keeps accepting a little
         * more of the body every second.) */
        char buffer[64 * 1024];
        while (have < length) {
            ssize_t r = recv(fd, buffer, sizeof(buffer), 0);
            if (r <= 0) goto done;
            have += r;
        }
        while (recv(fd, buffer, 1, 0) > 0) { /* hold until the client gives up */
        }
    } else if (!strcmp(path, "/flaky")) {
        int n = atomic_fetch_add(&flaky, 1);
        respond(fd, n ? "200 OK" : "503 Service Unavailable", n ? "ok" : "busy");
    } else {
        respond(fd, "404 Not Found", "");
    }
done:
    close(fd);
    return NULL;
}

static void *accept_loop(void *unused) {
    (void)unused;
    for (;;) {
        int fd = accept(listener, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR) continue;
            return NULL;
        }
        pthread_t thread;
        pthread_create(&thread, NULL, serve, (void *)(intptr_t)fd);
        pthread_detach(thread);
    }
}

static void start_server(void) {
    listener = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(listener >= 0);
    int one = 1, small = 16 * 1024;
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    /* Inherited by accepted sockets: keeps the slow PUT from landing in a
     * multi-megabyte loopback buffer. */
    setsockopt(listener, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(bind(listener, (struct sockaddr *)&address, sizeof(address)) == 0);
    CHECK(listen(listener, 16) == 0);
    socklen_t size = sizeof(address);
    CHECK(getsockname(listener, (struct sockaddr *)&address, &size) == 0);
    port = ntohs(address.sin_port);
    pthread_t thread;
    CHECK(pthread_create(&thread, NULL, accept_loop, NULL) == 0);
    pthread_detach(thread);
}

static int request(const struct VgiExternalOptions *o, const char *method, const char *path,
                   struct ArrowBufferView body, struct ArrowBuffer *out, double *elapsed) {
    char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%d%s", port, path);
    struct ArrowError error = {{0}};
    ArrowBufferInit(out);
    double start = now();
    int rc = VgiExternalHttp(o, method, url, body, NULL, NULL, 0, out, &error);
    *elapsed = now() - start;
    fprintf(stderr, "%s %s: rc=%d in %.1f s%s%s\n", method, path, rc, *elapsed, rc ? ": " : "",
            rc ? error.message : "");
    return rc;
}

int main(void) {
    signal(SIGPIPE, SIG_IGN);
    /* The env override, and its fallbacks. */
    setenv(VGI_RPC_STORAGE_STALL_TIMEOUT_ENV, "nonsense", 1);
    CHECK(VgiStorageStallTimeoutSeconds() == 30);
    setenv(VGI_RPC_STORAGE_STALL_TIMEOUT_ENV, "0", 1);
    CHECK(VgiStorageStallTimeoutSeconds() == 30);
    unsetenv(VGI_RPC_STORAGE_STALL_TIMEOUT_ENV);
    CHECK(VgiStorageStallTimeoutSeconds() == 30);
    struct VgiExternalOptions o;
    VgiExternalOptionsInit(&o);
    CHECK(o.timeout_seconds == 30 && o.connect_timeout_seconds == 30 && o.attempts == 3);
    setenv(VGI_RPC_STORAGE_STALL_TIMEOUT_ENV, "3", 1);
    VgiExternalOptionsInit(&o);
    CHECK(o.timeout_seconds == 3);
    o.validate_url = NULL; /* loopback http */

    start_server();
    struct ArrowBuffer out;
    double elapsed;

    /* A slow download longer than the stall timeout (and than the old
     * whole-request deadline) succeeds with one request. */
    CHECK(request(&o, "GET", "/slow-get", (struct ArrowBufferView){{NULL}, 0}, &out, &elapsed) ==
          0);
    CHECK(out.size_bytes == SLOW_GET_BYTES && elapsed > 3 * o.timeout_seconds / 2.0);
    CHECK(atomic_load(&slow_get) == 1);
    ArrowBufferReset(&out);

    /* A slow upload likewise. */
    char *payload = malloc(SLOW_PUT_BYTES);
    CHECK(payload);
    memset(payload, 'p', SLOW_PUT_BYTES);
    CHECK(request(&o, "PUT", "/slow-put", (struct ArrowBufferView){{payload}, SLOW_PUT_BYTES}, &out,
                  &elapsed) == 0);
    CHECK(elapsed > 3 * o.timeout_seconds / 2.0);
    CHECK(atomic_load(&slow_put) == 1 && atomic_load(&put_received) == SLOW_PUT_BYTES);
    ArrowBufferReset(&out);

    /* A transient 503 is retried; the second attempt succeeds. */
    CHECK(request(&o, "GET", "/flaky", (struct ArrowBufferView){{NULL}, 0}, &out, &elapsed) == 0);
    CHECK(atomic_load(&flaky) == 2 && out.size_bytes == 2);
    ArrowBufferReset(&out);

    /* Stalls fail within the stall timeout (plus about a second of progress
     * callback granularity), one attempt at a time. */
    o.attempts = 1;
    CHECK(request(&o, "GET", "/stall-get", (struct ArrowBufferView){{NULL}, 0}, &out, &elapsed) ==
          EIO);
    CHECK(elapsed >= o.timeout_seconds && elapsed < o.timeout_seconds + 3);
    CHECK(request(&o, "PUT", "/stall-put", (struct ArrowBufferView){{payload}, SLOW_PUT_BYTES},
                  &out, &elapsed) == EIO);
    CHECK(elapsed >= o.timeout_seconds && elapsed < o.timeout_seconds + 3);
    /* A stall is transient: the default policy tries three times. */
    o.attempts = 3;
    CHECK(request(&o, "GET", "/stall-get", (struct ArrowBufferView){{NULL}, 0}, &out, &elapsed) ==
          EIO);
    CHECK(atomic_load(&stall_get) == 4 && elapsed < 3 * (o.timeout_seconds + 3) + 2);
    free(payload);
    puts("storage timeout tests passed");
    return 0;
}
