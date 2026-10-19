#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <limits.h>
#include <netdb.h>
#include <poll.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "rpc_internal.h"

struct Connections {
    pthread_mutex_t mutex;
    struct VgiRpcServer *worker;
    int active;
    double idle_since;
};
struct Connection {
    struct Connections *connections;
    int fd;
};
static double monotonic(void) {
    struct timespec value;
    clock_gettime(CLOCK_MONOTONIC, &value);
    return (double)value.tv_sec + (double)value.tv_nsec / 1e9;
}
static void *serve_connection(void *data) {
    struct Connection *connection = data;
    struct Connections *connections = connection->connections;
    int fd = connection->fd, out_fd = dup(fd);
    free(connection);
    FILE *input = fdopen(fd, "rb"), *output = out_fd >= 0 ? fdopen(out_fd, "wb") : NULL;
    struct ArrowError error = {{0}};
    if (input && output) {
        int rc = VgiRpcServerServe(connections->worker, input, output, &error);
        if (rc && getenv("VGI_TRACE"))
            fprintf(stderr, "vgi-rpc-c: connection: %s\n", error.message);
    }
    if (input)
        fclose(input);
    else
        close(fd);
    if (output)
        fclose(output);
    else if (out_fd >= 0)
        close(out_fd);
    pthread_mutex_lock(&connections->mutex);
    if (!--connections->active) connections->idle_since = monotonic();
    pthread_mutex_unlock(&connections->mutex);
    return NULL;
}
int VgiRpcServerServeSocket(struct VgiRpcServer *worker, const char *unix_path,
                            const char *tcp_address, double idle_seconds,
                            struct ArrowError *error) {
    VgiRpcServerBindTransport(worker, unix_path ? VGI_TRANSPORT_UNIX : VGI_TRANSPORT_TCP);
    int listener = -1, rc = 0;
    char host[256] = "127.0.0.1", port[16] = "0";
    if (unix_path) {
        struct sockaddr_un address = {0};
        address.sun_family = AF_UNIX;
        if (strlen(unix_path) >= sizeof(address.sun_path)) {
            ArrowErrorSet(error, "Unix socket path is too long");
            return ENAMETOOLONG;
        }
        strcpy(address.sun_path, unix_path);
        listener = socket(AF_UNIX, SOCK_STREAM, 0);
        if (listener < 0) rc = errno;
        if (!rc && bind(listener, (struct sockaddr *)&address, sizeof(address))) rc = errno;
        if (!rc && chmod(unix_path, 0600)) rc = errno;
    } else {
        const char *colon = strrchr(tcp_address, ':');
        if (colon) {
            size_t length = (size_t)(colon - tcp_address);
            if (!length || length >= sizeof(host) || strlen(colon + 1) >= sizeof(port))
                return EINVAL;
            memcpy(host, tcp_address, length);
            host[length] = 0;
            strcpy(port, colon + 1);
        } else {
            if (strlen(tcp_address) >= sizeof(port)) return EINVAL;
            strcpy(port, tcp_address);
        }
        char *end = NULL;
        long number = strtol(port, &end, 10);
        if (!*port || *end || number < 0 || number > 65535) return EINVAL;
        struct addrinfo hints = {0}, *addresses = NULL;
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_PASSIVE;
        int gai = getaddrinfo(host, port, &hints, &addresses);
        if (gai) {
            ArrowErrorSet(error, "%s", gai_strerror(gai));
            return EINVAL;
        }
        rc = EADDRNOTAVAIL;
        for (struct addrinfo *address = addresses; address; address = address->ai_next) {
            listener = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
            if (listener < 0) continue;
            int enabled = 1;
            setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
            if (!bind(listener, address->ai_addr, address->ai_addrlen)) {
                rc = 0;
                break;
            }
            rc = errno;
            close(listener);
            listener = -1;
        }
        freeaddrinfo(addresses);
        if (!rc) {
            struct sockaddr_storage address;
            socklen_t size = sizeof(address);
            if (getsockname(listener, (struct sockaddr *)&address, &size))
                rc = errno;
            else if (getnameinfo((struct sockaddr *)&address, size, NULL, 0, port, sizeof(port),
                                 NI_NUMERICSERV))
                rc = EINVAL;
        }
    }
    if (!rc && listen(listener, 128)) rc = errno;
    if (rc) {
        ArrowErrorSet(error, "Unable to listen: %s", strerror(rc));
        if (listener >= 0) close(listener);
        return rc;
    }
    if (unix_path)
        printf("UNIX:%s\n", unix_path);
    else
        printf("TCP:%s:%s\n", host, port);
    fflush(stdout);
    struct Connections connections = {PTHREAD_MUTEX_INITIALIZER, worker, 0, monotonic()};
    for (;;) {
        pthread_mutex_lock(&connections.mutex);
        int expired = idle_seconds > 0 && !connections.active &&
                      monotonic() - connections.idle_since >= idle_seconds;
        pthread_mutex_unlock(&connections.mutex);
        if (expired) break;
        struct pollfd pfd = {listener, POLLIN, 0};
        int ready = poll(&pfd, 1, 100);
        if (ready < 0 && errno == EINTR) continue;
        if (ready < 0) {
            rc = errno;
            break;
        }
        if (!ready) continue;
        int fd = accept(listener, NULL, NULL);
        if (fd < 0 && errno == EINTR) continue;
        if (fd < 0) {
            rc = errno;
            break;
        }
        struct Connection *connection = malloc(sizeof(*connection));
        if (!connection) {
            close(fd);
            rc = ENOMEM;
            break;
        }
        *connection = (struct Connection){&connections, fd};
        pthread_mutex_lock(&connections.mutex);
        ++connections.active;
        pthread_mutex_unlock(&connections.mutex);
        pthread_t thread;
        rc = pthread_create(&thread, NULL, serve_connection, connection);
        if (rc) {
            free(connection);
            close(fd);
            pthread_mutex_lock(&connections.mutex);
            --connections.active;
            pthread_mutex_unlock(&connections.mutex);
            break;
        }
        pthread_detach(thread);
    }
    close(listener);
    if (unix_path) unlink(unix_path);
    /* Keep the shared connection state alive until every handler has returned. */
    for (;;) {
        pthread_mutex_lock(&connections.mutex);
        int active = connections.active;
        pthread_mutex_unlock(&connections.mutex);
        if (!active) break;
        struct timespec delay = {0, 10000000};
        nanosleep(&delay, NULL);
    }
    pthread_mutex_destroy(&connections.mutex);
    if (rc) ArrowErrorSet(error, "Socket server failed: %s", strerror(rc));
    return rc;
}
