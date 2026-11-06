#define _POSIX_C_SOURCE 200809L
#include "client_transport.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>

#include "client_internal.h"
#include "platform.h"

#if defined(_WIN32)

#include <io.h>
#include <ws2tcpip.h>

static INIT_ONCE winsock_once = INIT_ONCE_STATIC_INIT;
static int winsock_error;

static BOOL CALLBACK start_winsock(PINIT_ONCE once, PVOID parameter, PVOID *context) {
    (void)once;
    (void)parameter;
    (void)context;
    WSADATA data;
    winsock_error = WSAStartup(MAKEWORD(2, 2), &data);
    return TRUE;
}

static int ensure_winsock(void) {
    InitOnceExecuteOnce(&winsock_once, start_winsock, NULL, NULL);
    return winsock_error ? EIO : 0;
}

static wchar_t *utf8_to_wide(const char *value) {
    int count = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value, -1, NULL, 0);
    if (!count) return NULL;
    wchar_t *result = (wchar_t *)calloc((size_t)count, sizeof(wchar_t));
    if (!result || !MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value, -1, result, count)) {
        free(result);
        return NULL;
    }
    return result;
}

/* Quote one argv element using CommandLineToArgvW's backslash rules. */
static int append_quoted(struct ArrowBuffer *out, const char *argument) {
    wchar_t *wide = utf8_to_wide(argument);
    if (!wide) return EINVAL;
    int rc = 0;
    if (out->size_bytes) rc = ArrowBufferAppend(out, L" ", (int64_t)sizeof(wchar_t));
    int quote = !*wide || wcspbrk(wide, L" \t\n\v\"") != NULL;
    if (!rc && quote) rc = ArrowBufferAppend(out, L"\"", (int64_t)sizeof(wchar_t));
    size_t slashes = 0;
    for (const wchar_t *p = wide; !rc && *p; ++p) {
        if (*p == L'\\') {
            ++slashes;
            continue;
        }
        size_t copies = *p == L'\"' ? slashes * 2 + 1 : slashes;
        for (size_t i = 0; !rc && i < copies; ++i)
            rc = ArrowBufferAppend(out, L"\\", (int64_t)sizeof(wchar_t));
        slashes = 0;
        if (!rc) rc = ArrowBufferAppend(out, p, (int64_t)sizeof(wchar_t));
    }
    if (!rc && quote) {
        for (size_t i = 0; !rc && i < slashes * 2; ++i)
            rc = ArrowBufferAppend(out, L"\\", (int64_t)sizeof(wchar_t));
        if (!rc) rc = ArrowBufferAppend(out, L"\"", (int64_t)sizeof(wchar_t));
    } else {
        for (size_t i = 0; !rc && i < slashes; ++i)
            rc = ArrowBufferAppend(out, L"\\", (int64_t)sizeof(wchar_t));
    }
    free(wide);
    return rc;
}

static int spawn_child(struct VgiClient *c) {
    const char *const *argv = c->options.argv;
    if (!argv || !argv[0]) return EINVAL;
    SECURITY_ATTRIBUTES security = {sizeof(security), NULL, TRUE};
    HANDLE child_input = NULL, parent_output = NULL;
    HANDLE parent_input = NULL, child_output = NULL;
    if (!CreatePipe(&child_input, &parent_output, &security, 1u << 18)) return EIO;
    if (!SetHandleInformation(parent_output, HANDLE_FLAG_INHERIT, 0) ||
        !CreatePipe(&parent_input, &child_output, &security, 1u << 18) ||
        !SetHandleInformation(parent_input, HANDLE_FLAG_INHERIT, 0)) {
        CloseHandle(child_input);
        CloseHandle(parent_output);
        if (parent_input) CloseHandle(parent_input);
        if (child_output) CloseHandle(child_output);
        return EIO;
    }

    struct ArrowBuffer command;
    ArrowBufferInit(&command);
    int rc = 0;
    for (size_t i = 0; !rc && argv[i]; ++i) rc = append_quoted(&command, argv[i]);
    const wchar_t zero = 0;
    if (!rc) rc = ArrowBufferAppend(&command, &zero, (int64_t)sizeof(zero));

    STARTUPINFOW startup;
    PROCESS_INFORMATION process;
    memset(&startup, 0, sizeof(startup));
    memset(&process, 0, sizeof(process));
    startup.cb = sizeof(startup);
    startup.dwFlags = STARTF_USESTDHANDLES;
    startup.hStdInput = child_input;
    startup.hStdOutput = child_output;
    startup.hStdError = GetStdHandle(STD_ERROR_HANDLE);
    if (!rc && !CreateProcessW(NULL, (wchar_t *)command.data, NULL, NULL, TRUE, CREATE_NO_WINDOW,
                               NULL, NULL, &startup, &process))
        rc = EIO;
    CloseHandle(child_input);
    CloseHandle(child_output);
    ArrowBufferReset(&command);
    if (rc) {
        CloseHandle(parent_output);
        CloseHandle(parent_input);
        return rc;
    }
    CloseHandle(process.hThread);
    c->process_handle = process.hProcess;
    c->child = (intptr_t)process.dwProcessId;
    c->output_fd = _open_osfhandle((intptr_t)parent_output, _O_BINARY | _O_WRONLY);
    c->input_fd = _open_osfhandle((intptr_t)parent_input, _O_BINARY | _O_RDONLY);
    if (c->output_fd < 0 || c->input_fd < 0) {
        if (c->output_fd >= 0)
            _close(c->output_fd);
        else
            CloseHandle(parent_output);
        if (c->input_fd >= 0)
            _close(c->input_fd);
        else
            CloseHandle(parent_input);
        c->output_fd = c->input_fd = -1;
        return EIO;
    }
    return 0;
}

static int connect_named_pipe(struct VgiClient *c) {
    const char *name = c->options.target;
    if (!name || !*name) return EINVAL;
    double deadline = VgiMonotonicSeconds() + c->options.timeout_seconds;
    HANDLE pipe = INVALID_HANDLE_VALUE;
    while (pipe == INVALID_HANDLE_VALUE) {
        pipe = CreateFileA(name, GENERIC_READ | GENERIC_WRITE, 0, NULL, OPEN_EXISTING, 0, NULL);
        if (pipe != INVALID_HANDLE_VALUE) break;
        DWORD error = GetLastError();
        if (error != ERROR_PIPE_BUSY && error != ERROR_FILE_NOT_FOUND) return EIO;
        double left = deadline - VgiMonotonicSeconds();
        if (left <= 0) return ETIMEDOUT;
        if (error == ERROR_PIPE_BUSY)
            WaitNamedPipeA(name, (DWORD)(left * 1000.0));
        else
            VgiSleepMilliseconds(10);
    }
    DWORD mode = PIPE_READMODE_BYTE;
    SetNamedPipeHandleState(pipe, &mode, NULL, NULL);
    int fd = _open_osfhandle((intptr_t)pipe, _O_BINARY);
    if (fd < 0) {
        CloseHandle(pipe);
        return EIO;
    }
    c->input_fd = c->output_fd = fd;
    return 0;
}

static int connect_tcp(struct VgiClient *c) {
    int rc = ensure_winsock();
    if (rc) return rc;
    if (!c->options.target) return EINVAL;
    char *host = strdup(c->options.target);
    if (!host) return ENOMEM;
    char *port = strrchr(host, ':');
    if (!port) {
        free(host);
        return EINVAL;
    }
    *port++ = 0;
    struct addrinfo hints, *addresses = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    SOCKET socket_value = INVALID_SOCKET;
    if (getaddrinfo(*host ? host : "127.0.0.1", port, &hints, &addresses)) rc = EINVAL;
    for (struct addrinfo *address = addresses; !rc && address; address = address->ai_next) {
        socket_value = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
        if (socket_value == INVALID_SOCKET) continue;
        if (!connect(socket_value, address->ai_addr, (int)address->ai_addrlen)) break;
        closesocket(socket_value);
        socket_value = INVALID_SOCKET;
    }
    if (addresses) freeaddrinfo(addresses);
    free(host);
    if (socket_value == INVALID_SOCKET) return rc ? rc : ECONNREFUSED;
    BOOL no_delay = TRUE;
    setsockopt(socket_value, IPPROTO_TCP, TCP_NODELAY, (const char *)&no_delay, sizeof(no_delay));
    c->socket_handle = (intptr_t)socket_value;
    c->transport_is_socket = 1;
    return 0;
}

int VgiClientTransportOpen(struct VgiClient *c) {
    c->input_fd = c->output_fd = -1;
    c->socket_handle = -1;
    if (c->options.transport == VGI_CLIENT_STDIO) return spawn_child(c);
    if (c->options.transport == VGI_CLIENT_UNIX) return connect_named_pipe(c);
    if (c->options.transport == VGI_CLIENT_TCP) return connect_tcp(c);
    return EINVAL;
}

int VgiClientTransportReusable(struct VgiClient *c) {
    if (!c || c->broken || c->streaming || !c->process_handle || c->input_fd < 0) return 0;
    DWORD status = 0;
    if (!GetExitCodeProcess((HANDLE)c->process_handle, &status) || status != STILL_ACTIVE) return 0;
    DWORD available = 0;
    HANDLE input = (HANDLE)_get_osfhandle(c->input_fd);
    return PeekNamedPipe(input, NULL, 0, NULL, &available, NULL) && available == 0;
}

void VgiClientTransportClose(struct VgiClient *c) {
    if (c->transport_is_socket && c->socket_handle != -1) {
        closesocket((SOCKET)c->socket_handle);
        c->socket_handle = -1;
        c->transport_is_socket = 0;
    }
    if (c->output_fd >= 0 && c->output_fd != c->input_fd) _close(c->output_fd);
    if (c->input_fd >= 0) _close(c->input_fd);
    c->input_fd = c->output_fd = -1;
    if (c->process_handle) {
        HANDLE process = (HANDLE)c->process_handle;
        if (WaitForSingleObject(process, 400) == WAIT_TIMEOUT) {
            TerminateProcess(process, 1);
            WaitForSingleObject(process, 2000);
        }
        CloseHandle(process);
        c->process_handle = NULL;
        c->child = 0;
    }
}

int VgiClientTransportReady(struct VgiClient *c, int writing) {
    int fd = writing ? c->output_fd : c->input_fd;
    int timeout =
        c->options.timeout_seconds > 2147483U ? 2147483000 : (int)c->options.timeout_seconds * 1000;
    if (c->transport_is_socket) {
        WSAPOLLFD item;
        item.fd = (SOCKET)c->socket_handle;
        item.events = writing ? POLLWRNORM : POLLRDNORM;
        item.revents = 0;
        int result = WSAPoll(&item, 1, timeout);
        return result == 0 ? ETIMEDOUT : result < 0 ? EIO : 0;
    }
    if (fd < 0) return EBADF;
    if (writing) return 0;
    HANDLE pipe = (HANDLE)_get_osfhandle(fd);
    double deadline = VgiMonotonicSeconds() + c->options.timeout_seconds;
    for (;;) {
        DWORD available = 0;
        if (!PeekNamedPipe(pipe, NULL, 0, NULL, &available, NULL)) {
            DWORD error = GetLastError();
            return error == ERROR_BROKEN_PIPE ? 0 : EIO;
        }
        if (available) return 0;
        if (VgiMonotonicSeconds() >= deadline) return ETIMEDOUT;
        VgiSleepMilliseconds(1);
    }
}

int VgiClientTransportRead(struct VgiClient *c, uint8_t *data, int64_t length, int64_t *read_out) {
    unsigned count = (unsigned)(length > 65536 ? 65536 : length);
    int value = c->transport_is_socket ? recv((SOCKET)c->socket_handle, (char *)data, (int)count, 0)
                                       : _read(c->input_fd, data, count);
    if (c->transport_is_socket && value == SOCKET_ERROR) {
        int error = WSAGetLastError();
        return error == WSAEINTR         ? EINTR
               : error == WSAEWOULDBLOCK ? EAGAIN
               : error == WSAECONNRESET  ? ECONNRESET
                                         : EIO;
    }
    if (value < 0) return errno ? errno : EIO;
    *read_out = value;
    return 0;
}

int VgiClientTransportWrite(struct VgiClient *c, const uint8_t *data, int64_t length,
                            int64_t *written) {
    unsigned count = (unsigned)(length > 65536 ? 65536 : length);
    int value = c->transport_is_socket
                    ? send((SOCKET)c->socket_handle, (const char *)data, (int)count, 0)
                    : _write(c->output_fd, data, count);
    if (c->transport_is_socket && value == SOCKET_ERROR) {
        int error = WSAGetLastError();
        return error == WSAEINTR         ? EINTR
               : error == WSAEWOULDBLOCK ? EAGAIN
               : error == WSAECONNRESET  ? ECONNRESET
                                         : EIO;
    }
    if (value <= 0) return value < 0 && errno ? errno : EIO;
    *written = value;
    return 0;
}

#else

#include <netdb.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;

static int spawn_child(struct VgiClient *c) {
    const char *const *argv = c->options.argv;
    if (!argv || !argv[0]) return EINVAL;
    int input[2], output[2];
    if (pipe(input)) return errno;
    if (pipe(output)) {
        int rc = errno;
        close(input[0]);
        close(input[1]);
        return rc;
    }
    int fds[] = {input[0], input[1], output[0], output[1]};
    for (int i = 0; i < 4; ++i) fcntl(fds[i], F_SETFD, FD_CLOEXEC);
    posix_spawn_file_actions_t actions;
    int rc = posix_spawn_file_actions_init(&actions);
    if (!rc) {
        rc = posix_spawn_file_actions_adddup2(&actions, input[0], STDIN_FILENO);
        if (!rc) rc = posix_spawn_file_actions_adddup2(&actions, output[1], STDOUT_FILENO);
        for (int i = 0; !rc && i < 4; ++i) rc = posix_spawn_file_actions_addclose(&actions, fds[i]);
        pid_t child = 0;
        if (!rc) rc = posix_spawnp(&child, argv[0], &actions, NULL, (char *const *)argv, environ);
        c->child = child;
        posix_spawn_file_actions_destroy(&actions);
    }
    close(input[0]);
    close(output[1]);
    if (!rc) {
        c->input_fd = output[0];
        c->output_fd = input[1];
        fcntl(c->input_fd, F_SETFL, fcntl(c->input_fd, F_GETFL) | O_NONBLOCK);
        fcntl(c->output_fd, F_SETFL, fcntl(c->output_fd, F_GETFL) | O_NONBLOCK);
    } else {
        close(output[0]);
        close(input[1]);
    }
    return rc;
}

static int connect_socket(struct VgiClient *c) {
    int fd = -1, rc = 0;
    const char *target = c->options.target;
    if (!target) return EINVAL;
    if (c->options.transport == VGI_CLIENT_UNIX) {
        struct sockaddr_un address;
        memset(&address, 0, sizeof(address));
        address.sun_family = AF_UNIX;
        if (strlen(target) >= sizeof(address.sun_path)) return ENAMETOOLONG;
        strcpy(address.sun_path, target);
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd >= 0 && connect(fd, (struct sockaddr *)&address, sizeof(address))) {
            rc = errno;
            close(fd);
            fd = -1;
        }
    } else {
        char *host = strdup(target);
        if (!host) return ENOMEM;
        char *port = strrchr(host, ':');
        if (!port) {
            free(host);
            return EINVAL;
        }
        *port++ = 0;
        struct addrinfo hints, *addresses = NULL;
        memset(&hints, 0, sizeof(hints));
        hints.ai_socktype = SOCK_STREAM;
        rc = getaddrinfo(*host ? host : "127.0.0.1", port, &hints, &addresses) ? EINVAL : 0;
        for (struct addrinfo *address = addresses; !rc && address; address = address->ai_next) {
            fd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
            if (fd < 0) continue;
            if (!connect(fd, address->ai_addr, address->ai_addrlen)) break;
            close(fd);
            fd = -1;
        }
        if (addresses) freeaddrinfo(addresses);
        free(host);
    }
    if (fd < 0) return rc ? rc : ECONNREFUSED;
    fcntl(fd, F_SETFD, FD_CLOEXEC);
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
    c->input_fd = c->output_fd = fd;
    c->transport_is_socket = 1;
    return 0;
}

int VgiClientTransportOpen(struct VgiClient *c) {
    c->input_fd = c->output_fd = -1;
    return c->options.transport == VGI_CLIENT_STDIO ? spawn_child(c) : connect_socket(c);
}

int VgiClientTransportReusable(struct VgiClient *c) {
    if (!c || c->broken || c->streaming || c->child <= 0 || c->input_fd < 0) return 0;
    int status;
    pid_t child = (pid_t)c->child;
    pid_t result = waitpid(child, &status, WNOHANG);
    if (result == child || (result < 0 && errno == ECHILD)) {
        c->child = 0;
        c->broken = 1;
        return 0;
    }
    struct pollfd fd = {c->input_fd, POLLIN, 0};
    int rc;
    do rc = poll(&fd, 1, 0);
    while (rc < 0 && errno == EINTR);
    return rc == 0;
}

void VgiClientTransportClose(struct VgiClient *c) {
    if (c->output_fd >= 0 && c->output_fd != c->input_fd) close(c->output_fd);
    if (c->input_fd >= 0) close(c->input_fd);
    c->input_fd = c->output_fd = -1;
    if (c->child > 0) {
        int status, waited = 0;
        pid_t child = (pid_t)c->child;
        for (int i = 0; i < 40; ++i) {
            if (i == 20) kill(child, SIGTERM);
            pid_t result = waitpid(child, &status, WNOHANG);
            if (result == child || (result < 0 && errno == ECHILD)) {
                waited = 1;
                break;
            }
            VgiSleepMilliseconds(10);
        }
        if (!waited) {
            kill(child, SIGKILL);
            while (waitpid(child, &status, 0) < 0 && errno == EINTR) {
            }
        }
        c->child = 0;
    }
}

int VgiClientTransportReady(struct VgiClient *c, int writing) {
    struct pollfd item = {writing ? c->output_fd : c->input_fd, writing ? POLLOUT : POLLIN, 0};
    int timeout =
        c->options.timeout_seconds > 2147483U ? 2147483000 : (int)c->options.timeout_seconds * 1000;
    int rc;
    do rc = poll(&item, 1, timeout);
    while (rc < 0 && errno == EINTR);
    return rc == 0 ? ETIMEDOUT : rc < 0 ? errno : 0;
}

int VgiClientTransportRead(struct VgiClient *c, uint8_t *data, int64_t length, int64_t *read_out) {
    ssize_t value = read(c->input_fd, data, (size_t)(length > 65536 ? 65536 : length));
    if (value < 0) return errno;
    *read_out = value;
    return 0;
}

int VgiClientTransportWrite(struct VgiClient *c, const uint8_t *data, int64_t length,
                            int64_t *written) {
    ssize_t value = write(c->output_fd, data, (size_t)(length > 65536 ? 65536 : length));
    if (value <= 0) return value < 0 ? errno : EIO;
    *written = value;
    return 0;
}

#endif
