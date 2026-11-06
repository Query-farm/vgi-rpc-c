#ifndef VGI_PLATFORM_H
#define VGI_PLATFORM_H

#include <errno.h>
#include <stdint.h>
#include <time.h>

#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <winsock2.h>

#define strcasecmp _stricmp
#define strncasecmp _strnicmp
#define strdup _strdup
#define strtok_r strtok_s

static inline double VgiMonotonicSeconds(void) {
    LARGE_INTEGER frequency, value;
    QueryPerformanceFrequency(&frequency);
    QueryPerformanceCounter(&value);
    return (double)value.QuadPart / (double)frequency.QuadPart;
}

static inline void VgiSleepMilliseconds(unsigned milliseconds) { Sleep(milliseconds); }
#else
#include <strings.h>

static inline double VgiMonotonicSeconds(void) {
    struct timespec value;
    clock_gettime(CLOCK_MONOTONIC, &value);
    return (double)value.tv_sec + (double)value.tv_nsec / 1000000000.0;
}

static inline void VgiSleepMilliseconds(unsigned milliseconds) {
    struct timespec delay = {(time_t)(milliseconds / 1000), (long)(milliseconds % 1000) * 1000000L};
    while (nanosleep(&delay, &delay) && errno == EINTR) {
    }
}
#endif

#endif
