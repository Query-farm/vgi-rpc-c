/* Protocol version windows: parsing and formatting (PROTOCOL_VERSIONS_V1 §4.0). */
#include "vgi_rpc/protocol_versions.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

int VgiVersionMajorMinor(const char *text, size_t n, uint64_t out[2]) {
    if (!text || !out) return EINVAL;
    size_t i = 0;
    for (int part = 0; part < 2; ++part) {
        size_t start = i;
        out[part] = 0;
        while (i < n && text[i] >= '0' && text[i] <= '9') {
            if (out[part] > (UINT64_MAX - 9) / 10) return EINVAL;
            out[part] = out[part] * 10 + (uint64_t)(text[i++] - '0');
        }
        if (i == start || (i - start > 1 && text[start] == '0')) return EINVAL;
        if (part == 0 && (i >= n || text[i++] != '.')) return EINVAL;
    }
    if (i < n) {
        if (text[i++] != '.') return EINVAL;
        size_t start = i;
        while (i < n && text[i] >= '0' && text[i] <= '9') ++i;
        if (i == start || i != n || (i - start > 1 && text[start] == '0')) return EINVAL;
    }
    return 0;
}
static int less(const uint64_t a[2], const uint64_t b[2]) {
    return a[0] < b[0] || (a[0] == b[0] && a[1] < b[1]);
}
static int blank(char c) { return c == ' ' || c == '\t'; }
int VgiVersionWindowParse(const char *text, size_t n, struct VgiVersionWindow *out) {
    if (!text || !out) return EINVAL;
    while (n && blank(*text)) ++text, --n;
    while (n && blank(text[n - 1])) --n;
    const char *dash = memchr(text, '-', n);
    size_t low = dash ? (size_t)(dash - text) : n;
    while (low && blank(text[low - 1])) --low;
    if (VgiVersionMajorMinor(text, low, out->min)) return EINVAL;
    if (dash) {
        const char *high = dash + 1;
        size_t rest = n - (size_t)(high - text);
        while (rest && blank(*high)) ++high, --rest;
        if (VgiVersionMajorMinor(high, rest, out->max)) return EINVAL;
    } else {
        out->max[0] = out->min[0];
        out->max[1] = out->min[1];
    }
    return less(out->max, out->min) ? EINVAL : 0;
}
void VgiVersionWindowFormat(const struct VgiVersionWindow *w, char *out, size_t size) {
    snprintf(out, size, "%llu.%llu-%llu.%llu", (unsigned long long)w->min[0],
             (unsigned long long)w->min[1], (unsigned long long)w->max[0],
             (unsigned long long)w->max[1]);
}
int VgiVersionWindowContains(const struct VgiVersionWindow *w, const uint64_t version[2]) {
    return !less(version, w->min) && !less(w->max, version);
}
static int name_char(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' ||
           c == '.' || c == ':' || c == '/' || c == '-';
}
int VgiVersionsFind(const char *advertisement, const char *protocol, struct VgiVersionWindow *out) {
    if (!advertisement || !protocol || !out) return 0;
    size_t want = strlen(protocol);
    const char *entry = advertisement;
    for (;;) {
        const char *comma = strchr(entry, ',');
        size_t n = comma ? (size_t)(comma - entry) : strlen(entry);
        const char *e = entry;
        while (n && blank(*e)) ++e, --n;
        const char *eq = memchr(e, '=', n);
        if (eq) {
            size_t k = (size_t)(eq - e);
            while (k && blank(e[k - 1])) --k;
            int valid = k > 0;
            for (size_t i = 0; valid && i < k; ++i) valid = name_char(e[i]);
            struct VgiVersionWindow w;
            if (valid && !VgiVersionWindowParse(eq + 1, n - (size_t)(eq + 1 - e), &w) &&
                k == want && !memcmp(e, protocol, k)) {
                *out = w;
                return 1;
            }
            /* A readable earlier entry for the same name wins; an unreadable one is skipped. */
        }
        if (!comma) return 0;
        entry = comma + 1;
    }
}
