#ifndef VGI_RPC_PROTOCOL_VERSIONS_H
#define VGI_RPC_PROTOCOL_VERSIONS_H
/* Protocol version negotiation, v1 (vgi-rpc tools/cross-port/specs/
 * PROTOCOL_VERSIONS_V1.md).
 *
 * A server advertises, per protocol it gates, the inclusive window of
 * MAJOR.MINOR versions it accepts:
 *
 *     VGI-Protocol-Versions: vgi.v3=3.4-3.4, vgi.secret.v1=1.0-1.0
 *
 * on every HTTP response, and as the vgi_rpc.protocol_versions metadata value
 * on reflection results, __transport_options__ replies and
 * protocol_version_unsupported refusals on every transport. Clients send
 * their own window as vgi_rpc.protocol_versions_client (and, on HTTP, the
 * VGI-Client-Protocol-Versions header); servers never refuse because of it. */
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

#define VGI_RPC_PROTOCOL_VERSIONS_HEADER "VGI-Protocol-Versions"
#define VGI_RPC_CLIENT_PROTOCOL_VERSIONS_HEADER "VGI-Client-Protocol-Versions"
#define VGI_RPC_PROTOCOL_VERSIONS_KEY "vgi_rpc.protocol_versions"
#define VGI_RPC_CLIENT_PROTOCOL_VERSIONS_KEY "vgi_rpc.protocol_versions_client"
#define VGI_RPC_PROTOCOL_VERSION_UNSUPPORTED "protocol_version_unsupported"
/* The pre-negotiation refusal kind, still recognised from older servers. */
#define VGI_RPC_PROTOCOL_VERSION_MISMATCH "protocol_version_mismatch"

/* An inclusive window of (major, minor) versions. */
struct VgiVersionWindow {
    uint64_t min[2], max[2];
};
/* MAJOR.MINOR (no leading zeros except "0") of `n` bytes; a MAJOR.MINOR.PATCH
 * is accepted and its patch dropped. */
int VgiVersionMajorMinor(const char *text, size_t n, uint64_t out[2]);
/* "MIN-MAX", or a single MAJOR.MINOR meaning V-V; whitespace around it is
 * ignored. EINVAL when malformed or inverted. */
int VgiVersionWindowParse(const char *text, size_t n, struct VgiVersionWindow *out);
/* "MIN-MAX" with both ends; out needs at most 84 bytes. */
void VgiVersionWindowFormat(const struct VgiVersionWindow *, char *out, size_t size);
/* Whether (major, minor) lies inside the window. */
int VgiVersionWindowContains(const struct VgiVersionWindow *, const uint64_t version[2]);
/* Look `protocol` up in an advertisement ("name=MIN-MAX, ..."). Tolerant:
 * unreadable entries are skipped and a repeated name keeps its first window.
 * Returns 1 and fills *out when found, 0 otherwise. */
int VgiVersionsFind(const char *advertisement, const char *protocol, struct VgiVersionWindow *out);

#ifdef __cplusplus
}
#endif
#endif
