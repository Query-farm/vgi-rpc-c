#ifndef VGI_RPC_CLIENT_H
#define VGI_RPC_CLIENT_H
#include "vgi_rpc/rpc.h"
#include "vgi_rpc/shm.h"
#ifdef __cplusplus
extern "C" {
#endif

struct VgiClient;
struct VgiClientHeader {
    const char *name, *value;
};
/* Upload URL protocol this client speaks (VGI-Upload-URL-Support: 2). */
#define VGI_UPLOAD_URL_PROTOCOL 2
struct VgiClientCapabilities {
    /* upload_url_support: 0 absent, 1 the retired v1 ("true"), 2 sized/hashed v2. */
    int sticky_enabled, upload_url_support, externalization_enabled;
    int64_t sticky_default_ttl, max_request_bytes, max_response_bytes,
        max_externalized_response_bytes, max_upload_bytes; /* -1 if absent */
    const char *supported_encodings, *sticky_echo_headers; /* comma-separated */
};
enum VgiClientTransport { VGI_CLIENT_STDIO, VGI_CLIENT_UNIX, VGI_CLIENT_TCP, VGI_CLIENT_HTTP };
struct VgiClientOptions {
    enum VgiClientTransport transport;
    const char *target;      /* socket path, host:port, or HTTP base URL */
    const char *const *argv; /* NULL-terminated subprocess command for stdio */
    const char *protocol;
    const char *protocol_version;
    const struct VgiExternalOptions *external;
    /* Strings are borrowed for the callback only; extra_json is an object. */
    void (*on_log)(void *, const char *level, const char *message, const char *extra_json);
    void *log_state;
    unsigned timeout_seconds;
    const struct VgiClientHeader *headers;
    size_t header_count;
    int compression_level;               /* zstd; zero disables request compression */
    int64_t accepted_max_response_bytes; /* zero uses the server default */
    const char *tls_ca_file;             /* NULL uses system trust; verification stays enabled */
    const char *tls_certificate_file;
    const char *tls_key_file;
    int64_t shared_memory_size;      /* Zero disables; stdio and local IPC only. */
    int64_t shared_memory_threshold; /* 128 KiB default; zero forces eligible batches. */
    struct VgiTelemetry *telemetry;  /* Borrowed; outlives the client. */
};
struct VgiClientError {
    /* Owned by the client; valid until its next operation or close. */
    const char *type, *message, *traceback, *code, *kind, *details_json;
};
struct VgiAnnotatedBatch {
    struct VgiBatch batch;
    struct ArrowBuffer metadata;
};
void VgiAnnotatedBatchReset(struct VgiAnnotatedBatch *);
void VgiClientOptionsInit(struct VgiClientOptions *);
int VgiClientConnect(const struct VgiClientOptions *, struct VgiClient **, struct ArrowError *);
/* Local cleanup: closes descriptors and terminates any owned child. Does not
 * wait for a peer to finish an abandoned stream. */
void VgiClientFree(struct VgiClient *);
const struct VgiClientError *VgiClientLastError(const struct VgiClient *);
void VgiClientSharedMemoryStats(const struct VgiClient *, struct VgiShmStats *);
/* One call or stream at a time. Inputs are borrowed. Outputs must be zeroed and
 * are caller-owned. Request metadata is preserved; missing routing metadata is
 * supplied from method/options. Errors are available through LastError(). */
int VgiClientUnary(struct VgiClient *, const char *method, const struct VgiAnnotatedBatch *,
                   struct VgiAnnotatedBatch *);
/* Reflection v1: decoded ProtocolList and ServiceDescription payloads. Selects
 * the first hosted application protocol. The caller owns both output batches. */
int VgiClientDescribe(struct VgiClient *, struct VgiAnnotatedBatch *listing,
                      struct VgiAnnotatedBatch *description);
int VgiClientStreamOpen(struct VgiClient *, const char *method, const struct VgiAnnotatedBatch *,
                        int is_exchange, int has_header, struct VgiAnnotatedBatch *header);
int VgiClientStreamTick(struct VgiClient *, const char *metadata, struct VgiAnnotatedBatch *,
                        int *done);
int VgiClientStreamExchange(struct VgiClient *, const struct VgiAnnotatedBatch *,
                            struct VgiAnnotatedBatch *, int *done);
int VgiClientStreamClose(struct VgiClient *, int cancel);
/* Borrowed latest cursor; NULL on byte-stream transports or terminal streams. */
const char *VgiClientStreamToken(const struct VgiClient *);
/* HTTP capabilities are read at connection time. */
const struct VgiClientCapabilities *VgiClientCapabilities(const struct VgiClient *);
/* One upload: exact body length (>= 1) and its lowercase hex SHA-256. */
struct VgiUploadRequest {
    int64_t size;
    const char *sha256;
};
/* Request one URL pair per upload (1-100) from a v2 server; a v1 server is
 * refused ("upload protocol is too old"). PUT exactly `size` bytes to each
 * upload_url with every entry of its `headers`, and name the payload's sha256
 * on the pointer. */
int VgiClientUploadUrls(struct VgiClient *, const struct VgiUploadRequest *uploads, size_t count,
                        struct VgiExternalUploadUrl **out, size_t *out_count);
void VgiClientUploadUrlsFree(struct VgiExternalUploadUrl *, size_t);
/* Session scopes nest. End pops and best-effort deletes unless detached. */
int VgiClientSessionBegin(struct VgiClient *, const char *token);
const char *VgiClientSessionToken(const struct VgiClient *);
const char *VgiClientSessionDetach(struct VgiClient *);
size_t VgiClientSessionEchoHeaders(const struct VgiClient *, const struct VgiClientHeader **);
int VgiClientSessionEnd(struct VgiClient *);
#ifdef __cplusplus
}
#endif
#endif
