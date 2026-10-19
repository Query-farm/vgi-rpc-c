#ifndef VGI_RPC_CLIENT_H
#define VGI_RPC_CLIENT_H
#include "vgi_rpc/rpc.h"
#ifdef __cplusplus
extern "C" {
#endif

struct VgiClient;
struct VgiClientHeader {
    const char *name, *value;
};
struct VgiClientCapabilities {
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
int VgiClientUploadUrls(struct VgiClient *, int count, struct VgiExternalUploadUrl **out,
                        size_t *out_count);
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
