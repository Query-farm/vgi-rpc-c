#ifndef VGI_RPC_H
#define VGI_RPC_H
#include <stdio.h>

#include "vgi_rpc/batch.h"
#include "vgi_rpc/external.h"
#include "vgi_rpc/generated/protocol.h"
#include "vgi_rpc/method.h"
#include "vgi_rpc/stream.h"
#ifdef __cplusplus
extern "C" {
#endif

enum VgiTransportKind {
    VGI_TRANSPORT_PIPE,
    VGI_TRANSPORT_HTTP,
    VGI_TRANSPORT_UNIX,
    VGI_TRANSPORT_TCP
};
const char *VgiTransportKindName(enum VgiTransportKind);
struct VgiRpcServer;
struct VgiRpcLogEntry;
struct VgiSessionBinding;
struct VgiRpcCall {
    struct VgiRpcServer *server;
    enum VgiTransportKind transport_kind;
    const struct VgiMethod *method;
    struct VgiBatch request;
    struct ArrowBuffer metadata;
    char request_id[129];
    const char *protocol;
    const char *principal;
    const char *auth_domain;
    char *auth_claims_json; /* Call-owned JSON object; released by the runtime. */
    double auth_time;
    int has_auth_time;
    const unsigned char *signing_key;
    /* Transport-owned key, present on authenticating transports only. */
    const unsigned char *sealing_key;
    void *application_state;
    void (*release_application_state)(void *);
    const char *error_type;
    const char *error_code;
    const char *error_kind;
    char *error_storage;
    char *error_details;
    struct ArrowError error;
    struct VgiRpcLogEntry *logs;
    struct VgiRpcLogEntry *logs_tail;
    /* Bound by the HTTP runtime for this request only. */
    struct VgiSessionBinding *session_binding;
};

/* A stream callback returns one batch, or sets finished to signal EOS.
 * It may return a final batch together with finished.
 * The input is borrowed; the output is moved to the framework. */
struct VgiRpcStream {
    struct VgiBatch header;
    struct ArrowSchema output_schema;
    /* Producers consume empty ticks; exchanges consume client record batches. */
    int is_producer;
    int (*next)(void *state, struct VgiRpcCall *call, const struct VgiBatch *input,
                const char *metadata, struct VgiBatch *output, struct ArrowBuffer *output_metadata,
                int *finished);
    void (*on_cancel)(void *state, struct VgiRpcCall *call);
    void (*release)(void *state);
    void *state;
    /* Serialize only the advancing state. With a protocol resume callback this
     * enables stateless HTTP turns. Called after next(), including init. */
    int (*serialize)(void *state, struct ArrowBuffer *, struct ArrowError *);
};
struct VgiRpcReply {
    struct VgiBatch value;
    struct ArrowBuffer metadata;
    struct VgiRpcStream stream;
};
typedef int (*VgiRpcHandler)(void *state, struct VgiRpcCall *call, struct VgiRpcReply *reply);
struct VgiRpcProtocol {
    const char *name;
    const char *version;
    const char *hash;
    const struct VgiMethod *methods;
    size_t method_count;
    VgiRpcHandler handler;
    void *state;
    /* Reconstruct a stream from the original call and authenticated state.
     * Must not repeat init side effects or emit a header/init logs. The resolved
     * output schema from init is supplied; callbacks must preserve it. */
    int (*resume)(void *, struct VgiRpcCall *, struct ArrowBufferView, const struct ArrowSchema *,
                  struct VgiRpcReply *);
};

struct VgiRpcServer *VgiRpcServerNew(void);
/* Configure before serving. The options are copied; callback state and strings
 * must outlive the server. NULL disables external-location resolution. */
int VgiRpcServerSetExternal(struct VgiRpcServer *, const struct VgiExternalOptions *);
/* Managed protocol state is released when the server is freed. */
int VgiRpcServerAddManagedProtocol(struct VgiRpcServer *, const struct VgiRpcProtocol *,
                                   void (*)(void *), struct ArrowError *);
void VgiRpcServerFree(struct VgiRpcServer *server);
/* Configure before serving; enabled by default on every transport. */
void VgiRpcServerSetStartHook(struct VgiRpcServer *,
                              int (*hook)(void *, enum VgiTransportKind, struct ArrowError *),
                              void *state);
void VgiRpcServerSetTracebacks(struct VgiRpcServer *server, int enabled);
/* Registrations are copied except methods/state, which must outlive server. */
int VgiRpcServerAddProtocol(struct VgiRpcServer *server, const struct VgiRpcProtocol *protocol,
                            struct ArrowError *error);
int VgiRpcServerServe(struct VgiRpcServer *server, FILE *input, FILE *output,
                      struct ArrowError *error);
int VgiRpcServerServeSocket(struct VgiRpcServer *, const char *unix_path, const char *tcp_address,
                            double idle_seconds, struct ArrowError *);
int VgiRpcServerServeOne(struct VgiRpcServer *server, FILE *input, FILE *output,
                         struct ArrowError *error);
int VgiRpcLogExtra(struct VgiRpcCall *, const char *level, const char *message,
                   const char *extra_json);
int VgiRpcLog(struct VgiRpcCall *call, const char *level, const char *message);
/* Session state remains owned by the runtime until close, TTL expiry, or shutdown.
 * Open takes ownership only on success. ttl_seconds=0 uses the configured default. */
int VgiRpcSessionOpen(struct VgiRpcCall *, void *state, void (*release)(void *),
                      unsigned ttl_seconds);
void *VgiRpcSessionGet(struct VgiRpcCall *);
int VgiRpcSessionClose(struct VgiRpcCall *);
int VgiRpcSessionsSetDraining(struct VgiRpcCall *, int draining);
int VgiRpcFail(struct VgiRpcCall *call, const char *type, const char *code, const char *kind,
               const char *message);
/* Copy a JSON array of qualified, unique typed details. Oversized arrays are
 * omitted in their entirety, as required by the 4 KiB wire limit. */
int VgiRpcSetErrorDetails(struct VgiRpcCall *, const char *json);
int VgiRpcReplyPayload(struct VgiRpcCall *call, struct VgiRpcReply *reply,
                       const struct VgiBatch *payload);
int VgiMetadataSet(struct ArrowBuffer *metadata, const char *key, const char *value);
struct ArrowStringView VgiMetadataGet(const char *metadata, const char *key);
#ifdef __cplusplus
}
#endif
#endif
