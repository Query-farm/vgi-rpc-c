#ifndef VGI_CLIENT_INTERNAL_H
#define VGI_CLIENT_INTERNAL_H
#include <stdint.h>

#include "vgi_rpc/client.h"
struct VgiClient {
    struct VgiClientOptions options;
    char *protocol, *version;
    int input_fd, output_fd;
    intptr_t socket_handle;
    intptr_t child;
    void *process_handle;
    int transport_is_socket;
    struct VgiExternalOptions external;
    int external_enabled;
    struct VgiClientError error;
    struct ArrowError arrow_error;
    int streaming, exchange, eos;
    int broken;
    struct VgiIpcReader reader;
    struct VgiIpcWriter writer;
    struct ArrowSchema output_schema;
    struct VgiHttpClient *http;
    struct VgiShm *shm;
    struct VgiTraceSpan *trace;
    /* PROTOCOL_VERSIONS_V1 §6, per endpoint (this connection). declared is the
     * options' protocol_version; window the client's own range; advertised the
     * last VGI-Protocol-Versions seen (seen != 0), legacy a version a pre-spec
     * refusal named. version (above) is what the next call sends. */
    char *declared, *client_versions, *advertised;
    struct VgiVersionWindow window;
    uint64_t patch;
    int versioned, seen, legacy_set, probed;
    uint64_t legacy[2];
};
int VgiClientSetError(struct VgiClient *, const char *, const char *, const char *);
/* Negotiation: learn from an advertisement (NULL is ignored), and from a
 * refusal in c->error. */
void VgiClientObserveVersions(struct VgiClient *, const char *advertisement);
int VgiClientReusable(struct VgiClient *);
void VgiClientClearError(struct VgiClient *);
int VgiClientReadMetadata(struct VgiClient *, const char *);
int VgiClientResolve(struct VgiClient *, struct VgiAnnotatedBatch *);
int VgiClientRequestMetadata(struct VgiClient *, const char *, const char *, struct ArrowBuffer *);
int VgiClientInput(struct VgiClient *, struct ArrowIpcInputStream *);
int VgiClientOutput(struct VgiClient *, struct ArrowIpcOutputStream *);
int VgiHttpClientInit(struct VgiClient *);
const char *VgiHttpClientDefaultHeader(struct VgiClient *, const char *);
void VgiHttpClientReset(struct VgiClient *);
int VgiHttpClientUnary(struct VgiClient *, const char *, const struct VgiAnnotatedBatch *,
                       struct VgiAnnotatedBatch *);
int VgiHttpClientOpen(struct VgiClient *, const char *, const struct VgiAnnotatedBatch *, int, int,
                      struct VgiAnnotatedBatch *);
int VgiHttpClientStep(struct VgiClient *, const struct VgiAnnotatedBatch *,
                      struct VgiAnnotatedBatch *, int *);
int VgiHttpClientClose(struct VgiClient *, int);
#endif
