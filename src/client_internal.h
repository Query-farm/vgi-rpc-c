#ifndef VGI_CLIENT_INTERNAL_H
#define VGI_CLIENT_INTERNAL_H
#include <sys/types.h>

#include "vgi_rpc/client.h"
struct VgiClient {
    struct VgiClientOptions options;
    char *protocol, *version;
    FILE *input, *output;
    pid_t child;
    struct VgiRpcServer *resolver;
    struct VgiClientError error;
    struct ArrowError arrow_error;
    int streaming, exchange, eos;
    struct VgiIpcReader reader;
    struct VgiIpcWriter writer;
    struct ArrowSchema output_schema;
    struct VgiHttpClient *http;
};
int VgiClientSetError(struct VgiClient *, const char *, const char *, const char *);
void VgiClientClearError(struct VgiClient *);
int VgiClientReadMetadata(struct VgiClient *, const char *);
int VgiClientResolve(struct VgiClient *, struct VgiAnnotatedBatch *);
int VgiClientRequestMetadata(struct VgiClient *, const char *, const char *, struct ArrowBuffer *);
int VgiClientInput(struct VgiClient *, struct ArrowIpcInputStream *);
int VgiClientOutput(struct VgiClient *, struct ArrowIpcOutputStream *);
int VgiHttpClientInit(struct VgiClient *);
void VgiHttpClientReset(struct VgiClient *);
int VgiHttpClientUnary(struct VgiClient *, const char *, const struct VgiAnnotatedBatch *,
                       struct VgiAnnotatedBatch *);
int VgiHttpClientOpen(struct VgiClient *, const char *, const struct VgiAnnotatedBatch *, int, int,
                      struct VgiAnnotatedBatch *);
int VgiHttpClientStep(struct VgiClient *, const struct VgiAnnotatedBatch *,
                      struct VgiAnnotatedBatch *, int *);
int VgiHttpClientClose(struct VgiClient *, int);
#endif
