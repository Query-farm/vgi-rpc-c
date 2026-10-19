#ifndef CONFORMANCE_STORAGE_H
#define CONFORMANCE_STORAGE_H
#include <pthread.h>

#include "vgi_rpc/external.h"
#include "vgi_rpc/rpc.h"
struct PublishedEntry;
struct ConformanceStorage {
    const char *base_url;
    struct VgiExternalOptions options;
    int reject_localhost;
    pthread_mutex_t mutex;
    struct PublishedEntry *published;
};
void ConformanceStorageInit(struct ConformanceStorage *);
void ConformanceStorageReset(struct ConformanceStorage *);
int ConformancePublished(struct ConformanceStorage *, struct VgiRpcCall *, struct VgiRpcReply *);
#endif
