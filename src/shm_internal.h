#ifndef VGI_SHM_INTERNAL_H
#define VGI_SHM_INTERNAL_H
#include "vgi_rpc/rpc.h"
#include "vgi_rpc/shm.h"
int VgiShmAdvertise(struct VgiShm *, struct ArrowBuffer *);
int VgiShmFromMetadata(const char *, struct VgiShm **, struct ArrowError *);
int VgiShmResolve(struct VgiShm *, struct VgiBatch *, struct ArrowBuffer *, struct ArrowError *);
int VgiShmWrite(struct VgiShm *, struct VgiIpcWriter *, const struct ArrowArray *, const char *,
                struct ArrowError *);
void VgiShmReset(struct VgiShm *);
#endif
