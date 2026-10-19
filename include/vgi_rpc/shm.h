#ifndef VGI_RPC_SHM_H
#define VGI_RPC_SHM_H
#include "vgi_rpc/batch.h"
#ifdef __cplusplus
extern "C" {
#endif
/* POSIX shared-memory side channel. One lockstep connection per segment.
 * Resolved batches own their buffers and remain valid after reuse or close. */
struct VgiShm;
struct VgiShmStats {
    uint64_t batches_written, batches_read, bytes_written, bytes_read, inline_fallbacks;
};
int VgiShmCreate(int64_t size, struct VgiShm **, struct ArrowError *);
int VgiShmAttach(const char *name, int64_t expected_size, struct VgiShm **, struct ArrowError *);
void VgiShmFree(struct VgiShm *); /* Only the creator unlinks the segment. */
const char *VgiShmName(const struct VgiShm *);
int64_t VgiShmSize(const struct VgiShm *);
void VgiShmGetStats(const struct VgiShm *, struct VgiShmStats *);
/* Threshold defaults to 128 KiB; zero routes every nonempty batch through SHM. */
void VgiShmSetThreshold(struct VgiShm *, int64_t bytes);
#ifdef __cplusplus
}
#endif
#endif
