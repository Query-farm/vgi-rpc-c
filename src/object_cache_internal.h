#ifndef VGI_OBJECT_CACHE_INTERNAL_H
#define VGI_OBJECT_CACHE_INTERNAL_H
#include <stdint.h>

#include "nanoarrow/nanoarrow.h"
/* Reference-counted, immutable object bytes shared by the cache and its
 * readers, so an eviction never invalidates a value in use. */
struct VgiObjectBytes;
struct VgiObjectBytes *VgiObjectBytesNew(int64_t size);
uint8_t *VgiObjectBytesData(struct VgiObjectBytes *);
int64_t VgiObjectBytesSize(const struct VgiObjectBytes *);
void VgiObjectBytesRelease(struct VgiObjectBytes *);
/* Produce the verified value on a miss. Its return code (and error message)
 * is shared with every caller that joined the flight. */
typedef int (*VgiObjectFetch)(void *state, struct VgiObjectBytes **out, struct ArrowError *);
/* Look `id` up; on a miss exactly one caller runs fetch and the others wait
 * for its result. *out is a new reference on success; *hit says whether the
 * value came from the cache. */
int VgiObjectCacheGetOrFetch(const char *id, VgiObjectFetch, void *state,
                             struct VgiObjectBytes **out, int *hit, struct ArrowError *);
#endif
