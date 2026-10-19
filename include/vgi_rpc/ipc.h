#ifndef VGI_IPC_H
#define VGI_IPC_H
#include "nanoarrow/nanoarrow.h"
#ifdef __cplusplus
extern "C" {
#endif

/* Encode exactly one batch as a complete IPC stream, including dictionaries.
 * Inputs are borrowed for the duration of the call. metadata is NULL or Arrow
 * C Data metadata (ArrowMetadataBuilder). out must be initialized and empty;
 * it remains empty on failure. Release successful output with ArrowBufferReset. */
int VgiIpcWriteBatch(const struct ArrowSchema *schema, const struct ArrowArray *array,
                     const char *metadata, struct ArrowBuffer *out, struct ArrowError *error);

/* Decode exactly one complete, uncompressed IPC stream containing one batch.
 * Input bytes are borrowed. Outputs own their data and must be zero-initialized
 * (metadata must be initialized and empty). On failure no outputs are retained.
 * Trailing bytes, multiple batches, missing EOS and truncated input are errors.
 * Release outputs with ArrowSchemaRelease, ArrowArrayRelease, ArrowBufferReset. */
int VgiIpcReadBatch(struct ArrowBufferView input, struct ArrowSchema *schema,
                    struct ArrowArray *array, struct ArrowBuffer *metadata,
                    struct ArrowError *error);
#ifdef __cplusplus
}
#endif
#endif
