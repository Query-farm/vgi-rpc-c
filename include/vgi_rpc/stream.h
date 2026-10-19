#ifndef VGI_STREAM_H
#define VGI_STREAM_H
#include "nanoarrow/nanoarrow_ipc.h"
#ifdef __cplusplus
extern "C" {
#endif

/* Incremental IPC with per-record-batch metadata. Init moves the input/output
 * stream on success. Reset releases it; ownership of any returned array stays
 * with the caller. All structs and outputs must be zero-initialized. */
struct VgiIpcReader {
    struct ArrowArrayStream stream;
    void *private_data;
};
int VgiIpcReaderInit(struct VgiIpcReader *reader, struct ArrowIpcInputStream *input);
int VgiIpcReaderSchema(struct VgiIpcReader *reader, struct ArrowSchema *out,
                       struct ArrowError *error);
int VgiIpcReaderNext(struct VgiIpcReader *reader, struct ArrowArray *out,
                     struct ArrowBuffer *metadata, struct ArrowError *error);
void VgiIpcReaderReset(struct VgiIpcReader *reader);

struct VgiIpcWriter {
    struct ArrowIpcWriter writer;
    struct ArrowSchema schema;
    int finished;
};
int VgiIpcWriterInit(struct VgiIpcWriter *writer, const struct ArrowSchema *schema,
                     struct ArrowIpcOutputStream *output, struct ArrowError *error);
int VgiIpcWriterNext(struct VgiIpcWriter *writer, const struct ArrowArray *array,
                     const char *metadata, struct ArrowError *error);
int VgiIpcWriterFinish(struct VgiIpcWriter *writer, struct ArrowError *error);
void VgiIpcWriterReset(struct VgiIpcWriter *writer);

/* Schema IPC carries no batches and is used for function arguments/results. */
int VgiIpcWriteSchema(const struct ArrowSchema *schema, struct ArrowBuffer *out,
                      struct ArrowError *error);
int VgiIpcReadSchema(struct ArrowBufferView input, struct ArrowSchema *out,
                     struct ArrowError *error);
#ifdef __cplusplus
}
#endif
#endif
