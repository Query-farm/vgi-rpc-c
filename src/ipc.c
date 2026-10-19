#include "vgi_rpc/ipc.h"

#include <errno.h>
#include <string.h>

#include "nanoarrow/nanoarrow_ipc.h"

struct BorrowedBatch {
    const struct ArrowSchema *schema;
    const struct ArrowArray *array;
    int sent;
};

static void borrowed_release(struct ArrowArray *array) { array->release = NULL; }
static int borrowed_schema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
    struct BorrowedBatch *batch = stream->private_data;
    return ArrowSchemaDeepCopy(batch->schema, out);
}
static int borrowed_next(struct ArrowArrayStream *stream, struct ArrowArray *out) {
    struct BorrowedBatch *batch = stream->private_data;
    if (batch->sent) {
        out->release = NULL;
    } else {
        *out = *batch->array;
        out->release = borrowed_release;
        batch->sent = 1;
    }
    return NANOARROW_OK;
}
static const char *borrowed_error(struct ArrowArrayStream *stream) {
    (void)stream;
    return NULL;
}
static void borrowed_stream_release(struct ArrowArrayStream *stream) { stream->release = NULL; }

int VgiIpcWriteBatch(const struct ArrowSchema *schema, const struct ArrowArray *array,
                     const char *metadata, struct ArrowBuffer *out, struct ArrowError *error) {
    if (!schema || !schema->release || !array || !array->release || !out || out->size_bytes)
        return EINVAL;
    struct ArrowArrayView view;
    ArrowArrayViewInitFromType(&view, NANOARROW_TYPE_UNINITIALIZED);
    int result = ArrowArrayViewInitFromSchema(&view, schema, error);
    if (!result) result = ArrowArrayViewSetArray(&view, array, error);
    if (!result) result = ArrowArrayViewValidate(&view, NANOARROW_VALIDATION_LEVEL_FULL, error);
    ArrowArrayViewReset(&view);
    if (result) return result;

    struct ArrowBuffer bytes;
    struct ArrowBuffer custom_metadata;
    ArrowBufferInit(&bytes);
    ArrowBufferInit(&custom_metadata);
    struct ArrowIpcOutputStream output = {0};
    struct ArrowIpcWriter writer = {0};
    struct BorrowedBatch batch = {schema, array, 0};
    struct ArrowArrayStream stream = {borrowed_schema, borrowed_next, borrowed_error,
                                      borrowed_stream_release, &batch};
    result = ArrowIpcOutputStreamInitBuffer(&output, &bytes);
    if (!result) result = ArrowIpcWriterInit(&writer, &output);
    if (!result && metadata) {
        result = ArrowMetadataBuilderInit(&custom_metadata, metadata);
        if (!result) result = ArrowIpcWriterSetMessageMetadata(&writer, &custom_metadata, error);
    }
    if (!result) result = ArrowIpcWriterWriteArrayStream(&writer, &stream, error);
    if (writer.private_data) ArrowIpcWriterReset(&writer);
    if (output.release) output.release(&output);
    ArrowBufferReset(&custom_metadata);
    if (!result) {
        ArrowBufferReset(out);
        ArrowBufferMove(&bytes, out);
    }
    ArrowBufferReset(&bytes);
    return result;
}

static uint32_t read_u32(const uint8_t *p) {
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

/* Inspect framing independently of the reader: stream readers also accept EOF
 * as EOS and stop before trailing bytes. VGI payloads require an exact stream. */
static int inspect(struct ArrowBufferView input, struct ArrowBuffer *metadata,
                   struct ArrowError *error) {
    struct ArrowIpcDecoder decoder = {0};
    int result = ArrowIpcDecoderInit(&decoder);
    if (result) return result;
    int64_t offset = 0;
    int batches = 0;
    int messages = 0;
    int eos = 0;
    while (offset < input.size_bytes) {
        if (input.size_bytes - offset < 8) {
            result = EINVAL;
            break;
        }
        const uint8_t *start = input.data.as_uint8 + offset;
        if (read_u32(start) != UINT32_MAX) {
            result = EINVAL;
            break;
        }
        uint32_t header_length = read_u32(start + 4);
        if (header_length == 0) {
            offset += 8;
            eos = 1;
            break;
        }
        if (header_length > INT32_MAX - 8 || header_length > input.size_bytes - offset - 8) {
            result = EINVAL;
            break;
        }
        struct ArrowBufferView header = {{start}, (int64_t)header_length + 8};
        result = ArrowIpcDecoderVerifyHeader(&decoder, header, error);
        if (result) break;
        if (decoder.body_size_bytes < 0 ||
            decoder.body_size_bytes > input.size_bytes - offset - header.size_bytes) {
            result = EINVAL;
            break;
        }
        if (messages == 0 && decoder.message_type != NANOARROW_IPC_MESSAGE_TYPE_SCHEMA) {
            result = EINVAL;
            break;
        }
        if (messages > 0 && decoder.message_type == NANOARROW_IPC_MESSAGE_TYPE_SCHEMA) {
            result = EINVAL;
            break;
        }
        if (decoder.message_type == NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH) {
            if (++batches > 1) {
                result = EINVAL;
                break;
            }
            result = ArrowIpcDecoderGetMessageMetadata(&decoder, metadata, error);
            if (result) break;
        } else if (decoder.message_type != NANOARROW_IPC_MESSAGE_TYPE_SCHEMA &&
                   decoder.message_type != NANOARROW_IPC_MESSAGE_TYPE_DICTIONARY_BATCH) {
            result = EINVAL;
            break;
        }
        offset += header.size_bytes + decoder.body_size_bytes;
        ++messages;
    }
    if (!result && (!eos || offset != input.size_bytes || batches != 1)) result = EINVAL;
    ArrowIpcDecoderReset(&decoder);
    if (result)
        ArrowErrorSet(error, "Expected one complete IPC stream with exactly one record batch");
    return result;
}

int VgiIpcReadBatch(struct ArrowBufferView input, struct ArrowSchema *schema,
                    struct ArrowArray *array, struct ArrowBuffer *metadata,
                    struct ArrowError *error) {
    if (!schema || schema->release || !array || array->release || !metadata ||
        metadata->size_bytes || input.size_bytes < 0 || (!input.data.data && input.size_bytes))
        return EINVAL;
    struct ArrowBuffer custom_metadata;
    struct ArrowBuffer bytes;
    ArrowBufferInit(&custom_metadata);
    ArrowBufferInit(&bytes);
    struct ArrowIpcInputStream input_stream = {0};
    struct ArrowArrayStream stream = {0};
    struct ArrowArray extra = {0};
    struct ArrowArrayView view;
    ArrowArrayViewInitFromType(&view, NANOARROW_TYPE_UNINITIALIZED);
    /* FlatBuffers headers must be aligned even when the caller's bytes are not. */
    int result = ArrowBufferAppend(&bytes, input.data.data, input.size_bytes);
    if (!result)
        result = inspect((struct ArrowBufferView){{bytes.data}, bytes.size_bytes}, &custom_metadata,
                         error);
    if (!result) result = ArrowIpcInputStreamInitBuffer(&input_stream, &bytes);
    if (!result) result = ArrowIpcArrayStreamReaderInit(&stream, &input_stream, NULL);
    if (!result) result = ArrowArrayStreamGetSchema(&stream, schema, error);
    if (!result) result = ArrowArrayStreamGetNext(&stream, array, error);
    if (!result && !array->release) result = EINVAL;
    if (!result) result = ArrowArrayStreamGetNext(&stream, &extra, error);
    if (!result && extra.release) result = EINVAL;
    if (!result) result = ArrowArrayViewInitFromSchema(&view, schema, error);
    if (!result) result = ArrowArrayViewSetArray(&view, array, error);
    if (!result) result = ArrowArrayViewValidate(&view, NANOARROW_VALIDATION_LEVEL_FULL, error);
    ArrowArrayViewReset(&view);
    if (extra.release) ArrowArrayRelease(&extra);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (input_stream.release) input_stream.release(&input_stream);
    ArrowBufferReset(&bytes);
    if (result) {
        if (schema->release) ArrowSchemaRelease(schema);
        if (array->release) ArrowArrayRelease(array);
    } else {
        ArrowBufferReset(metadata);
        ArrowBufferMove(&custom_metadata, metadata);
    }
    ArrowBufferReset(&custom_metadata);
    return result;
}
