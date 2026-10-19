#include "vgi_rpc/stream.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

struct ObservedInput {
    struct ArrowIpcInputStream input;
    struct ArrowIpcDecoder decoder;
    struct ArrowBuffer header;
    struct ArrowBuffer metadata;
    int64_t header_size;
    int64_t body_left;
    int eos;
};

static uint32_t le32(const uint8_t *p) {
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

/* Observe only headers. Body bytes go straight to nanoarrow's reader, so this
 * wrapper neither buffers whole streams nor changes dictionary lifetimes. */
static int observe(struct ObservedInput *in, const uint8_t *data, int64_t size,
                   struct ArrowError *error) {
    while (size > 0) {
        if (in->eos) return EINVAL;
        if (in->body_left) {
            int64_t take = size < in->body_left ? size : in->body_left;
            data += take;
            size -= take;
            in->body_left -= take;
            continue;
        }
        int64_t take = in->header_size - in->header.size_bytes;
        if (take > size) take = size;
        NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(&in->header, data, take));
        size -= take;
        data += take;
        if (in->header.size_bytes < in->header_size) continue;
        if (in->header_size == 8) {
            uint32_t length = le32(in->header.data + 4);
            if (le32(in->header.data) != UINT32_MAX || length > INT32_MAX - 8) {
                ArrowErrorSet(error, "Invalid IPC continuation token or message length");
                return EINVAL;
            }
            if (!length) {
                in->eos = 1;
                continue;
            }
            in->header_size = (int64_t)length + 8;
            continue;
        }
        struct ArrowBufferView header = {{in->header.data}, in->header.size_bytes};
        NANOARROW_RETURN_NOT_OK(ArrowIpcDecoderVerifyHeader(&in->decoder, header, error));
        if (in->decoder.body_size_bytes < 0) return EINVAL;
        if (in->decoder.message_type == NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH) {
            ArrowBufferReset(&in->metadata);
            NANOARROW_RETURN_NOT_OK(
                ArrowIpcDecoderGetMessageMetadata(&in->decoder, &in->metadata, error));
        }
        in->body_left = in->decoder.body_size_bytes;
        in->header.size_bytes = 0;
        in->header_size = 8;
    }
    return 0;
}

static int observed_read(struct ArrowIpcInputStream *stream, uint8_t *buf, int64_t size,
                         int64_t *read_out, struct ArrowError *error) {
    struct ObservedInput *in = stream->private_data;
    NANOARROW_RETURN_NOT_OK(in->input.read(&in->input, buf, size, read_out, error));
    return observe(in, buf, *read_out, error);
}

static void observed_release(struct ArrowIpcInputStream *stream) {
    struct ObservedInput *in = stream->private_data;
    if (in->input.release) in->input.release(&in->input);
    ArrowIpcDecoderReset(&in->decoder);
    ArrowBufferReset(&in->header);
    ArrowBufferReset(&in->metadata);
    free(in);
    stream->release = NULL;
}

int VgiIpcReaderInit(struct VgiIpcReader *reader, struct ArrowIpcInputStream *input) {
    if (!reader || reader->stream.release || !input || !input->release) return EINVAL;
    struct ObservedInput *in = calloc(1, sizeof(*in));
    if (!in) return ENOMEM;
    ArrowBufferInit(&in->header);
    ArrowBufferInit(&in->metadata);
    in->header_size = 8;
    int rc = ArrowIpcDecoderInit(&in->decoder);
    if (rc) {
        free(in);
        return rc;
    }
    struct ArrowIpcInputStream observed = {observed_read, observed_release, in};
    rc = ArrowIpcArrayStreamReaderInit(&reader->stream, &observed, NULL);
    if (rc) {
        observed.release(&observed);
        return rc;
    }
    ArrowIpcInputStreamMove(input, &in->input);
    reader->private_data = in;
    return 0;
}

int VgiIpcReaderSchema(struct VgiIpcReader *reader, struct ArrowSchema *out,
                       struct ArrowError *error) {
    if (!reader || !reader->stream.release || !out || out->release) return EINVAL;
    return ArrowArrayStreamGetSchema(&reader->stream, out, error);
}

int VgiIpcReaderNext(struct VgiIpcReader *reader, struct ArrowArray *out,
                     struct ArrowBuffer *metadata, struct ArrowError *error) {
    if (!reader || !reader->stream.release || !out || out->release) return EINVAL;
    struct ObservedInput *in = reader->private_data;
    int rc = ArrowArrayStreamGetNext(&reader->stream, out, error);
    if (!rc && !out->release && !in->eos) {
        ArrowErrorSet(error, "Truncated IPC stream: missing end-of-stream marker");
        rc = EINVAL;
    }
    if (!rc && metadata) {
        ArrowBufferReset(metadata);
        if (out->release)
            rc = ArrowBufferAppend(metadata, in->metadata.data, in->metadata.size_bytes);
    }
    if (rc && out->release) ArrowArrayRelease(out);
    return rc;
}

void VgiIpcReaderReset(struct VgiIpcReader *reader) {
    if (reader->stream.release) ArrowArrayStreamRelease(&reader->stream);
    memset(reader, 0, sizeof(*reader));
}

int VgiIpcWriterInit(struct VgiIpcWriter *writer, const struct ArrowSchema *schema,
                     struct ArrowIpcOutputStream *output, struct ArrowError *error) {
    if (!writer || writer->writer.private_data || !schema || !schema->release || !output ||
        !output->release)
        return EINVAL;
    int rc = ArrowSchemaDeepCopy(schema, &writer->schema);
    if (!rc) rc = ArrowIpcWriterInit(&writer->writer, output);
    if (!rc) rc = ArrowIpcWriterWriteSchema(&writer->writer, schema, error);
    if (rc) VgiIpcWriterReset(writer);
    return rc;
}

struct DictionaryView {
    int64_t id;
    const struct ArrowArrayView *view;
};
static int dictionaries(const struct ArrowArrayView *view, struct ArrowBuffer *out, int64_t *id) {
    if (view->dictionary) {
        struct DictionaryView entry = {(*id)++, view->dictionary};
        NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(out, &entry, sizeof(entry)));
    }
    for (int64_t i = 0; i < view->n_children; ++i)
        NANOARROW_RETURN_NOT_OK(dictionaries(view->children[i], out, id));
    if (view->dictionary) return dictionaries(view->dictionary, out, id);
    return 0;
}

int VgiIpcWriterNext(struct VgiIpcWriter *writer, const struct ArrowArray *array,
                     const char *metadata, struct ArrowError *error) {
    if (!writer || !writer->writer.private_data || writer->finished || !array || !array->release)
        return EINVAL;
    struct ArrowArrayView view;
    ArrowArrayViewInitFromType(&view, NANOARROW_TYPE_UNINITIALIZED);
    struct ArrowBuffer dicts, md;
    ArrowBufferInit(&dicts);
    ArrowBufferInit(&md);
    int rc = ArrowArrayViewInitFromSchema(&view, &writer->schema, error);
    if (!rc) rc = ArrowArrayViewSetArray(&view, array, error);
    if (!rc) rc = ArrowArrayViewValidate(&view, NANOARROW_VALIDATION_LEVEL_FULL, error);
    int64_t id = 0;
    if (!rc) rc = dictionaries(&view, &dicts, &id);
    const struct DictionaryView *entries = (const struct DictionaryView *)dicts.data;
    for (int64_t i = id - 1; !rc && i >= 0; --i)
        rc = ArrowIpcWriterWriteDictionaryBatch(&writer->writer, entries[i].id, 0, entries[i].view,
                                                error);
    if (!rc && metadata) rc = ArrowMetadataBuilderInit(&md, metadata);
    if (!rc) rc = ArrowIpcWriterSetMessageMetadata(&writer->writer, &md, error);
    if (!rc) rc = ArrowIpcWriterWriteArrayView(&writer->writer, &view, error);
    ArrowBufferReset(&md);
    ArrowBufferReset(&dicts);
    ArrowArrayViewReset(&view);
    return rc;
}

int VgiIpcWriterFinish(struct VgiIpcWriter *writer, struct ArrowError *error) {
    if (!writer || !writer->writer.private_data) return EINVAL;
    if (writer->finished) return 0;
    int rc = ArrowIpcWriterWriteArrayView(&writer->writer, NULL, error);
    if (!rc) writer->finished = 1;
    return rc;
}

void VgiIpcWriterReset(struct VgiIpcWriter *writer) {
    if (writer->writer.private_data) ArrowIpcWriterReset(&writer->writer);
    if (writer->schema.release) ArrowSchemaRelease(&writer->schema);
    memset(writer, 0, sizeof(*writer));
}

int VgiIpcWriteSchema(const struct ArrowSchema *schema, struct ArrowBuffer *out,
                      struct ArrowError *error) {
    if (!schema || !schema->release || !out || out->size_bytes) return EINVAL;
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    struct ArrowIpcOutputStream output = {0};
    struct VgiIpcWriter writer = {0};
    int rc = ArrowIpcOutputStreamInitBuffer(&output, &bytes);
    if (!rc) rc = VgiIpcWriterInit(&writer, schema, &output, error);
    if (!rc) rc = VgiIpcWriterFinish(&writer, error);
    VgiIpcWriterReset(&writer);
    if (output.release) output.release(&output);
    if (!rc) {
        ArrowBufferReset(out);
        ArrowBufferMove(&bytes, out);
    }
    ArrowBufferReset(&bytes);
    return rc;
}

int VgiIpcReadSchema(struct ArrowBufferView input, struct ArrowSchema *out,
                     struct ArrowError *error) {
    if (!out || out->release || input.size_bytes < 0 || (!input.data.data && input.size_bytes))
        return EINVAL;
    struct ArrowBuffer bytes;
    ArrowBufferInit(&bytes);
    struct ArrowIpcInputStream source = {0};
    struct VgiIpcReader reader = {0};
    int rc = ArrowBufferAppend(&bytes, input.data.data, input.size_bytes);
    if (!rc) rc = ArrowIpcInputStreamInitBuffer(&source, &bytes);
    if (!rc) rc = VgiIpcReaderInit(&reader, &source);
    if (!rc) rc = VgiIpcReaderSchema(&reader, out, error);
    VgiIpcReaderReset(&reader);
    if (source.release) source.release(&source);
    ArrowBufferReset(&bytes);
    return rc;
}
