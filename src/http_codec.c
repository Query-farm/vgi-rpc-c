#include "http_codec.h"

#include <errno.h>
#include <limits.h>
#include <string.h>
#include <zlib.h>
#include <zstd.h>

int VgiHttpDecode(const char *codec, struct ArrowBufferView input, int64_t limit,
                  struct ArrowBuffer *out) {
    if (input.size_bytes < 0 || limit < 0) return EINVAL;
    if (!codec || !*codec || !strcmp(codec, "identity"))
        return input.size_bytes > limit ? EFBIG
                                        : ArrowBufferAppend(out, input.data.data, input.size_bytes);
    unsigned char chunk[65536];
    int rc = 0;
    if (!strcmp(codec, "zstd")) {
        ZSTD_DStream *stream = ZSTD_createDStream();
        if (!stream) return ENOMEM;
        size_t status = ZSTD_initDStream(stream);
        ZSTD_inBuffer in = {input.data.data, (size_t)input.size_bytes, 0};
        while (!rc && !ZSTD_isError(status)) {
            ZSTD_outBuffer result = {chunk, sizeof(chunk), 0};
            size_t before = in.pos;
            status = ZSTD_decompressStream(stream, &result, &in);
            if (ZSTD_isError(status)) {
                rc = EINVAL;
                break;
            }
            if ((int64_t)result.pos > limit - out->size_bytes) {
                rc = EFBIG;
                break;
            }
            rc = ArrowBufferAppend(out, chunk, (int64_t)result.pos);
            if (!status) {
                if (in.pos != in.size) rc = EINVAL;
                break;
            }
            if (in.pos == before && !result.pos) {
                rc = EINVAL;
                break;
            }
        }
        if (ZSTD_isError(status)) rc = EINVAL;
        ZSTD_freeDStream(stream);
    } else if (!strcmp(codec, "gzip")) {
        z_stream stream = {0};
        if (inflateInit2(&stream, MAX_WBITS + 16) != Z_OK) return ENOMEM;
        int status = Z_OK;
        int64_t pos = 0;
        while (!rc && status != Z_STREAM_END) {
            if (!stream.avail_in && pos < input.size_bytes) {
                uInt n =
                    (uInt)(input.size_bytes - pos > UINT_MAX ? UINT_MAX : input.size_bytes - pos);
                stream.next_in = (Bytef *)input.data.as_uint8 + pos;
                stream.avail_in = n;
                pos += n;
            }
            stream.next_out = chunk;
            stream.avail_out = sizeof(chunk);
            status = inflate(&stream, Z_NO_FLUSH);
            size_t n = sizeof(chunk) - stream.avail_out;
            if (status != Z_OK && status != Z_STREAM_END) {
                rc = EINVAL;
                break;
            }
            if ((int64_t)n > limit - out->size_bytes) {
                rc = EFBIG;
                break;
            }
            rc = ArrowBufferAppend(out, chunk, (int64_t)n);
            if (status == Z_STREAM_END && (stream.avail_in || pos != input.size_bytes)) rc = EINVAL;
        }
        inflateEnd(&stream);
    } else
        rc = ENOTSUP;
    if (rc) ArrowBufferReset(out);
    return rc;
}
int VgiHttpEncode(const char *codec, struct ArrowBufferView input, struct ArrowBuffer *out) {
    if (!codec || !*codec || !strcmp(codec, "identity"))
        return ArrowBufferAppend(out, input.data.data, input.size_bytes);
    int rc = 0;
    if (!strcmp(codec, "zstd")) {
        size_t bound = ZSTD_compressBound((size_t)input.size_bytes);
        if (bound > INT64_MAX) return EOVERFLOW;
        rc = ArrowBufferResize(out, (int64_t)bound, 0);
        if (!rc) {
            size_t n =
                ZSTD_compress(out->data, bound, input.data.data, (size_t)input.size_bytes, 3);
            if (ZSTD_isError(n))
                rc = EIO;
            else
                out->size_bytes = (int64_t)n;
        }
    } else if (!strcmp(codec, "gzip")) {
        z_stream stream = {0};
        if (deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + 16, 8,
                         Z_DEFAULT_STRATEGY) != Z_OK)
            return ENOMEM;
        int status = Z_OK;
        int64_t pos = 0;
        unsigned char chunk[65536];
        while (!rc && status != Z_STREAM_END) {
            if (!stream.avail_in && pos < input.size_bytes) {
                uInt n =
                    (uInt)(input.size_bytes - pos > UINT_MAX ? UINT_MAX : input.size_bytes - pos);
                stream.next_in = (Bytef *)input.data.as_uint8 + pos;
                stream.avail_in = n;
                pos += n;
            }
            stream.next_out = chunk;
            stream.avail_out = sizeof(chunk);
            status = deflate(&stream, pos == input.size_bytes ? Z_FINISH : Z_NO_FLUSH);
            if (status != Z_OK && status != Z_STREAM_END) {
                rc = EIO;
                break;
            }
            rc = ArrowBufferAppend(out, chunk, (int64_t)(sizeof(chunk) - stream.avail_out));
        }
        deflateEnd(&stream);
    } else
        rc = ENOTSUP;
    if (rc) ArrowBufferReset(out);
    return rc;
}
