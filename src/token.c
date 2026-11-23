#include "token.h"

#include <errno.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>

#include "http_codec.h"

int VgiTokenAppendU64(struct ArrowBuffer *out, uint64_t n) {
    uint8_t bytes[8];
    for (int i = 0; i < 8; ++i) bytes[i] = (uint8_t)(n >> (i * 8));
    return ArrowBufferAppend(out, bytes, sizeof(bytes));
}
uint64_t VgiTokenReadU64(const uint8_t *bytes) {
    uint64_t n = 0;
    for (int i = 0; i < 8; ++i) n |= (uint64_t)bytes[i] << (i * 8);
    return n;
}
static int aad_init(const struct VgiTokenContext *c, struct ArrowBuffer *aad) {
    const char *parts[] = {"vgi-rpc-c.token.v1", c->purpose, c->protocol, c->method, c->domain,
                           c->principal};
    int rc = 0;
    for (size_t i = 0; !rc && i < sizeof(parts) / sizeof(parts[0]); ++i) {
        const char *s = parts[i] ? parts[i] : "";
        rc = ArrowBufferAppend(aad, s, (int64_t)strlen(s) + 1);
    }
    return rc;
}
int VgiTokenCodecEncode(struct ArrowBufferView input, struct ArrowBuffer *out) {
    if (!out || input.size_bytes < 0) return EINVAL;
    struct ArrowBuffer compressed;
    ArrowBufferInit(&compressed);
    int rc = input.size_bytes ? VgiHttpEncode("zstd", input, &compressed) : 0;
    uint8_t codec = !rc && compressed.size_bytes && compressed.size_bytes < input.size_bytes
                        ? VGI_TOKEN_CODEC_ZSTD
                        : VGI_TOKEN_CODEC_RAW;
    /* A compressor failure only costs the saving; raw is always valid. */
    rc = ArrowBufferAppend(out, &codec, 1);
    if (!rc)
        rc = ArrowBufferAppend(out, codec ? compressed.data : input.data.as_uint8,
                               codec ? compressed.size_bytes : input.size_bytes);
    ArrowBufferReset(&compressed);
    return rc;
}
int VgiTokenCodecDecode(struct ArrowBufferView input, int64_t limit, struct ArrowBuffer *out) {
    if (!out || out->size_bytes || input.size_bytes < 1 || limit < 0) return EINVAL;
    struct ArrowBufferView body = {{input.data.as_uint8 + 1}, input.size_bytes - 1};
    int rc;
    switch (input.data.as_uint8[0]) {
        case VGI_TOKEN_CODEC_RAW:
            rc = VgiHttpDecode("identity", body, limit, out);
            break;
        case VGI_TOKEN_CODEC_ZSTD:
            rc = VgiHttpDecode("zstd", body, limit, out);
            break;
        default:
            /* Reserved and unknown tags, DEFLATE included: this writer never
             * emits them, so they are refused rather than guessed at. */
            rc = EINVAL;
    }
    if (rc) ArrowBufferReset(out);
    return rc;
}
int VgiTokenSealRaw(const struct VgiTokenContext *c, struct ArrowBufferView input,
                    struct ArrowBuffer *out) {
    if (!c || !c->key || !out || out->size_bytes || input.size_bytes < 0 ||
        input.size_bytes > VGI_TOKEN_MAX_BYTES + 1)
        return EINVAL;
    struct ArrowBuffer aad;
    ArrowBufferInit(&aad);
    int rc = aad_init(c, &aad);
    if (!rc) rc = ArrowBufferResize(out, 25 + input.size_bytes + 16, 0);
    if (!rc) {
        out->data[0] = 1;
        randombytes_buf(out->data + 1, 24);
        unsigned long long n;
        rc = crypto_aead_xchacha20poly1305_ietf_encrypt(
            out->data + 25, &n, input.data.as_uint8, (unsigned long long)input.size_bytes, aad.data,
            (unsigned long long)aad.size_bytes, NULL, out->data + 1, c->key);
    }
    if (rc) ArrowBufferReset(out);
    ArrowBufferReset(&aad);
    return rc;
}
int VgiTokenOpenRaw(const struct VgiTokenContext *c, struct ArrowBufferView input,
                    struct ArrowBuffer *out) {
    if (!c || !c->key || !out || out->size_bytes || input.size_bytes < 0) return EINVAL;
    if (input.size_bytes < 42 || input.data.as_uint8[0] != 1) return EINVAL;
    struct ArrowBuffer aad;
    ArrowBufferInit(&aad);
    int rc = aad_init(c, &aad);
    if (!rc) rc = ArrowBufferResize(out, input.size_bytes - 41, 0);
    unsigned long long length = 0;
    if (!rc && crypto_aead_xchacha20poly1305_ietf_decrypt(
                   out->data, &length, NULL, input.data.as_uint8 + 25,
                   (unsigned long long)input.size_bytes - 25, aad.data,
                   (unsigned long long)aad.size_bytes, input.data.as_uint8 + 1, c->key))
        rc = EINVAL;
    if (!rc)
        out->size_bytes = (int64_t)length;
    else {
        sodium_memzero(out->data, (size_t)out->size_bytes);
        ArrowBufferReset(out);
    }
    ArrowBufferReset(&aad);
    return rc;
}
int VgiTokenSeal(const struct VgiTokenContext *c, struct ArrowBufferView input, char **out) {
    if (!c || !c->key || !out || *out || input.size_bytes < 0 ||
        input.size_bytes > VGI_TOKEN_MAX_BYTES)
        return EINVAL;
    struct ArrowBuffer plain, envelope;
    ArrowBufferInit(&plain);
    ArrowBufferInit(&envelope);
    int rc = VgiTokenCodecEncode(input, &plain);
    if (!rc)
        rc =
            VgiTokenSealRaw(c, (struct ArrowBufferView){{plain.data}, plain.size_bytes}, &envelope);
    if (!rc) {
        size_t n = sodium_base64_ENCODED_LEN((size_t)envelope.size_bytes,
                                             sodium_base64_VARIANT_URLSAFE_NO_PADDING);
        *out = malloc(n);
        if (!*out)
            rc = ENOMEM;
        else
            sodium_bin2base64(*out, n, envelope.data, (size_t)envelope.size_bytes,
                              sodium_base64_VARIANT_URLSAFE_NO_PADDING);
    }
    sodium_memzero(plain.data, (size_t)plain.size_bytes);
    ArrowBufferReset(&plain);
    ArrowBufferReset(&envelope);
    return rc;
}
int VgiTokenOpen(const struct VgiTokenContext *c, struct ArrowStringView input,
                 struct ArrowBuffer *out) {
    if (!c || !c->key || !out || out->size_bytes || input.size_bytes < 56 ||
        input.size_bytes > (VGI_TOKEN_MAX_BYTES + 64LL) * 4 / 3)
        return EINVAL;
    struct ArrowBuffer envelope, plain;
    ArrowBufferInit(&envelope);
    ArrowBufferInit(&plain);
    int rc = ArrowBufferResize(&envelope, input.size_bytes, 0);
    size_t n = 0;
    if (!rc && sodium_base642bin(envelope.data, (size_t)envelope.size_bytes, input.data,
                                 (size_t)input.size_bytes, NULL, &n, NULL,
                                 sodium_base64_VARIANT_URLSAFE_NO_PADDING))
        rc = EINVAL;
    if (!rc) rc = VgiTokenOpenRaw(c, (struct ArrowBufferView){{envelope.data}, (int64_t)n}, &plain);
    /* Codec tags follow the standard: an unknown tag or an undecodable body is
     * the same uniform failure as a bad seal. */
    if (!rc)
        rc = VgiTokenCodecDecode((struct ArrowBufferView){{plain.data}, plain.size_bytes},
                                 VGI_TOKEN_MAX_BYTES, out);
    if (rc) ArrowBufferReset(out);
    sodium_memzero(plain.data, (size_t)plain.size_bytes);
    ArrowBufferReset(&envelope);
    ArrowBufferReset(&plain);
    return rc;
}
