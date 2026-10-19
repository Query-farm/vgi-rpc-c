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
int VgiTokenSeal(const struct VgiTokenContext *c, struct ArrowBufferView input, char **out) {
    if (!c || !c->key || !out || *out || input.size_bytes < 0 ||
        input.size_bytes > VGI_TOKEN_MAX_BYTES)
        return EINVAL;
    struct ArrowBuffer aad, compressed, plain, envelope;
    ArrowBufferInit(&aad);
    ArrowBufferInit(&compressed);
    ArrowBufferInit(&plain);
    ArrowBufferInit(&envelope);
    int rc = aad_init(c, &aad);
    if (!rc && input.size_bytes >= 512) rc = VgiHttpEncode("zstd", input, &compressed);
    uint8_t codec = compressed.size_bytes && compressed.size_bytes < input.size_bytes ? 1 : 0;
    if (!rc) rc = ArrowBufferAppend(&plain, &codec, 1);
    if (!rc)
        rc = ArrowBufferAppend(&plain, codec ? compressed.data : input.data.as_uint8,
                               codec ? compressed.size_bytes : input.size_bytes);
    if (!rc) rc = ArrowBufferResize(&envelope, 25 + plain.size_bytes + 16, 0);
    if (!rc) {
        envelope.data[0] = 1;
        randombytes_buf(envelope.data + 1, 24);
        unsigned long long n;
        rc = crypto_aead_xchacha20poly1305_ietf_encrypt(
            envelope.data + 25, &n, plain.data, (unsigned long long)plain.size_bytes, aad.data,
            (unsigned long long)aad.size_bytes, NULL, envelope.data + 1, c->key);
    }
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
    ArrowBufferReset(&aad);
    ArrowBufferReset(&compressed);
    ArrowBufferReset(&plain);
    ArrowBufferReset(&envelope);
    return rc;
}
int VgiTokenOpen(const struct VgiTokenContext *c, struct ArrowStringView input,
                 struct ArrowBuffer *out) {
    if (!c || !c->key || !out || out->size_bytes || input.size_bytes < 56 ||
        input.size_bytes > (VGI_TOKEN_MAX_BYTES + 64LL) * 4 / 3)
        return EINVAL;
    struct ArrowBuffer aad, envelope, plain;
    ArrowBufferInit(&aad);
    ArrowBufferInit(&envelope);
    ArrowBufferInit(&plain);
    int rc = aad_init(c, &aad);
    if (!rc) rc = ArrowBufferResize(&envelope, input.size_bytes, 0);
    size_t n = 0;
    if (!rc && sodium_base642bin(envelope.data, (size_t)envelope.size_bytes, input.data,
                                 (size_t)input.size_bytes, NULL, &n, NULL,
                                 sodium_base64_VARIANT_URLSAFE_NO_PADDING))
        rc = EINVAL;
    if (!rc && (n < 42 || envelope.data[0] != 1)) rc = EINVAL;
    if (!rc) rc = ArrowBufferResize(&plain, (int64_t)n - 41, 0);
    unsigned long long length = 0;
    if (!rc && crypto_aead_xchacha20poly1305_ietf_decrypt(
                   plain.data, &length, NULL, envelope.data + 25, n - 25, aad.data,
                   (unsigned long long)aad.size_bytes, envelope.data + 1, c->key))
        rc = EINVAL;
    if (!rc && (!length || plain.data[0] > 1)) rc = EINVAL;
    if (!rc)
        rc = VgiHttpDecode(plain.data[0] ? "zstd" : "identity",
                           (struct ArrowBufferView){{plain.data + 1}, (int64_t)length - 1},
                           VGI_TOKEN_MAX_BYTES, out);
    if (rc) ArrowBufferReset(out);
    sodium_memzero(plain.data, (size_t)plain.size_bytes);
    ArrowBufferReset(&aad);
    ArrowBufferReset(&envelope);
    ArrowBufferReset(&plain);
    return rc;
}
