#ifndef VGI_TOKEN_H
#define VGI_TOKEN_H
#include "nanoarrow/nanoarrow.h"
#define VGI_TOKEN_MAX_BYTES (32 * 1024 * 1024)
/* The standard token codec (vgi-rpc STATE_REF_V1.md §7): every sealed payload
 * is codec (1 byte) || body. This port writes RAW or ZSTD; a reader accepts
 * exactly the codecs its writer emits and rejects every other tag. */
#define VGI_TOKEN_CODEC_RAW 0x00
#define VGI_TOKEN_CODEC_ZSTD 0x01
/* Reserved by the standard for runtimes without zstd; never written here, so
 * refused like any unknown tag. */
#define VGI_TOKEN_CODEC_DEFLATE 0x02
/* Tokens are opaque, bounded, independently sealed by purpose/route/identity.
 * Returned text is malloc-owned. Open outputs are empty on failure. */
struct VgiTokenContext {
    const unsigned char *key;
    const char *purpose, *protocol, *method, *domain, *principal;
};
int VgiTokenSeal(const struct VgiTokenContext *, struct ArrowBufferView, char **);
int VgiTokenOpen(const struct VgiTokenContext *, struct ArrowStringView, struct ArrowBuffer *);
int VgiTokenAppendU64(struct ArrowBuffer *, uint64_t);
uint64_t VgiTokenReadU64(const uint8_t *);
/* codec || body: zstd when that is smaller than the input, raw otherwise
 * (a small payload never grows). Appends to out. */
int VgiTokenCodecEncode(struct ArrowBufferView, struct ArrowBuffer *);
/* Inverse of VgiTokenCodecEncode, bounded at limit decoded bytes. EINVAL for
 * an empty payload, an unknown codec tag, or a body that fails to decode;
 * EFBIG over the limit. out must be empty; it is left empty on failure. */
int VgiTokenCodecDecode(struct ArrowBufferView, int64_t limit, struct ArrowBuffer *);
/* Binary AEAD envelope 0x01 || nonce (24) || ciphertext+tag (16), with the
 * context's purpose/route/identity as associated data. No codec, no base64:
 * callers seal already-encoded payloads. out must be empty. */
int VgiTokenSealRaw(const struct VgiTokenContext *, struct ArrowBufferView, struct ArrowBuffer *);
int VgiTokenOpenRaw(const struct VgiTokenContext *, struct ArrowBufferView, struct ArrowBuffer *);
#endif
