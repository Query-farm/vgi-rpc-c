#include "vgi_rpc/crypto.h"

#include <errno.h>
#include <sodium.h>

int VgiSha256(const void *data, size_t size, uint8_t out[32]) {
    if ((!data && size) || !out) return EINVAL;
    return crypto_hash_sha256(out, (const unsigned char *)data,
                              (unsigned long long)size) == 0 ? 0 : EIO;
}
