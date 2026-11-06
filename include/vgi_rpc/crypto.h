#ifndef VGI_RPC_CRYPTO_H
#define VGI_RPC_CRYPTO_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Compute a SHA-256 digest without exposing the transport's crypto provider. */
int VgiSha256(const void *data, size_t size, uint8_t out[32]);

#ifdef __cplusplus
}
#endif
#endif
