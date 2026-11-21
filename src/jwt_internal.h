#ifndef VGI_RPC_JWT_INTERNAL_H
#define VGI_RPC_JWT_INTERNAL_H
/* Internal JWT entry points, exposed for the native tests. */
#include <stddef.h>

#include "vgi_rpc/jwt.h"

struct KeySet;
typedef double (*VgiJwtClock)(void *);

/* Replace the monotonic clock (key-set ages) and the wall clock (claims, in
 * seconds since the epoch). NULL restores the system clock. */
void VgiJwtSetClock(struct VgiJwtVerifier *, VgiJwtClock monotonic, VgiJwtClock wall, void *state);
/* Bit i of *out set: the i-th supported algorithm is allowed. */
int VgiJwtParseAlgorithms(const char *list, unsigned *out, struct ArrowError *error);
int VgiJwtCacheControlMaxAge(const char *value, double *out);
int VgiJwtKeySetParse(const char *json, size_t size, struct KeySet **out);
void VgiJwtKeySetFree(struct KeySet *);
size_t VgiJwtKeySetCount(const struct KeySet *);
/* Verify one compact token against keys at wall time now. Returns 0 with
 * owned claims and principal, EACCES, ESTALE, ENOMEM, or -1 for a kid that
 * none of the keys carries. */
int VgiJwtVerifyWithKeys(const struct VgiJwtVerifier *, const struct KeySet *, const char *token,
                         double now, char **claims, char **principal, const char **reason);
#endif
