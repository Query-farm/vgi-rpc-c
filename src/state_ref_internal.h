#ifndef VGI_STATE_REF_INTERNAL_H
#define VGI_STATE_REF_INTERNAL_H
#include "nanoarrow/nanoarrow.h"
/* Seal an argument reference for a grant (STATE_REF_V1.md §10.2): ARGREF_AAD
 * binds it to the grant request's principal; the size and sha256 are the
 * claimed values the grant was minted for. out must be empty. */
int VgiArgumentRefMint(const unsigned char *key, const char *domain, const char *principal,
                       int64_t size, const char *sha256_hex, const char *object_id,
                       struct ArrowBuffer *out);
#endif
