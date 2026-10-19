#ifndef VGI_HTTP_CODEC_H
#define VGI_HTTP_CODEC_H
#include "nanoarrow/nanoarrow.h"
int VgiHttpDecode(const char *, struct ArrowBufferView, int64_t, struct ArrowBuffer *);
int VgiHttpEncode(const char *, struct ArrowBufferView, struct ArrowBuffer *);
#endif
