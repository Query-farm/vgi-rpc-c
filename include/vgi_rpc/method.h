#ifndef VGI_RPC_METHOD_H
#define VGI_RPC_METHOD_H
#include "nanoarrow/nanoarrow.h"
typedef int (*VgiSchemaFactory)(struct ArrowSchema *out, struct ArrowError *error);
enum VgiMethodKind { VGI_METHOD_UNARY, VGI_METHOD_VOID, VGI_METHOD_STREAM };
enum VgiStreamKind { VGI_STREAM_UNKNOWN, VGI_STREAM_PRODUCER, VGI_STREAM_EXCHANGE };
struct VgiMethod {
    const char *name;
    enum VgiMethodKind kind;
    VgiSchemaFactory params;
    VgiSchemaFactory result;
    VgiSchemaFactory payload;
    VgiSchemaFactory header;
    enum VgiStreamKind stream_kind;
};
#endif
