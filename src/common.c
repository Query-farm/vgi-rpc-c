#include <errno.h>
#include <string.h>

#include "rpc_internal.h"

int VgiMetadataSet(struct ArrowBuffer *metadata, const char *key, const char *value) {
    if (!metadata->data) NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderInit(metadata, NULL));
    return ArrowMetadataBuilderSet(metadata, ArrowCharView(key), ArrowCharView(value));
}

struct ArrowStringView VgiMetadataGet(const char *metadata, const char *key) {
    struct ArrowStringView value = {NULL, 0};
    if (metadata && ArrowMetadataGetValue(metadata, ArrowCharView(key), &value))
        value = (struct ArrowStringView){NULL, 0};
    return value;
}

int VgiRpcBatchInitNamed(struct VgiBatch *batch, const char *name, struct ArrowError *error) {
    struct ArrowSchema schema = {0};
    int rc = VgiRpcSchemaInit(!strcmp(name, "Empty") ? "RpcEmpty" : name, &schema, error);
    if (!rc) rc = VgiBatchInit(batch, &schema, error);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
