#include "vgi_rpc/schema.h"

#include <errno.h>

static int build(const struct VgiSchemaNode *node, struct ArrowSchema *out) {
    ArrowSchemaInit(out);
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetFormat(out, node->format));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(out, node->name));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetMetadata(out, node->metadata));
    out->flags = node->flags;
    NANOARROW_RETURN_NOT_OK(ArrowSchemaAllocateChildren(out, node->n_children));
    for (int64_t i = 0; i < node->n_children; ++i) {
        NANOARROW_RETURN_NOT_OK(build(node->children[i], out->children[i]));
    }
    if (node->dictionary) {
        NANOARROW_RETURN_NOT_OK(ArrowSchemaAllocateDictionary(out));
        NANOARROW_RETURN_NOT_OK(build(node->dictionary, out->dictionary));
    }
    return NANOARROW_OK;
}

int VgiSchemaBuild(const struct VgiSchemaNode *node, struct ArrowSchema *out,
                   struct ArrowError *error) {
    if (!node || !out || out->release) return EINVAL;
    int result = build(node, out);
    if (result) {
        if (out->release) ArrowSchemaRelease(out);
        ArrowErrorSet(error, "Failed to construct VGI schema (error %d)", result);
    }
    return result;
}
