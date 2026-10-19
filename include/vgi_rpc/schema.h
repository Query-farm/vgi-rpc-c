#ifndef VGI_SCHEMA_INTERNAL_H
#define VGI_SCHEMA_INTERNAL_H
#include "nanoarrow/nanoarrow.h"

struct VgiSchemaNode {
    const char *format;
    const char *name;
    const char *metadata;
    int64_t flags;
    int64_t n_children;
    const struct VgiSchemaNode *const *children;
    const struct VgiSchemaNode *dictionary;
};
int VgiSchemaBuild(const struct VgiSchemaNode *node, struct ArrowSchema *out,
                   struct ArrowError *error);
#endif
