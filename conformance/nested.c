/* Copyright 2026 Query Farm LLC. SPDX-License-Identifier: Apache-2.0 */
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "protocol.h"
#include "vgi_rpc/rpc.h"

static int point(struct ArrowArray *out, double x, double y) {
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendDouble(out->children[0], x));
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendDouble(out->children[1], y));
    return ArrowArrayFinishElement(out);
}
static int text_number(struct ArrowArray *out, const char *prefix, int64_t n) {
    char buffer[96];
    snprintf(buffer, sizeof(buffer), "%s%" PRId64, prefix, n);
    return ArrowArrayAppendString(out, ArrowCharView(buffer));
}
static int map_int(struct ArrowArray *out, const char *key, int64_t value) {
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendString(out->children[0], ArrowCharView(key)));
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(out->children[1], value));
    return ArrowArrayFinishElement(out);
}
int ConformanceRichHeader(struct VgiBatch *out, int64_t seed, struct ArrowError *error) {
    struct ArrowSchema schema = {0};
    int rc = VgiSchemaRichHeader(&schema, error);
    if (!rc) rc = VgiBatchInit(out, &schema, error);
    if (schema.release) ArrowSchemaRelease(&schema);
    if (rc) return rc;
    struct ArrowArray **a = out->array.children;
    NANOARROW_RETURN_NOT_OK(text_number(a[0], "seed-", seed));
    uint8_t bytes[3] = {(uint8_t)seed, (uint8_t)(seed + 1), (uint8_t)(seed + 2)};
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendBytes(a[1], (struct ArrowBufferView){{bytes}, 3}));
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(a[2], seed * 7));
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendDouble(a[3], (double)seed * 1.5));
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(a[4], seed % 2 == 0));
    for (int i = 0; i < 3; ++i)
        NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(a[5]->children[0], seed + i));
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(a[5]));
    for (int i = 0; i < 2; ++i)
        NANOARROW_RETURN_NOT_OK(text_number(a[6]->children[0], "item-", seed + i));
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(a[6]));
    NANOARROW_RETURN_NOT_OK(map_int(a[7]->children[0], "a", seed));
    NANOARROW_RETURN_NOT_OK(map_int(a[7]->children[0], "b", seed + 1));
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(a[7]));
    const char *statuses[] = {"pending", "active", "closed"};
    NANOARROW_RETURN_NOT_OK(
        VgiArrayAppendString(a[8], ArrowCharView(statuses[(seed % 3 + 3) % 3])));
    NANOARROW_RETURN_NOT_OK(point(a[9], (double)seed, (double)seed * 2));
    NANOARROW_RETURN_NOT_OK(seed % 2 == 0 ? text_number(a[10], "opt-", seed)
                                          : ArrowArrayAppendNull(a[10], 1));
    NANOARROW_RETURN_NOT_OK(seed % 2 != 0 ? ArrowArrayAppendInt(a[11], seed * 3)
                                          : ArrowArrayAppendNull(a[11], 1));
    NANOARROW_RETURN_NOT_OK(seed % 3 == 0 ? point(a[12], (double)seed, 0)
                                          : ArrowArrayAppendNull(a[12], 1));
    NANOARROW_RETURN_NOT_OK(point(a[13]->children[0], (double)seed, (double)(seed + 1)));
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(a[13]));
    struct ArrowArray *inner = a[14]->children[0];
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(inner->children[0], seed));
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(inner->children[0], seed + 1));
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(inner));
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(inner->children[0], seed + 2));
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(inner));
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(a[14]));
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(a[15], (seed % 1000 + 1000) % 1000));
    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendDouble(a[16], (double)seed / 3));
    NANOARROW_RETURN_NOT_OK(
        ArrowArrayAppendString(a[17]->children[0]->children[0], ArrowCharView("key")));
    NANOARROW_RETURN_NOT_OK(text_number(a[17]->children[0]->children[1], "val-", seed));
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(a[17]->children[0]));
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(a[17]));
    return VgiBatchFinishRow(out, error);
}
static int copy(struct ArrowArray *out, const struct ArrowSchema *schema,
                const struct ArrowArrayView *in, const struct ArrowSchema *in_schema, int64_t row,
                struct ArrowError *error) {
    return VgiArrayAppendValue(out, schema, in, in_schema, row, error);
}
int ConformancePackNested(struct VgiRpcCall *c, struct VgiRpcReply *reply) {
    struct VgiBatch payload = {0}, tagged = {0};
    struct ArrowSchema schema = {0};
    struct ArrowBuffer ipc;
    ArrowBufferInit(&ipc);
    int rc = VgiSchemaNestedContainers(&schema, &c->error);
    if (!rc) rc = VgiBatchInit(&payload, &schema, &c->error);
    if (schema.release) ArrowSchemaRelease(&schema);
    for (int i = 0; !rc && i < 3; ++i)
        rc = copy(payload.array.children[i], payload.schema.children[i],
                  c->request.view.children[i], c->request.schema.children[i], 0, &c->error);
    const struct ArrowArrayView *statuses = VgiBatchGet(&c->request, "statuses"),
                                *points = VgiBatchGet(&c->request, "points");
    const struct ArrowSchema *status_schema =
        c->request.schema.children[VgiFieldIndex(&c->request.schema, "statuses")];
    const struct ArrowSchema *point_schema =
        c->request.schema.children[VgiFieldIndex(&c->request.schema, "points")];
    int64_t start = ArrowArrayViewListChildOffset(statuses, 0),
            end = ArrowArrayViewListChildOffset(statuses, 1);
    for (int64_t i = start; !rc && i < end; ++i) {
        struct ArrowStringView value = VgiValueString(statuses->children[0], i);
        int seen = 0;
        for (int64_t j = start; j < i; ++j) {
            struct ArrowStringView previous = VgiValueString(statuses->children[0], j);
            if (value.size_bytes == previous.size_bytes &&
                !memcmp(value.data, previous.data, (size_t)value.size_bytes))
                seen = 1;
        }
        if (!seen)
            rc = copy(payload.array.children[3]->children[0],
                      payload.schema.children[3]->children[0], statuses->children[0],
                      status_schema->children[0], i, &c->error);
    }
    if (!rc) rc = ArrowArrayFinishElement(payload.array.children[3]);
    if (!rc)
        rc = end > start ? copy(payload.array.children[4], payload.schema.children[4],
                                statuses->children[0], status_schema->children[0], start, &c->error)
                         : ArrowArrayAppendNull(payload.array.children[4], 1);
    start = ArrowArrayViewListChildOffset(points, 0);
    end = ArrowArrayViewListChildOffset(points, 1);
    if (!rc)
        rc = end > start ? copy(payload.array.children[5], payload.schema.children[5],
                                points->children[0], point_schema->children[0], start, &c->error)
                         : ArrowArrayAppendNull(payload.array.children[5], 1);
    ArrowSchemaInit(&schema);
    if (!rc) rc = ArrowSchemaSetTypeStruct(&schema, 1);
    if (!rc) rc = ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT64);
    if (!rc) rc = ArrowSchemaSetName(schema.children[0], "value");
    if (!rc) rc = VgiBatchInit(&tagged, &schema, &c->error);
    for (int i = 1; !rc && i <= 2; ++i) {
        rc = ArrowArrayAppendInt(tagged.array.children[0], i);
        if (!rc) rc = ArrowArrayFinishElement(&tagged.array);
    }
    if (!rc) rc = VgiBatchFinish(&tagged, &c->error);
    if (!rc) rc = VgiIpcWriteBatch(&tagged.schema, &tagged.array, NULL, &ipc, &c->error);
    if (!rc) rc = VgiBatchSetBytes(&payload, "tagged_batch", ipc.data, ipc.size_bytes);
    if (!rc) rc = VgiBatchFinishRow(&payload, &c->error);
    if (!rc) rc = VgiRpcReplyPayload(c, reply, &payload);
    ArrowBufferReset(&ipc);
    VgiBatchReset(&payload);
    VgiBatchReset(&tagged);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
