#ifndef VGI_BATCH_H
#define VGI_BATCH_H
#include "vgi_rpc/ipc.h"
#ifdef __cplusplus
extern "C" {
#endif

struct VgiBatch {
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowArrayView view;
};
void VgiBatchReset(struct VgiBatch *batch);
int VgiBatchInit(struct VgiBatch *batch, const struct ArrowSchema *schema,
                 struct ArrowError *error);
int VgiBatchDecode(struct VgiBatch *batch, struct ArrowBufferView bytes, struct ArrowError *error);
int VgiBatchView(struct VgiBatch *batch, struct ArrowError *error);
int64_t VgiFieldIndex(const struct ArrowSchema *schema, const char *name);
struct ArrowArray *VgiBatchColumn(struct VgiBatch *batch, const char *name);
const struct ArrowArrayView *VgiBatchGet(const struct VgiBatch *batch, const char *name);
struct ArrowStringView VgiBatchString(const struct VgiBatch *batch, const char *name);
struct ArrowBufferView VgiBatchBytes(const struct VgiBatch *batch, const char *name);
int64_t VgiBatchInt(const struct VgiBatch *batch, const char *name, int64_t fallback);
int VgiBatchIsNull(const struct VgiBatch *batch, const char *name);
int VgiBatchChild(const struct VgiBatch *batch, const char *name, struct VgiBatch *out,
                  struct ArrowError *error);
/* Setters append one value. FinishRow fills omitted fields with null when
 * nullable, otherwise an empty/default value; use Finish for explicit rows. */
int VgiBatchSetString(struct VgiBatch *batch, const char *name, const char *value);
/* Binary columns may be plain or dictionary-encoded (dictionary<int32, binary>). */
int VgiBatchSetBytes(struct VgiBatch *batch, const char *name, const void *data, int64_t size);
int VgiBatchSetInt(struct VgiBatch *batch, const char *name, int64_t value);
int VgiBatchSetNull(struct VgiBatch *batch, const char *name);
int VgiBatchFinishRow(struct VgiBatch *batch, struct ArrowError *error);
int VgiBatchFinish(struct VgiBatch *batch, struct ArrowError *error);
int VgiArrayAppendString(struct ArrowArray *array, struct ArrowStringView value);
int VgiArrayAppendDefault(struct ArrowArray *array, const struct ArrowSchema *schema);
/* Copy one logical value, decoding dictionaries and casting compatible storage.
 * The destination must be an appending nanoarrow array with the given schema. */
int VgiArrayAppendValue(struct ArrowArray *out, const struct ArrowSchema *schema,
                        const struct ArrowArrayView *value, const struct ArrowSchema *value_schema,
                        int64_t index, struct ArrowError *error);
int VgiBatchCoerce(const struct VgiBatch *input, const struct ArrowSchema *schema,
                   struct VgiBatch *out, struct ArrowError *error);
struct ArrowStringView VgiValueString(const struct ArrowArrayView *value, int64_t index);
int VgiValueIsNumeric(const struct ArrowArrayView *value);
/* Numbers convert directly, decimals and text through their decimal string;
 * any other type (nested, interval, union, ...) fails with EINVAL. */
int VgiValueDouble(const struct ArrowArrayView *value, const struct ArrowSchema *schema,
                   int64_t index, double *out, struct ArrowError *error);
#ifdef __cplusplus
}
#endif
#endif
