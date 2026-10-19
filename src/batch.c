#include "vgi_rpc/batch.h"

#include <errno.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

void VgiBatchReset(struct VgiBatch *batch) {
    ArrowArrayViewReset(&batch->view);
    if (batch->array.release) ArrowArrayRelease(&batch->array);
    if (batch->schema.release) ArrowSchemaRelease(&batch->schema);
    memset(batch, 0, sizeof(*batch));
}
int VgiBatchInit(struct VgiBatch *batch, const struct ArrowSchema *schema,
                 struct ArrowError *error) {
    if (!batch || batch->schema.release || batch->array.release) return EINVAL;
    int rc = ArrowSchemaDeepCopy(schema, &batch->schema);
    if (!rc) rc = ArrowArrayInitFromSchema(&batch->array, schema, error);
    if (!rc) rc = ArrowArrayStartAppending(&batch->array);
    if (rc) VgiBatchReset(batch);
    return rc;
}
int VgiBatchView(struct VgiBatch *batch, struct ArrowError *error) {
    ArrowArrayViewReset(&batch->view);
    int rc = ArrowArrayViewInitFromSchema(&batch->view, &batch->schema, error);
    if (!rc) rc = ArrowArrayViewSetArray(&batch->view, &batch->array, error);
    return rc;
}
int VgiBatchDecode(struct VgiBatch *batch, struct ArrowBufferView bytes, struct ArrowError *error) {
    if (!batch || batch->schema.release || batch->array.release) return EINVAL;
    struct ArrowBuffer metadata;
    ArrowBufferInit(&metadata);
    int rc = VgiIpcReadBatch(bytes, &batch->schema, &batch->array, &metadata, error);
    if (!rc) rc = VgiBatchView(batch, error);
    ArrowBufferReset(&metadata);
    if (rc) VgiBatchReset(batch);
    return rc;
}
int64_t VgiFieldIndex(const struct ArrowSchema *schema, const char *name) {
    if (!schema || !name) return -1;
    for (int64_t i = 0; i < schema->n_children; ++i)
        if (schema->children[i]->name && !strcmp(schema->children[i]->name, name)) return i;
    return -1;
}
struct ArrowArray *VgiBatchColumn(struct VgiBatch *batch, const char *name) {
    int64_t index = VgiFieldIndex(&batch->schema, name);
    return index < 0 ? NULL : batch->array.children[index];
}
const struct ArrowArrayView *VgiBatchGet(const struct VgiBatch *batch, const char *name) {
    int64_t index = VgiFieldIndex(&batch->schema, name);
    return index < 0 || index >= batch->view.n_children ? NULL : batch->view.children[index];
}
int VgiBatchIsNull(const struct VgiBatch *batch, const char *name) {
    const struct ArrowArrayView *col = VgiBatchGet(batch, name);
    return !col || !col->length || ArrowArrayViewIsNull(col, 0);
}
static const struct ArrowArrayView *string_view(const struct ArrowArrayView *view, int64_t *index) {
    if (view && view->dictionary) {
        *index = ArrowArrayViewGetIntUnsafe(view, *index);
        view = view->dictionary;
    }
    return view;
}
struct ArrowStringView VgiBatchString(const struct VgiBatch *batch, const char *name) {
    if (VgiBatchIsNull(batch, name)) return (struct ArrowStringView){NULL, 0};
    return VgiValueString(VgiBatchGet(batch, name), 0);
}
struct ArrowStringView VgiValueString(const struct ArrowArrayView *value, int64_t index) {
    if (!value || index < 0 || index >= value->length || ArrowArrayViewIsNull(value, index))
        return (struct ArrowStringView){NULL, 0};
    value = string_view(value, &index);
    if (index < 0 || index >= value->length) return (struct ArrowStringView){NULL, 0};
    switch (value->storage_type) {
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING:
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY:
        case NANOARROW_TYPE_STRING_VIEW:
        case NANOARROW_TYPE_BINARY_VIEW:
        case NANOARROW_TYPE_FIXED_SIZE_BINARY:
            return ArrowArrayViewGetStringUnsafe(value, index);
        default:
            return (struct ArrowStringView){NULL, 0};
    }
}
int VgiValueIsNumeric(const struct ArrowArrayView *value) {
    if (!value) return 0;
    switch (value->storage_type) {
        case NANOARROW_TYPE_BOOL:
        case NANOARROW_TYPE_INT8:
        case NANOARROW_TYPE_INT16:
        case NANOARROW_TYPE_INT32:
        case NANOARROW_TYPE_INT64:
        case NANOARROW_TYPE_UINT8:
        case NANOARROW_TYPE_UINT16:
        case NANOARROW_TYPE_UINT32:
        case NANOARROW_TYPE_UINT64:
        case NANOARROW_TYPE_HALF_FLOAT:
        case NANOARROW_TYPE_FLOAT:
        case NANOARROW_TYPE_DOUBLE:
            return 1;
        default:
            return 0;
    }
}
int VgiValueDouble(const struct ArrowArrayView *value, const struct ArrowSchema *schema,
                   int64_t index, double *out, struct ArrowError *error) {
    if (!value || index < 0 || index >= value->length || ArrowArrayViewIsNull(value, index))
        return EINVAL;
    if (value->dictionary)
        return VgiValueDouble(value->dictionary, schema->dictionary,
                              ArrowArrayViewGetIntUnsafe(value, index), out, error);
    if (VgiValueIsNumeric(value)) {
        *out = ArrowArrayViewGetDoubleUnsafe(value, index);
        return 0;
    }
    struct ArrowSchemaView type;
    NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&type, schema, error));
    struct ArrowBuffer text;
    ArrowBufferInit(&text);
    int rc = 0;
    if (type.decimal_bitwidth) {
        struct ArrowDecimal decimal;
        ArrowDecimalInit(&decimal, type.decimal_bitwidth, type.decimal_precision,
                         type.decimal_scale);
        ArrowArrayViewGetDecimalUnsafe(value, index, &decimal);
        rc = ArrowDecimalAppendStringToBuffer(&decimal, &text);
    } else {
        struct ArrowStringView string = VgiValueString(value, index);
        rc = string.data ? ArrowBufferAppend(&text, string.data, string.size_bytes) : EINVAL;
    }
    if (!rc) rc = ArrowBufferAppend(&text, "", 1);
    if (!rc) {
        char *end;
        *out = strtod((const char *)text.data, &end);
        if (end == (char *)text.data || end != (char *)text.data + text.size_bytes - 1) rc = EINVAL;
    }
    ArrowBufferReset(&text);
    if (rc) ArrowErrorSet(error, "Cannot convert Arrow value to double");
    return rc;
}
struct ArrowBufferView VgiBatchBytes(const struct VgiBatch *batch, const char *name) {
    struct ArrowStringView value = VgiBatchString(batch, name);
    return (struct ArrowBufferView){{value.data}, value.size_bytes};
}
int64_t VgiBatchInt(const struct VgiBatch *batch, const char *name, int64_t fallback) {
    const struct ArrowArrayView *value = VgiBatchGet(batch, name);
    return VgiBatchIsNull(batch, name) || !VgiValueIsNumeric(value)
               ? fallback
               : ArrowArrayViewGetIntUnsafe(value, 0);
}
int VgiBatchChild(const struct VgiBatch *batch, const char *name, struct VgiBatch *out,
                  struct ArrowError *error) {
    if (VgiBatchIsNull(batch, name)) return ENOENT;
    return VgiBatchDecode(out, VgiBatchBytes(batch, name), error);
}
int VgiArrayAppendString(struct ArrowArray *array, struct ArrowStringView value) {
    if (!array) return ENOENT;
    if (array->dictionary) {
        int64_t index = array->dictionary->length;
        NANOARROW_RETURN_NOT_OK(ArrowArrayAppendString(array->dictionary, value));
        return ArrowArrayAppendInt(array, index);
    }
    return ArrowArrayAppendString(array, value);
}
int VgiBatchSetString(struct VgiBatch *batch, const char *name, const char *value) {
    return value ? VgiArrayAppendString(VgiBatchColumn(batch, name), ArrowCharView(value))
                 : VgiBatchSetNull(batch, name);
}
int VgiBatchSetBytes(struct VgiBatch *batch, const char *name, const void *data, int64_t size) {
    struct ArrowArray *col = VgiBatchColumn(batch, name);
    if (!col) return ENOENT;
    struct ArrowBufferView view = {{data}, size};
    return ArrowArrayAppendBytes(col, view);
}
int VgiBatchSetInt(struct VgiBatch *batch, const char *name, int64_t value) {
    struct ArrowArray *col = VgiBatchColumn(batch, name);
    return col ? ArrowArrayAppendInt(col, value) : ENOENT;
}
int VgiBatchSetNull(struct VgiBatch *batch, const char *name) {
    struct ArrowArray *col = VgiBatchColumn(batch, name);
    return col ? ArrowArrayAppendNull(col, 1) : ENOENT;
}
int VgiArrayAppendDefault(struct ArrowArray *array, const struct ArrowSchema *schema) {
    if (schema->flags & ARROW_FLAG_NULLABLE) return ArrowArrayAppendNull(array, 1);
    if (array->dictionary) return VgiArrayAppendString(array, ArrowCharView(""));
    struct ArrowSchemaView view;
    NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&view, schema, NULL));
    switch (view.type) {
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING:
            return ArrowArrayAppendString(array, ArrowCharView(""));
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY:
            return ArrowArrayAppendBytes(array, (struct ArrowBufferView){{""}, 0});
        case NANOARROW_TYPE_STRUCT:
            for (int64_t i = 0; i < array->n_children; ++i)
                NANOARROW_RETURN_NOT_OK(
                    VgiArrayAppendDefault(array->children[i], schema->children[i]));
            return ArrowArrayFinishElement(array);
        case NANOARROW_TYPE_LIST:
        case NANOARROW_TYPE_LARGE_LIST:
        case NANOARROW_TYPE_MAP:
            return ArrowArrayFinishElement(array);
        default:
            return ArrowArrayAppendEmpty(array, 1);
    }
}
int VgiBatchFinishRow(struct VgiBatch *batch, struct ArrowError *error) {
    for (int64_t i = 0; i < batch->array.n_children; ++i) {
        struct ArrowArray *child = batch->array.children[i];
        if (child->length == batch->array.length)
            NANOARROW_RETURN_NOT_OK(VgiArrayAppendDefault(child, batch->schema.children[i]));
    }
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(&batch->array));
    return VgiBatchFinish(batch, error);
}
int VgiBatchFinish(struct VgiBatch *batch, struct ArrowError *error) {
    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(&batch->array, error));
    return VgiBatchView(batch, error);
}

int VgiArrayAppendValue(struct ArrowArray *out, const struct ArrowSchema *schema,
                        const struct ArrowArrayView *value, const struct ArrowSchema *value_schema,
                        int64_t index, struct ArrowError *error) {
    if (!value || index < 0 || index >= value->length) return EINVAL;
    if (ArrowArrayViewIsNull(value, index)) return ArrowArrayAppendNull(out, 1);
    if (value->dictionary) {
        index = ArrowArrayViewGetIntUnsafe(value, index);
        value = value->dictionary;
        value_schema = value_schema->dictionary;
        if (index < 0 || index >= value->length) return EINVAL;
    }
    if (out->dictionary) {
        /* Deduplicate to preserve narrow dictionary index types across long streams. */
        struct ArrowStringView string = VgiValueString(value, index);
        if (string.data) {
            struct ArrowArrayView existing = {0};
            int rc = ArrowArrayFinishBuildingDefault(out->dictionary, error);
            if (!rc) rc = ArrowArrayViewInitFromSchema(&existing, schema->dictionary, error);
            if (!rc) rc = ArrowArrayViewSetArray(&existing, out->dictionary, error);
            int64_t found = -1;
            for (int64_t i = 0; !rc && i < existing.length; ++i) {
                struct ArrowStringView candidate = VgiValueString(&existing, i);
                if (candidate.data && candidate.size_bytes == string.size_bytes &&
                    !memcmp(candidate.data, string.data, (size_t)string.size_bytes)) {
                    found = i;
                    break;
                }
            }
            ArrowArrayViewReset(&existing);
            if (rc) return rc;
            if (found >= 0) return ArrowArrayAppendInt(out, found);
        }
        int64_t next = out->dictionary->length;
        NANOARROW_RETURN_NOT_OK(VgiArrayAppendValue(out->dictionary, schema->dictionary, value,
                                                    value_schema, index, error));
        return ArrowArrayAppendInt(out, next);
    }
    struct ArrowSchemaView target;
    NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&target, schema, error));
    switch (target.type) {
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING:
        case NANOARROW_TYPE_STRING_VIEW: {
            struct ArrowStringView string = VgiValueString(value, index);
            return string.data ? ArrowArrayAppendString(out, string) : EINVAL;
        }
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY:
        case NANOARROW_TYPE_BINARY_VIEW:
        case NANOARROW_TYPE_FIXED_SIZE_BINARY: {
            struct ArrowStringView string = VgiValueString(value, index);
            return string.data
                       ? ArrowArrayAppendBytes(
                             out, (struct ArrowBufferView){{string.data}, string.size_bytes})
                       : EINVAL;
        }
        case NANOARROW_TYPE_SPARSE_UNION:
        case NANOARROW_TYPE_DENSE_UNION: {
            if (value->storage_type != NANOARROW_TYPE_SPARSE_UNION &&
                value->storage_type != NANOARROW_TYPE_DENSE_UNION)
                return EINVAL;
            int8_t source = ArrowArrayViewUnionChildIndex(value, index);
            if (source < 0 || source >= value->n_children) return EINVAL;
            int64_t destination = VgiFieldIndex(schema, value_schema->children[source]->name);
            if (destination < 0 || destination > 127) return EINVAL;
            NANOARROW_RETURN_NOT_OK(
                VgiArrayAppendValue(out->children[destination], schema->children[destination],
                                    value->children[source], value_schema->children[source],
                                    ArrowArrayViewUnionChildOffset(value, index), error));
            return ArrowArrayFinishUnionElement(out, (int8_t)destination);
        }
        case NANOARROW_TYPE_STRUCT:
            if (value->storage_type != NANOARROW_TYPE_STRUCT) return EINVAL;
            for (int64_t i = 0; i < schema->n_children; ++i) {
                int64_t field = VgiFieldIndex(value_schema, schema->children[i]->name);
                if (field < 0) {
                    if (!(schema->children[i]->flags & ARROW_FLAG_NULLABLE)) return EINVAL;
                    NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(out->children[i], 1));
                } else
                    NANOARROW_RETURN_NOT_OK(VgiArrayAppendValue(
                        out->children[i], schema->children[i], value->children[field],
                        value_schema->children[field], index + value->offset, error));
            }
            return ArrowArrayFinishElement(out);
        case NANOARROW_TYPE_LIST:
        case NANOARROW_TYPE_LARGE_LIST:
        case NANOARROW_TYPE_MAP:
        case NANOARROW_TYPE_FIXED_SIZE_LIST: {
            int64_t start, end;
            if (value->storage_type == NANOARROW_TYPE_FIXED_SIZE_LIST) {
                struct ArrowSchemaView source;
                NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&source, value_schema, error));
                start = (index + value->offset) * source.fixed_size;
                end = start + source.fixed_size;
            } else if (value->storage_type == NANOARROW_TYPE_LIST ||
                       value->storage_type == NANOARROW_TYPE_LARGE_LIST ||
                       value->storage_type == NANOARROW_TYPE_MAP) {
                start = ArrowArrayViewListChildOffset(value, index);
                end = ArrowArrayViewListChildOffset(value, index + 1);
            } else
                return EINVAL;
            if (target.type == NANOARROW_TYPE_FIXED_SIZE_LIST && end - start != target.fixed_size)
                return EINVAL;
            for (int64_t i = start; i < end; ++i)
                NANOARROW_RETURN_NOT_OK(VgiArrayAppendValue(out->children[0], schema->children[0],
                                                            value->children[0],
                                                            value_schema->children[0], i, error));
            return ArrowArrayFinishElement(out);
        }
        default:
            break;
    }
    if (VgiValueIsNumeric(value)) {
        switch (target.storage_type) {
            case NANOARROW_TYPE_HALF_FLOAT:
            case NANOARROW_TYPE_FLOAT:
            case NANOARROW_TYPE_DOUBLE:
                return ArrowArrayAppendDouble(out, ArrowArrayViewGetDoubleUnsafe(value, index));
            case NANOARROW_TYPE_UINT8:
            case NANOARROW_TYPE_UINT16:
            case NANOARROW_TYPE_UINT32:
            case NANOARROW_TYPE_UINT64:
                if (value->storage_type == NANOARROW_TYPE_FLOAT ||
                    value->storage_type == NANOARROW_TYPE_DOUBLE) {
                    double number = ArrowArrayViewGetDoubleUnsafe(value, index);
                    if (!isfinite(number) || number < 0 || number >= 18446744073709551616.0 ||
                        trunc(number) != number)
                        return EINVAL;
                    return ArrowArrayAppendUInt(out, (uint64_t)number);
                }
                if (value->storage_type != NANOARROW_TYPE_UINT64 &&
                    ArrowArrayViewGetIntUnsafe(value, index) < 0)
                    return EINVAL;
                return ArrowArrayAppendUInt(out, ArrowArrayViewGetUIntUnsafe(value, index));
            case NANOARROW_TYPE_BOOL:
            case NANOARROW_TYPE_INT8:
            case NANOARROW_TYPE_INT16:
            case NANOARROW_TYPE_INT32:
            case NANOARROW_TYPE_INT64:
                if (value->storage_type == NANOARROW_TYPE_FLOAT ||
                    value->storage_type == NANOARROW_TYPE_DOUBLE) {
                    double number = ArrowArrayViewGetDoubleUnsafe(value, index);
                    if (!isfinite(number) || number < -9223372036854775808.0 ||
                        number >= 9223372036854775808.0 || trunc(number) != number)
                        return EINVAL;
                    return ArrowArrayAppendInt(out, (int64_t)number);
                }
                if (value->storage_type == NANOARROW_TYPE_UINT64 &&
                    ArrowArrayViewGetUIntUnsafe(value, index) > INT64_MAX)
                    return EINVAL;
                return ArrowArrayAppendInt(out, ArrowArrayViewGetIntUnsafe(value, index));
            default:
                break;
        }
    }
    if (strcmp(schema->format, value_schema->format)) {
        ArrowErrorSet(error, "Cannot cast Arrow format %s to %s", value_schema->format,
                      schema->format);
        return EINVAL;
    }
    struct ArrowArrayView slice = *value;
    slice.offset += index;
    slice.length = 1;
    slice.null_count = 0;
    return ArrowArrayAppendStorageFromArrayView(out, &slice, error);
}
int VgiBatchCoerce(const struct VgiBatch *input, const struct ArrowSchema *schema,
                   struct VgiBatch *out, struct ArrowError *error) {
    int rc = VgiBatchInit(out, schema, error);
    for (int64_t i = 0; !rc && i < input->array.length; ++i)
        rc = VgiArrayAppendValue(&out->array, schema, &input->view, &input->schema, i, error);
    if (!rc) rc = VgiBatchFinish(out, error);
    if (rc) VgiBatchReset(out);
    return rc;
}
