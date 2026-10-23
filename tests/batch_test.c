/* VgiValueDouble across Arrow types, and VgiBatchSetBytes on plain and
 * dictionary-encoded binary columns. */
#include "vgi_rpc/batch.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x)                                                    \
    do {                                                            \
        if (!(x)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); \
            abort();                                                \
        }                                                           \
    } while (0)

static struct ArrowError error;

/* Leave nonzero garbage where the next call's frame lands, so a read of an
 * uninitialized ArrowSchemaView field shows. */
static void __attribute__((noinline)) dirty_stack(void) {
    volatile unsigned char junk[4096];
    for (size_t i = 0; i < sizeof(junk); ++i) junk[i] = 0xA5;
}
static int value_double(const struct ArrowArrayView *view, const struct ArrowSchema *schema,
                        double *out) {
    dirty_stack();
    return VgiValueDouble(view, schema, 0, out, &error);
}
enum Kind { MAP, ARRAY, INTERVAL, INTERVAL_MONTHS, STRUCT, TEXT, DECIMAL, INT };
static void one_value(enum Kind kind, struct ArrowSchema *schema, struct ArrowArray *array) {
    ArrowSchemaInit(schema);
    switch (kind) {
        case MAP:
            CHECK(!ArrowSchemaSetType(schema, NANOARROW_TYPE_MAP));
            CHECK(!ArrowSchemaSetType(schema->children[0]->children[0], NANOARROW_TYPE_STRING));
            CHECK(!ArrowSchemaSetType(schema->children[0]->children[1], NANOARROW_TYPE_INT64));
            break;
        case ARRAY:
            CHECK(!ArrowSchemaSetTypeFixedSize(schema, NANOARROW_TYPE_FIXED_SIZE_LIST, 2));
            CHECK(!ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT32));
            break;
        case INTERVAL:
            CHECK(!ArrowSchemaSetType(schema, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO));
            break;
        case INTERVAL_MONTHS:
            CHECK(!ArrowSchemaSetType(schema, NANOARROW_TYPE_INTERVAL_MONTHS));
            break;
        case STRUCT:
            CHECK(!ArrowSchemaSetTypeStruct(schema, 1));
            CHECK(!ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT64));
            CHECK(!ArrowSchemaSetName(schema->children[0], "a"));
            break;
        case TEXT:
            CHECK(!ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING));
            break;
        case DECIMAL:
            CHECK(!ArrowSchemaSetTypeDecimal(schema, NANOARROW_TYPE_DECIMAL128, 10, 2));
            break;
        case INT:
            CHECK(!ArrowSchemaSetType(schema, NANOARROW_TYPE_INT32));
            break;
    }
    CHECK(!ArrowArrayInitFromSchema(array, schema, &error));
    CHECK(!ArrowArrayStartAppending(array));
    switch (kind) {
        case MAP:
            CHECK(!ArrowArrayAppendString(array->children[0]->children[0], ArrowCharView("k")));
            CHECK(!ArrowArrayAppendInt(array->children[0]->children[1], 7));
            CHECK(!ArrowArrayFinishElement(array->children[0]));
            CHECK(!ArrowArrayFinishElement(array));
            break;
        case ARRAY:
            CHECK(!ArrowArrayAppendInt(array->children[0], 1));
            CHECK(!ArrowArrayAppendInt(array->children[0], 2));
            CHECK(!ArrowArrayFinishElement(array));
            break;
        case INTERVAL:
        case INTERVAL_MONTHS: {
            struct ArrowInterval interval;
            ArrowIntervalInit(&interval, kind == INTERVAL ? NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO
                                                          : NANOARROW_TYPE_INTERVAL_MONTHS);
            interval.months = 1;
            CHECK(!ArrowArrayAppendInterval(array, &interval));
            break;
        }
        case STRUCT:
            CHECK(!ArrowArrayAppendInt(array->children[0], 3));
            CHECK(!ArrowArrayFinishElement(array));
            break;
        case TEXT:
            CHECK(!ArrowArrayAppendString(array, ArrowCharView("1.5")));
            break;
        case DECIMAL: {
            struct ArrowDecimal decimal;
            ArrowDecimalInit(&decimal, 128, 10, 2);
            ArrowDecimalSetInt(&decimal, 1234);
            CHECK(!ArrowArrayAppendDecimal(array, &decimal));
            break;
        }
        case INT:
            CHECK(!ArrowArrayAppendInt(array, 42));
            break;
    }
    CHECK(!ArrowArrayFinishBuildingDefault(array, &error));
}
static void check_value_double(void) {
    struct {
        enum Kind kind;
        int rc;
        double expected;
    } cases[] = {{MAP, EINVAL, 0},      {ARRAY, EINVAL, 0},
                 {INTERVAL, EINVAL, 0}, {INTERVAL_MONTHS, EINVAL, 0},
                 {STRUCT, EINVAL, 0},   {TEXT, 0, 1.5},
                 {DECIMAL, 0, 12.34},   {INT, 0, 42}};
    for (size_t i = 0; i < sizeof(cases) / sizeof(*cases); ++i) {
        struct ArrowSchema schema;
        struct ArrowArray array;
        struct ArrowArrayView view;
        one_value(cases[i].kind, &schema, &array);
        CHECK(!ArrowArrayViewInitFromSchema(&view, &schema, &error));
        CHECK(!ArrowArrayViewSetArray(&view, &array, &error));
        double value = -1;
        int rc = value_double(&view, &schema, &value);
        if (rc != cases[i].rc) fprintf(stderr, "case %zu: rc %d\n", i, rc);
        CHECK(rc == cases[i].rc);
        if (!rc) CHECK(value == cases[i].expected);
        ArrowArrayViewReset(&view);
        ArrowArrayRelease(&array);
        ArrowSchemaRelease(&schema);
    }
}
static void check_set_bytes(void) {
    struct ArrowSchema schema;
    ArrowSchemaInit(&schema);
    CHECK(!ArrowSchemaSetTypeStruct(&schema, 2));
    CHECK(!ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_BINARY));
    CHECK(!ArrowSchemaSetName(schema.children[0], "plain"));
    CHECK(!ArrowSchemaSetType(schema.children[1], NANOARROW_TYPE_INT32));
    CHECK(!ArrowSchemaSetName(schema.children[1], "encoded"));
    CHECK(!ArrowSchemaAllocateDictionary(schema.children[1]));
    CHECK(!ArrowSchemaInitFromType(schema.children[1]->dictionary, NANOARROW_TYPE_BINARY));
    struct VgiBatch batch = {0};
    CHECK(!VgiBatchInit(&batch, &schema, &error));
    static const unsigned char bytes[] = {0, 1, 2, 0xff};
    CHECK(!VgiBatchSetBytes(&batch, "plain", bytes, sizeof(bytes)));
    CHECK(!VgiBatchSetBytes(&batch, "encoded", bytes, sizeof(bytes)));
    CHECK(VgiBatchSetBytes(&batch, "missing", bytes, sizeof(bytes)) == ENOENT);
    CHECK(!VgiBatchFinishRow(&batch, &error));
    const char *names[] = {"plain", "encoded"};
    for (int i = 0; i < 2; ++i) {
        struct ArrowBufferView value = VgiBatchBytes(&batch, names[i]);
        CHECK(value.size_bytes == (int64_t)sizeof(bytes));
        CHECK(!memcmp(value.data.data, bytes, sizeof(bytes)));
    }
    VgiBatchReset(&batch);
    ArrowSchemaRelease(&schema);
}
int main(void) {
    check_value_double();
    check_set_bytes();
    puts("VgiValueDouble types and VgiBatchSetBytes dictionaries passed");
    return 0;
}
