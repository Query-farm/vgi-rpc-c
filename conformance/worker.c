/* Copyright 2026 Query Farm LLC. SPDX-License-Identifier: Apache-2.0 */
#include <errno.h>
#include <inttypes.h>
#include <json-c/json.h>
#include <math.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "protocol.h"
#include "storage.h"
#include "vgi_rpc/http.h"

int ConformanceRichHeader(struct VgiBatch *, int64_t, struct ArrowError *);
int ConformancePackNested(struct VgiRpcCall *, struct VgiRpcReply *);
int ConformanceIdentityConfigure(struct VgiRpcServer *, struct VgiHttpOptions *, const char *,
                                 struct ArrowError *);
int ConformanceRejectAuth(void *, const struct VgiHttpRequest *, struct VgiHttpIdentity *,
                          struct ArrowError *);
static _Atomic int64_t probes[3];
int ConformanceSecondaryRegister(struct VgiRpcServer *, struct ArrowError *);
static int named(struct VgiBatch *out, const char *name, struct ArrowError *error) {
    struct ArrowSchema schema = {0};
    int rc = ConformanceSchemaInit(name, &schema, error);
    if (!rc) rc = VgiBatchInit(out, &schema, error);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
static int result_init(struct VgiRpcCall *call, struct VgiBatch *out) {
    struct ArrowSchema schema = {0};
    int rc = call->method->result(&schema, &call->error);
    if (!rc) rc = VgiBatchInit(out, &schema, &call->error);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
static char *string(struct ArrowStringView v) {
    char *s = malloc((size_t)v.size_bytes + 1);
    if (s) {
        memcpy(s, v.data, (size_t)v.size_bytes);
        s[v.size_bytes] = 0;
    }
    return s;
}
static int copy_value(struct ArrowArray *out, const struct ArrowSchema *schema,
                      const struct VgiBatch *input, int column, int64_t row, struct ArrowError *e) {
    return VgiArrayAppendValue(out, schema, input->view.children[column],
                               input->schema.children[column], row, e);
}
static int make_schema(struct ArrowSchema *out, int n, const char **names, const char **formats) {
    ArrowSchemaInit(out);
    int rc = ArrowSchemaSetTypeStruct(out, n);
    for (int i = 0; !rc && i < n; ++i) {
        rc = ArrowSchemaSetFormat(out->children[i], formats[i]);
        if (!rc) rc = ArrowSchemaSetName(out->children[i], names[i]);
    }
    return rc;
}
static int log_value(struct VgiRpcCall *c, const char *level, const char *prefix,
                     const char *extra) {
    struct ArrowStringView value = VgiBatchString(&c->request, "value");
    struct ArrowBuffer b;
    ArrowBufferInit(&b);
    int rc = ArrowBufferAppend(&b, prefix, (int64_t)strlen(prefix));
    if (!rc) rc = ArrowBufferAppend(&b, value.data, value.size_bytes);
    if (!rc) rc = ArrowBufferAppend(&b, "", 1);
    if (!rc) rc = VgiRpcLogExtra(c, level, (char *)b.data, extra);
    ArrowBufferReset(&b);
    return rc;
}
struct StreamState {
    const char *name;
    struct ArrowSchema schema;
    int64_t count, current, rows, fail_on;
    double factor, sum;
    int dynamic_strings, dynamic_floats;
};
static void stream_release(void *data) {
    struct StreamState *s = data;
    if (s->schema.release) ArrowSchemaRelease(&s->schema);
    free(s);
}
static void stream_cancel(void *state, struct VgiRpcCall *call) {
    (void)state;
    (void)call;
    atomic_fetch_add(&probes[2], 1);
}
static int compare_text(const void *a, const void *b) {
    return strcmp(*(char *const *)a, *(char *const *)b);
}
static int metadata_keys(const char *metadata, struct ArrowBuffer *out) {
    if (!metadata) return 0;
    struct ArrowMetadataReader reader;
    int rc = ArrowMetadataReaderInit(&reader, metadata);
    int n = reader.remaining_keys;
    char **keys = calloc((size_t)n + 1, sizeof(char *));
    if (!keys) return ENOMEM;
    for (int i = 0; !rc && i < n; ++i) {
        struct ArrowStringView key, value;
        rc = ArrowMetadataReaderRead(&reader, &key, &value);
        if (!rc && !(keys[i] = string(key))) rc = ENOMEM;
    }
    if (!rc) qsort(keys, (size_t)n, sizeof(char *), compare_text);
    for (int i = 0; !rc && i < n; ++i) {
        if (i) rc = ArrowBufferAppend(out, ",", 1);
        if (!rc) rc = ArrowBufferAppend(out, keys[i], (int64_t)strlen(keys[i]));
    }
    for (int i = 0; i < n; ++i) free(keys[i]);
    free(keys);
    return rc;
}
static int stream_next(void *data, struct VgiRpcCall *c, const struct VgiBatch *input,
                       const char *metadata, struct VgiBatch *out, struct ArrowBuffer *md,
                       int *finished) {
    struct StreamState *s = data;
    const char *name = s->name;
    int exchange = !strncmp(name, "exchange_", 9) || !strcmp(name, "cancellable_exchange");
    if (!strcmp(name, "produce_error_mid_stream") && s->current >= s->count) {
        char message[128];
        snprintf(message, sizeof(message), "intentional error after %" PRId64 " batches", s->count);
        return VgiRpcFail(c, "RuntimeError", "UNKNOWN", "", message);
    }
    if (!exchange && s->current >= s->count) {
        *finished = 1;
        return 0;
    }
    if (!strcmp(name, "exchange_error_on_nth") && s->current + 1 >= s->fail_on) {
        char message[128];
        snprintf(message, sizeof(message), "intentional error on exchange %" PRId64,
                 s->current + 1);
        return VgiRpcFail(c, "RuntimeError", "UNKNOWN", "", message);
    }
    if (!strcmp(name, "cancellable_producer")) atomic_fetch_add(&probes[0], 1);
    if (!strcmp(name, "cancellable_exchange")) atomic_fetch_add(&probes[1], 1);
    int rc = VgiBatchInit(out, &s->schema, &c->error);
    if (!rc && !strcmp(name, "produce_with_logs")) {
        char message[96];
        snprintf(message, sizeof(message), "producing batch %" PRId64, s->current);
        rc = VgiRpcLog(c, "INFO", message);
    }
    if (!rc && !strcmp(name, "exchange_with_logs")) {
        rc = VgiRpcLog(c, "INFO", "exchange processing");
        if (!rc) rc = VgiRpcLog(c, "DEBUG", "exchange debug");
    }
    if (!rc &&
        (!strcmp(name, "stream_session_counter") || !strcmp(name, "exchange_session_counter"))) {
        int64_t *counter = VgiRpcSessionGet(c);
        if (!counter)
            return VgiRpcFail(c, "RuntimeError", "UNKNOWN", "",
                              "no sticky counter bound to this request");
        if (exchange) {
            const struct ArrowArrayView *by = VgiBatchGet(input, "by");
            if (!by)
                return VgiRpcFail(c, "TypeError", "INVALID_ARGUMENT", "",
                                  "Missing input column by");
            for (int64_t i = 0; !rc && i < input->array.length; ++i) {
                *counter += ArrowArrayViewGetIntUnsafe(by, i);
                rc = ArrowArrayAppendInt(out->array.children[0], *counter);
                if (!rc) rc = ArrowArrayFinishElement(&out->array);
            }
        } else {
            ++*counter;
            rc = ArrowArrayAppendInt(out->array.children[0], *counter);
            if (!rc) rc = ArrowArrayFinishElement(&out->array);
        }
    } else if (!rc && !strcmp(name, "exchange_input_metadata")) {
        struct ArrowStringView seen = VgiMetadataGet(metadata, "vgi.conformance.input");
        struct ArrowBuffer keys;
        ArrowBufferInit(&keys);
        rc = metadata_keys(metadata, &keys);
        if (!rc)
            rc = ArrowArrayAppendString(out->array.children[0],
                                        seen.data ? seen : ArrowCharView(""));
        if (!rc)
            rc = ArrowArrayAppendString(
                out->array.children[1],
                (struct ArrowStringView){(char *)keys.data, keys.size_bytes});
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
        ArrowBufferReset(&keys);
    } else if (!rc && !strcmp(name, "exchange_accumulate")) {
        const struct ArrowArrayView *value = VgiBatchGet(input, "value");
        for (int64_t row = 0; value && row < input->array.length; ++row)
            if (!ArrowArrayViewIsNull(value, row))
                s->sum += ArrowArrayViewGetDoubleUnsafe(value, row);
        rc = ArrowArrayAppendDouble(out->array.children[0], s->sum);
        if (!rc) rc = ArrowArrayAppendInt(out->array.children[1], s->current + 1);
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
    } else if (!rc && !strcmp(name, "exchange_zero_columns")) {
        /* A zero-row batch is still an exchange response. */
    } else if (!rc && exchange && strcmp(name, "exchange_oversized")) {
        const struct ArrowArrayView *value = VgiBatchGet(input, "value");
        if (!value)
            return VgiRpcFail(c, "TypeError", "INVALID_ARGUMENT", "", "Missing input column value");
        for (int64_t row = 0; !rc && row < input->array.length; ++row) {
            if (ArrowArrayViewIsNull(value, row))
                rc = ArrowArrayAppendNull(out->array.children[0], 1);
            else {
                double number;
                rc = VgiValueDouble(value,
                                    input->schema.children[VgiFieldIndex(&input->schema, "value")],
                                    row, &number, &c->error);
                if (!rc) rc = ArrowArrayAppendDouble(out->array.children[0], number * s->factor);
            }
            if (!rc) rc = ArrowArrayFinishElement(&out->array);
        }
    } else if (!rc && !strcmp(name, "produce_tick_metadata")) {
        struct ArrowStringView seen = VgiMetadataGet(metadata, "vgi.conformance.tick");
        rc = ArrowArrayAppendInt(out->array.children[0], s->current);
        if (!rc)
            rc = ArrowArrayAppendString(out->array.children[1],
                                        seen.data ? seen : ArrowCharView(""));
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
    } else if (!rc && !strcmp(name, "produce_dynamic_schema")) {
        rc = ArrowArrayAppendInt(out->array.children[0], s->current);
        int column = 1;
        if (!rc && s->dynamic_strings) {
            char label[80];
            snprintf(label, sizeof(label), "row-%" PRId64, s->current);
            rc = ArrowArrayAppendString(out->array.children[column++], ArrowCharView(label));
        }
        if (!rc && s->dynamic_floats)
            rc = ArrowArrayAppendDouble(out->array.children[column], s->current * 1.5);
        if (!rc) rc = ArrowArrayFinishElement(&out->array);
    } else if (!rc) {
        int annotated = !strcmp(name, "produce_annotated_batches");
        for (int64_t row = 0; !rc && row < s->rows; ++row) {
            int64_t index = s->current * s->rows + row;
            rc = ArrowArrayAppendInt(out->array.children[0],
                                     annotated ? s->current * 1000000 + row : index);
            if (!rc && !annotated) rc = ArrowArrayAppendInt(out->array.children[1], index * 10);
            if (!rc) rc = ArrowArrayFinishElement(&out->array);
        }
        if (!rc && annotated) {
            char index[32], total[32];
            snprintf(index, sizeof(index), "%" PRId64, s->current);
            snprintf(total, sizeof(total), "%" PRId64, s->count);
            rc = VgiMetadataSet(md, "conformance.batch_index", index);
            if (!rc) rc = VgiMetadataSet(md, "conformance.batch_total", total);
            if (!rc) rc = VgiMetadataSet(md, "conformance.emit_label", "ünïcode-λ");
        }
    }
    ++s->current;
    return rc ? rc : VgiBatchFinish(out, &c->error);
}
static int stream_serialize(void *data, struct ArrowBuffer *out, struct ArrowError *error) {
    (void)error;
    struct StreamState *s = data;
    uint64_t sum;
    memcpy(&sum, &s->sum, sizeof(sum));
    uint8_t bytes[16];
    for (int i = 0; i < 8; ++i) {
        bytes[i] = (uint8_t)((uint64_t)s->current >> (i * 8));
        bytes[i + 8] = (uint8_t)(sum >> (i * 8));
    }
    return ArrowBufferAppend(out, bytes, sizeof(bytes));
}
static int init_stream(struct VgiRpcCall *c, struct VgiRpcReply *reply, int initial,
                       const struct ArrowSchema *resolved_schema) {
    const char *name = c->method->name;
    if (strstr(name, "error_on_init"))
        return VgiRpcFail(c, "RuntimeError", "UNKNOWN", "",
                          !strncmp(name, "exchange", 8) ? "intentional exchange init error"
                                                        : "intentional init error");
    struct StreamState *s = calloc(1, sizeof(*s));
    if (!s) return ENOMEM;
    s->name = name;
    s->factor = 1;
    s->rows = VgiBatchInt(&c->request, "rows_per_batch", 1);
    s->count = VgiBatchInt(&c->request, "count", VgiBatchInt(&c->request, "batch_count", 1));
    if (!strcmp(name, "produce_empty")) s->count = 0;
    if (!strcmp(name, "cancellable_producer")) s->count = INT64_MAX;
    if (!strcmp(name, "produce_error_mid_stream"))
        s->count = VgiBatchInt(&c->request, "emit_before_error", 0);
    s->fail_on = VgiBatchInt(&c->request, "fail_on", 0);
    const struct ArrowArrayView *factor = VgiBatchGet(&c->request, "factor");
    if (factor) s->factor = ArrowArrayViewGetDoubleUnsafe(factor, 0);
    int exchange = !strncmp(name, "exchange_", 9) || !strcmp(name, "cancellable_exchange");
    const char *schema = exchange ? "SCALE_OUTPUT_SCHEMA" : "COUNTER_SCHEMA";
    if (!strcmp(name, "exchange_accumulate")) schema = "ACCUM_OUTPUT_SCHEMA";
    if (!strcmp(name, "exchange_input_metadata")) schema = "INPUT_METADATA_OUTPUT_SCHEMA";
    if (!strcmp(name, "exchange_oversized")) schema = "COUNTER_SCHEMA";
    if (!strcmp(name, "produce_annotated_batches")) schema = "ANNOTATED_SCHEMA";
    if (!strcmp(name, "stream_session_counter") || !strcmp(name, "exchange_session_counter"))
        schema = "SESSION_COUNTER_OUTPUT_SCHEMA";
    int rc = 0;
    if (!strcmp(name, "exchange_zero_columns")) {
        ArrowSchemaInit(&s->schema);
        rc = ArrowSchemaSetTypeStruct(&s->schema, 0);
    } else if (!strcmp(name, "produce_tick_metadata")) {
        const char *names[] = {"index", "seen"}, *formats[] = {"l", "u"};
        rc = make_schema(&s->schema, 2, names, formats);
    } else if (!strcmp(name, "produce_dynamic_schema")) {
        s->dynamic_strings = (int)VgiBatchInt(&c->request, "include_strings", 0);
        s->dynamic_floats = (int)VgiBatchInt(&c->request, "include_floats", 0);
        const char *names[3] = {"index"}, *formats[3] = {"l"};
        int n = 1;
        if (s->dynamic_strings) {
            names[n] = "label";
            formats[n++] = "u";
        }
        if (s->dynamic_floats) {
            names[n] = "score";
            formats[n++] = "g";
        }
        rc = make_schema(&s->schema, n, names, formats);
    } else
        rc = ConformanceSchemaInit(schema, &s->schema, &c->error);
    if (!rc && resolved_schema) {
        ArrowSchemaRelease(&s->schema);
        rc = ArrowSchemaDeepCopy(resolved_schema, &s->schema);
    }
    if (!rc) rc = ArrowSchemaDeepCopy(&s->schema, &reply->stream.output_schema);
    reply->stream.is_producer = !exchange;
    reply->stream.next = stream_next;
    reply->stream.release = stream_release;
    reply->stream.state = s;
    reply->stream.serialize = stream_serialize;
    if (!strncmp(name, "cancellable_", 12)) reply->stream.on_cancel = stream_cancel;
    if (!rc && initial && c->method->header &&
        (strstr(name, "rich_header") || !strcmp(name, "produce_dynamic_schema"))) {
        rc = ConformanceRichHeader(&reply->stream.header, VgiBatchInt(&c->request, "seed", 0),
                                   &c->error);
    } else if (!rc && initial && c->method->header) {
        rc = c->method->header(&reply->stream.header.schema, &c->error);
        if (!rc)
            rc = ArrowArrayInitFromSchema(&reply->stream.header.array, &reply->stream.header.schema,
                                          &c->error);
        if (!rc) rc = ArrowArrayStartAppending(&reply->stream.header.array);
        char message[160];
        if (exchange)
            snprintf(message, sizeof(message), "scale by %.1f", s->factor);
        else
            snprintf(message, sizeof(message), "producing %" PRId64 " %s", s->count,
                     strstr(name, "and_logs") ? "with logs" : "batches");
        if (!rc)
            rc = VgiBatchSetInt(&reply->stream.header, "total_expected", exchange ? 0 : s->count);
        if (!rc) rc = VgiBatchSetString(&reply->stream.header, "description", message);
        if (!rc) rc = VgiBatchFinishRow(&reply->stream.header, &c->error);
        if (!rc && strstr(name, "and_logs")) rc = VgiRpcLog(c, "INFO", "stream init log");
    }
    return rc;
}
static int resume(void *state, struct VgiRpcCall *c, struct ArrowBufferView serialized,
                  const struct ArrowSchema *schema, struct VgiRpcReply *reply) {
    (void)state;
    if (serialized.size_bytes != 16) return EINVAL;
    uint64_t current = 0, sum = 0;
    for (int i = 0; i < 8; ++i) {
        current |= (uint64_t)serialized.data.as_uint8[i] << (i * 8);
        sum |= (uint64_t)serialized.data.as_uint8[i + 8] << (i * 8);
    }
    if (current > INT64_MAX) return EINVAL;
    int rc = init_stream(c, reply, 0, schema);
    if (!rc) {
        struct StreamState *s = reply->stream.state;
        s->current = (int64_t)current;
        memcpy(&s->sum, &sum, sizeof(sum));
    }
    return rc;
}
static int dispatch(void *state, struct VgiRpcCall *c, struct VgiRpcReply *reply) {
    const char *name = c->method->name;
    if (!strcmp(name, "published_string")) return ConformancePublished(state, c, reply);
    if (!strcmp(name, "report_transport_kind")) {
        int rc = result_init(c, &reply->value);
        if (!rc)
            rc =
                VgiBatchSetString(&reply->value, "result", VgiTransportKindName(c->transport_kind));
        return rc ? rc : VgiBatchFinishRow(&reply->value, &c->error);
    }
    if (c->method->kind == VGI_METHOD_STREAM) return init_stream(c, reply, 1, NULL);
    if (!strcmp(name, "reset_cancel_probe")) {
        for (int i = 0; i < 3; ++i) atomic_store(&probes[i], 0);
        return 0;
    }
    if (!strncmp(name, "void_", 5)) return 0;
    if (!strncmp(name, "raise_", 6)) {
        char *message = string(VgiBatchString(&c->request, "message"));
        if (!message) return ENOMEM;
        const char *type = !strcmp(name, "raise_value_error")  ? "ValueError"
                           : !strcmp(name, "raise_type_error") ? "TypeError"
                                                               : "RuntimeError";
        int rc = VgiRpcFail(c, type, "UNKNOWN", "", message);
        free(message);
        return rc;
    }
    if (!strcmp(name, "pack_nested_containers")) return ConformancePackNested(c, reply);
    int rc = result_init(c, &reply->value);
    if (rc) return rc;
    struct ArrowArray *out = reply->value.array.children[0];
    if (!strcmp(name, "open_counter")) {
        int64_t *counter = malloc(sizeof(*counter));
        if (!counter) return ENOMEM;
        *counter = VgiBatchInt(&c->request, "initial", 0);
        rc = VgiRpcSessionOpen(c, counter, free, 0);
        if (rc)
            free(counter);
        else
            rc = ArrowArrayAppendInt(out, *counter);
    } else if (!strcmp(name, "increment_counter") || !strcmp(name, "close_counter")) {
        int64_t *counter = VgiRpcSessionGet(c);
        if (!counter)
            return VgiRpcFail(c, "RuntimeError", "UNKNOWN", "",
                              "no sticky counter bound to this request");
        if (!strcmp(name, "increment_counter")) *counter += VgiBatchInt(&c->request, "by", 0);
        rc = ArrowArrayAppendInt(out, *counter);
        if (!rc && !strcmp(name, "close_counter")) rc = VgiRpcSessionClose(c);
    } else if (!strcmp(name, "cancel_probe_counters")) {
        for (int i = 0; !rc && i < 3; ++i)
            rc = ArrowArrayAppendInt(out->children[0], atomic_load(&probes[i]));
        if (!rc) rc = ArrowArrayFinishElement(out);
    } else if (!strncmp(name, "echo_", 5)) {
        if (!strcmp(name, "echo_with_info_log")) rc = log_value(c, "INFO", "info: ", NULL);
        if (!strcmp(name, "echo_with_multi_logs")) {
            rc = log_value(c, "DEBUG", "debug: ", NULL);
            if (!rc) rc = log_value(c, "INFO", "info: ", NULL);
            if (!rc) rc = log_value(c, "WARN", "warn: ", NULL);
        }
        if (!strcmp(name, "echo_with_all_log_levels")) {
            const char *levels[] = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR"},
                       *prefixes[] = {"trace: ", "debug: ", "info: ", "warn: ", "error: "};
            for (int i = 0; !rc && i < 5; ++i) rc = log_value(c, levels[i], prefixes[i], NULL);
        }
        if (!strcmp(name, "echo_with_log_extras")) {
            struct json_object *extra = json_object_new_object();
            struct ArrowStringView v = VgiBatchString(&c->request, "value");
            json_object_object_add(extra, "source", json_object_new_string("conformance"));
            json_object_object_add(extra, "detail",
                                   json_object_new_string_len(v.data, (int)v.size_bytes));
            rc = log_value(c, "INFO",
                           "info: ", json_object_to_json_string_ext(extra, JSON_C_TO_STRING_PLAIN));
            json_object_put(extra);
        }
        if (!rc)
            rc = copy_value(out, reply->value.schema.children[0], &c->request, 0, 0, &c->error);
    } else if (!strcmp(name, "add_floats")) {
        double a = ArrowArrayViewGetDoubleUnsafe(VgiBatchGet(&c->request, "a"), 0),
               b = ArrowArrayViewGetDoubleUnsafe(VgiBatchGet(&c->request, "b"), 0);
        rc = ArrowArrayAppendDouble(out, a + b);
    } else if (!strcmp(name, "concatenate")) {
        struct ArrowBuffer b;
        ArrowBufferInit(&b);
        const char *fields[] = {"prefix", "separator", "suffix"};
        for (int i = 0; !rc && i < 3; ++i) {
            struct ArrowStringView v = VgiBatchString(&c->request, fields[i]);
            if (i == 1 && !v.data) v = ArrowCharView("-");
            rc = ArrowBufferAppend(&b, v.data, v.size_bytes);
        }
        if (!rc)
            rc =
                ArrowArrayAppendString(out, (struct ArrowStringView){(char *)b.data, b.size_bytes});
        ArrowBufferReset(&b);
    } else if (!strcmp(name, "with_defaults")) {
        struct ArrowStringView v = VgiBatchString(&c->request, "optional_str");
        char *optional = string(v.data ? v : ArrowCharView("default"));
        if (!optional) return ENOMEM;
        size_t size = strlen(optional) + 160;
        char *message = malloc(size);
        if (message) {
            snprintf(message, size, "required=%" PRId64 ", optional_str=%s, optional_int=%" PRId64,
                     VgiBatchInt(&c->request, "required", 0), optional,
                     VgiBatchInt(&c->request, "optional_int", 42));
            rc = ArrowArrayAppendString(out, ArrowCharView(message));
        } else
            rc = ENOMEM;
        free(message);
        free(optional);
    } else if (!strcmp(name, "inspect_point")) {
        struct VgiBatch point = {0};
        rc = VgiBatchChild(&c->request, "point", &point, &c->error);
        if (!rc) {
            char message[200];
            snprintf(message, sizeof(message), "Point(%.1f, %.1f)",
                     ArrowArrayViewGetDoubleUnsafe(VgiBatchGet(&point, "x"), 0),
                     ArrowArrayViewGetDoubleUnsafe(VgiBatchGet(&point, "y"), 0));
            rc = ArrowArrayAppendString(out, ArrowCharView(message));
        }
        VgiBatchReset(&point);
    } else if (!strcmp(name, "oversized_unary")) {
        int64_t size = VgiBatchInt(&c->request, "target_bytes", 0);
        if (size < 0)
            return VgiRpcFail(c, "ValueError", "INVALID_ARGUMENT", "",
                              "target_bytes must be non-negative");
        void *bytes = calloc((size_t)size + 1, 1);
        if (!bytes) return ENOMEM;
        rc = ArrowArrayAppendBytes(out, (struct ArrowBufferView){{bytes}, size});
        free(bytes);
    } else
        return VgiRpcFail(c, "NotImplementedError", "UNIMPLEMENTED", "",
                          "Conformance fixture pending implementation");
    return rc ? rc : VgiBatchFinishRow(&reply->value, &c->error);
}
static int fail_start(void *state, enum VgiTransportKind kind, struct ArrowError *error) {
    (void)kind;
    int *calls = state;
    if (!(*calls)++) {
        ArrowErrorSet(error, "conformance: first serve-start hook failed");
        return EIO;
    }
    return 0;
}
static int probe_params(struct ArrowSchema *s, struct ArrowError *e) {
    (void)e;
    ArrowSchemaInit(s);
    return ArrowSchemaSetTypeStruct(s, 0);
}
static int probe_result(struct ArrowSchema *s, struct ArrowError *e) {
    (void)e;
    const char *names[] = {"result"}, *formats[] = {"u"};
    int rc = make_schema(s, 1, names, formats);
    if (!rc) s->children[0]->flags = 0;
    return rc;
}
static int control(void *state, const char *path, int deleting, struct VgiRpcCall *call) {
    (void)state;
    if (strcmp(path, "/__test_drain__")) return ENOENT;
    return VgiRpcSessionsSetDraining(call, !deleting);
}
static int sticky_auth(void *state, const struct VgiHttpRequest *request,
                       struct VgiHttpIdentity *identity, struct ArrowError *error) {
    (void)state;
    (void)error;
    identity->principal = request->header(request->state, "X-Conformance-Principal");
    identity->domain = identity->principal ? "conformance" : "";
    return 0;
}
int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    struct VgiRpcServer *server = VgiRpcServerNew();
    struct ArrowError error = {{0}};
    if (!server) return 1;
    struct ConformanceStorage storage;
    ConformanceStorageInit(&storage);
    struct VgiRpcProtocol protocol = {VGI_CONFORMANCE_NAME,
                                      VGI_CONFORMANCE_VERSION,
                                      VGI_CONFORMANCE_HASH,
                                      VGI_CONFORMANCE_METHODS,
                                      VGI_CONFORMANCE_METHOD_COUNT,
                                      dispatch,
                                      &storage,
                                      resume};
    struct VgiMethod *probe_methods = NULL;
    for (int i = 1; i < argc; ++i)
        if (!strcmp(argv[i], "--transport-kind-probe")) {
            probe_methods = calloc(protocol.method_count + 1, sizeof(*probe_methods));
            if (!probe_methods) {
                VgiRpcServerFree(server);
                return 1;
            }
            memcpy(probe_methods, protocol.methods, protocol.method_count * sizeof(*probe_methods));
            probe_methods[protocol.method_count++] = (struct VgiMethod){
                "report_transport_kind", VGI_METHOD_UNARY, probe_params, probe_result, NULL, NULL};
            protocol.methods = probe_methods;
            protocol.version = "";
        }
    int start_calls = 0;
    int rc = VgiRpcServerAddProtocol(server, &protocol, &error);
    if (!rc) rc = ConformanceSecondaryRegister(server, &error);
    const char *unix_path = NULL, *tcp_address = NULL;
    int http = 0;
    struct VgiHttpOptions options;
    VgiHttpOptionsInit(&options);
    int explicit_request_cap = 0;
    const char *echo_names[] = {"x-vgi-conformance-echo"},
               *echo_values[] = {"conformance-fixed-marker"};
    options.sticky_enabled = 1;
    options.sticky_echo_names = echo_names;
    options.sticky_echo_values = echo_values;
    options.sticky_echo_count = 1;
    options.control = control;
    for (int i = 1; !rc && i < argc; ++i) {
        if (!strcmp(argv[i], "--http"))
            http = 1;
        else if (!strcmp(argv[i], "--pipe") || !strcmp(argv[i], "--describe")) {
        } else if (!strcmp(argv[i], "--transport-kind-probe")) {
        } else if (!strcmp(argv[i], "--fail-serve-start-once"))
            VgiRpcServerSetStartHook(server, fail_start, &start_calls);
        else if (!strcmp(argv[i], "--auth-reject-all"))
            options.authenticate_request = ConformanceRejectAuth;
        else if (i + 1 < argc && !strcmp(argv[i], "--identity"))
            rc = ConformanceIdentityConfigure(server, &options, argv[++i], &error);
        else if (!strcmp(argv[i], "--access-log-debug")) {
        } else if (!strcmp(argv[i], "--no-tracebacks"))
            VgiRpcServerSetTracebacks(server, 0);
        else if (!strcmp(argv[i], "--no-compression"))
            options.compression = 0;
        else if (!strcmp(argv[i], "--sticky-auth"))
            options.authenticate_request = sticky_auth;
        else if (i + 1 < argc && !strcmp(argv[i], "--sticky-ttl"))
            options.sticky_ttl_seconds = (unsigned)strtoul(argv[++i], NULL, 10);
        else if (i + 1 < argc && !strcmp(argv[i], "--token-key"))
            options.token_key = argv[++i];
        else if (i + 1 < argc && !strcmp(argv[i], "--prefix"))
            options.prefix = argv[++i];
        else if (i + 1 < argc && !strcmp(argv[i], "--proof-mode")) {
            const char *mode = argv[++i];
            if (!strcmp(mode, "off"))
                options.proof.mode = VGI_PROOF_OFF;
            else if (!strcmp(mode, "allow"))
                options.proof.mode = VGI_PROOF_ALLOW;
            else if (!strcmp(mode, "require"))
                options.proof.mode = VGI_PROOF_REQUIRE;
            else
                rc = EINVAL;
        } else if (i + 1 < argc && !strcmp(argv[i], "--proof-origin-id"))
            options.proof.origin_id = argv[++i];
        else if (i + 1 < argc && !strcmp(argv[i], "--proof-secrets"))
            options.proof.secrets = argv[++i];
        else if (i + 1 < argc && !strcmp(argv[i], "--proof-skew"))
            options.proof.skew_seconds = (unsigned)strtoul(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--proof-no-replay-cache"))
            options.proof.replay_capacity = 0;
        else if (i + 1 < argc && !strcmp(argv[i], "--host"))
            options.host = argv[++i];
        else if (i + 1 < argc && !strcmp(argv[i], "--port"))
            options.port = (unsigned)strtoul(argv[++i], NULL, 10);
        else if (i + 1 < argc && !strcmp(argv[i], "--unix"))
            unix_path = argv[++i];
        else if (i + 1 < argc && !strcmp(argv[i], "--tcp"))
            tcp_address = argv[++i];
        else if (i + 1 < argc && !strcmp(argv[i], "--access-log"))
            options.access_log = argv[++i];
        else if (i + 1 < argc && !strcmp(argv[i], "--signing-key"))
            options.signing_key = argv[++i];
        else if (i + 1 < argc && !strcmp(argv[i], "--cors-origin"))
            options.cors_origin = argv[++i];
        else if (i + 1 < argc && !strcmp(argv[i], "--max-response-bytes"))
            options.max_response_bytes = strtoll(argv[++i], NULL, 10);
        else if (i + 1 < argc && !strcmp(argv[i], "--max-externalized-response-bytes"))
            options.max_externalized_response_bytes = strtoll(argv[++i], NULL, 10);
        else if (i + 1 < argc && !strcmp(argv[i], "--max-request-bytes")) {
            options.max_request_bytes = strtoll(argv[++i], NULL, 10);
            explicit_request_cap = 1;
        } else if (i + 1 < argc && !strcmp(argv[i], "--fake-storage"))
            storage.base_url = argv[++i];
        else if (i + 1 < argc && !strcmp(argv[i], "--externalize-threshold"))
            storage.options.threshold_bytes = strtoll(argv[++i], NULL, 10);
        else if (i + 1 < argc && !strcmp(argv[i], "--externalize-compression"))
            storage.options.compression = argv[++i];
        else if (i + 1 < argc && !strcmp(argv[i], "--max-fetch-bytes"))
            storage.options.max_fetch_bytes = strtoll(argv[++i], NULL, 10);
        else if (i + 1 < argc && !strcmp(argv[i], "--max-decompressed-fetch-bytes"))
            storage.options.max_decompressed_bytes = strtoll(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--no-call-state-cache"))
            options.require_stateless_streams = 1;
        else if (!strcmp(argv[i], "--reject-localhost-redirects"))
            storage.reject_localhost = 1;
        else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            rc = EINVAL;
        }
    }
    if (!rc && storage.base_url) {
        rc = VgiRpcServerSetExternal(server, &storage.options);
        if (!explicit_request_cap && storage.options.threshold_bytes > 0)
            options.max_request_bytes = storage.options.threshold_bytes;
    }
    struct VgiTelemetry *telemetry = NULL;
    if (!rc && getenv("VGI_RPC_OTEL_ENDPOINT")) {
        struct VgiTelemetryOptions otel;
        VgiTelemetryOptionsInit(&otel);
        otel.endpoint = getenv("VGI_RPC_OTEL_ENDPOINT");
        otel.service_name = "vgi-rpc-c-server";
        rc = VgiTelemetryNew(&otel, &telemetry, &error);
        if (!rc) rc = VgiRpcServerSetTelemetry(server, telemetry);
    }
    if (!rc)
        rc = http ? VgiRpcServerServeHttp(server, &options, &error)
             : unix_path || tcp_address
                 ? VgiRpcServerServeSocket(server, unix_path, tcp_address, 0, &error)
                 : VgiRpcServerServe(server, stdin, stdout, &error);
    if (rc) fprintf(stderr, "vgi-rpc-c: %s (error %d)\n", error.message, rc);
    VgiRpcServerFree(server);
    VgiTelemetryFree(telemetry);
    ConformanceStorageReset(&storage);
    free(probe_methods);
    return rc ? 1 : 0;
}
