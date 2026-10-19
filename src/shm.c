#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <sodium.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "shm_internal.h"

#define HEADER 65536
#define MAX_ALLOCS ((HEADER - 24) / 16)
#define OFFSET "vgi_rpc.shm_offset"
#define LENGTH "vgi_rpc.shm_length"
#define NAME "vgi_rpc.shm_segment_name"
#define SIZE "vgi_rpc.shm_segment_size"
struct VgiShm {
    uint8_t *data;
    int64_t size, threshold;
    char name[256];
    int owner;
    struct VgiShmStats stats;
};
static uint64_t get(const uint8_t *p, int n) {
    uint64_t v = 0;
    for (int i = 0; i < n; ++i) v |= (uint64_t)p[i] << (8 * i);
    return v;
}
static void put(uint8_t *p, uint64_t v, int n) {
    for (int i = 0; i < n; ++i) p[i] = (uint8_t)(v >> (8 * i));
}
static int validate(struct VgiShm *s) {
    if (memcmp(s->data, "VGIS", 4) || get(s->data + 4, 4) != 1 ||
        get(s->data + 8, 8) != (uint64_t)s->size - HEADER)
        return EINVAL;
    uint64_t count = get(s->data + 16, 4), end = HEADER;
    if (count > MAX_ALLOCS) return EINVAL;
    for (uint64_t i = 0; i < count; ++i) {
        uint8_t *p = s->data + 24 + 16 * i;
        uint64_t off = get(p, 8), len = get(p + 8, 8);
        if (off < end || off > (uint64_t)s->size || !len || len > (uint64_t)s->size - off)
            return EINVAL;
        end = off + len;
    }
    return 0;
}
static int mapping(const char *name, int64_t size, int create, struct VgiShm **out,
                   struct ArrowError *error) {
    if (!out || *out || size <= HEADER || (uint64_t)size > SIZE_MAX || !name || !*name)
        return EINVAL;
    if (*name == '/') ++name;
    if (!*name || strlen(name) >= 240 || strchr(name, '/')) return EINVAL;
    char path[256];
    snprintf(path, sizeof(path), "/%s", name);
    int fd = shm_open(path, O_RDWR | (create ? O_CREAT | O_EXCL : 0), 0600);
    if (fd < 0) return errno;
    fcntl(fd, F_SETFD, FD_CLOEXEC);
    int rc = create && ftruncate(fd, (off_t)size) ? errno : 0;
    struct stat st;
    if (!rc && fstat(fd, &st)) rc = errno;
    /* Never map past the actual object. Require a same-user, private segment. */
    if (!rc && (st.st_size < size || st.st_size <= HEADER || st.st_uid != geteuid() ||
                (st.st_mode & 0077)))
        rc = EINVAL;
    struct VgiShm *s = !rc ? calloc(1, sizeof(*s)) : NULL;
    if (!rc && !s) rc = ENOMEM;
    if (!rc) {
        s->size = st.st_size;
        s->threshold = 128 * 1024;
        const char *threshold = getenv("VGI_RPC_SHM_MIN_BATCH_BYTES");
        if (threshold && *threshold) {
            char *end;
            long long value = strtoll(threshold, &end, 10);
            if (!*end && value >= 0) s->threshold = value;
        }
        s->data = mmap(NULL, (size_t)s->size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (s->data == MAP_FAILED) {
            s->data = NULL;
            rc = errno;
        }
    }
    close(fd);
    if (!rc) {
        snprintf(s->name, sizeof(s->name), "%s", name);
        if (create) {
            memcpy(s->data, "VGIS", 4);
            put(s->data + 4, 1, 4);
            put(s->data + 8, (uint64_t)s->size - HEADER, 8);
            put(s->data + 16, 0, 8);
        }
        rc = validate(s);
    }
    if (rc) {
        if (s && s->data) munmap(s->data, (size_t)s->size);
        free(s);
        if (create) shm_unlink(path);
        ArrowErrorSet(error, "Invalid or unavailable shared-memory segment: %s", strerror(rc));
    } else {
        s->owner = create;
        *out = s;
    }
    return rc;
}
int VgiShmCreate(int64_t size, struct VgiShm **out, struct ArrowError *error) {
    if (sodium_init() < 0) return EIO;
    uint8_t random[8];
    char hex[17], name[32];
    randombytes_buf(random, sizeof(random));
    sodium_bin2hex(hex, sizeof(hex), random, sizeof(random));
    snprintf(name, sizeof(name), "vgic_%s", hex);
    return mapping(name, size, 1, out, error);
}
int VgiShmAttach(const char *name, int64_t size, struct VgiShm **out, struct ArrowError *e) {
    return mapping(name, size, 0, out, e);
}
void VgiShmFree(struct VgiShm *s) {
    if (!s) return;
    munmap(s->data, (size_t)s->size);
    if (s->owner) {
        char path[260];
        snprintf(path, sizeof(path), "/%s", s->name);
        shm_unlink(path);
    }
    free(s);
}
const char *VgiShmName(const struct VgiShm *s) { return s ? s->name : NULL; }
int64_t VgiShmSize(const struct VgiShm *s) { return s ? s->size : 0; }
void VgiShmSetThreshold(struct VgiShm *s, int64_t bytes) {
    if (s && bytes >= 0) s->threshold = bytes;
}
void VgiShmGetStats(const struct VgiShm *s, struct VgiShmStats *out) {
    if (out) *out = s ? s->stats : (struct VgiShmStats){0};
}
void VgiShmReset(struct VgiShm *s) {
    if (s) put(s->data + 16, 0, 4);
}
static int number(struct ArrowStringView v, uint64_t *out) {
    if (!v.data || !v.size_bytes || v.size_bytes > 20) return EINVAL;
    uint64_t n = 0;
    for (int64_t i = 0; i < v.size_bytes; ++i) {
        unsigned d = (unsigned)(v.data[i] - '0');
        if (d > 9 || n > (UINT64_MAX - d) / 10) return EINVAL;
        n = n * 10 + d;
    }
    *out = n;
    return 0;
}
int VgiShmAdvertise(struct VgiShm *s, struct ArrowBuffer *md) {
    if (!s) return 0;
    char size[32];
    snprintf(size, sizeof(size), "%" PRId64, s->size);
    int rc = VgiMetadataSet(md, NAME, s->name);
    if (!rc) rc = VgiMetadataSet(md, SIZE, size);
    return rc;
}
int VgiShmFromMetadata(const char *md, struct VgiShm **out, struct ArrowError *error) {
    struct ArrowStringView name = VgiMetadataGet(md, NAME);
    if (!name.data) return 0;
    char text[256];
    uint64_t size;
    if (name.size_bytes <= 0 || name.size_bytes >= (int64_t)sizeof(text) ||
        memchr(name.data, 0, (size_t)name.size_bytes) || number(VgiMetadataGet(md, SIZE), &size) ||
        size > INT64_MAX)
        return EINVAL;
    memcpy(text, name.data, (size_t)name.size_bytes);
    text[name.size_bytes] = 0;
    if (*out && !strcmp((*out)->name, text) && (*out)->size == (int64_t)size) return validate(*out);
    struct VgiShm *replacement = NULL;
    int rc = VgiShmAttach(text, (int64_t)size, &replacement, error);
    if (!rc) {
        VgiShmFree(*out);
        *out = replacement;
    }
    return rc;
}
static int allocate(struct VgiShm *s, uint64_t size, uint64_t *offset) {
    int rc = validate(s);
    if (rc) return rc;
    uint64_t count = get(s->data + 16, 4), prev = HEADER, i = 0;
    if (count == MAX_ALLOCS) return ENOSPC;
    for (; i < count; ++i) {
        uint8_t *p = s->data + 24 + i * 16;
        if (get(p, 8) - prev >= size) break;
        prev = get(p, 8) + get(p + 8, 8);
    }
    if (size > (uint64_t)s->size - prev) return ENOSPC;
    uint8_t *p = s->data + 24 + i * 16;
    memmove(p + 16, p, (size_t)(count - i) * 16);
    put(p, prev, 8);
    put(p + 8, size, 8);
    put(s->data + 16, count + 1, 4);
    *offset = prev;
    return 0;
}
static int region(struct VgiShm *s, uint64_t offset, uint64_t length, uint64_t *index) {
    int rc = validate(s);
    if (rc) return rc;
    uint64_t count = get(s->data + 16, 4);
    for (uint64_t i = 0; i < count; ++i) {
        uint8_t *p = s->data + 24 + i * 16;
        if (get(p, 8) == offset && length && length <= get(p + 8, 8)) {
            *index = i;
            return 0;
        }
    }
    return EINVAL;
}
static void release_region(struct VgiShm *s, uint64_t index) {
    uint64_t count = get(s->data + 16, 4);
    uint8_t *p = s->data + 24 + index * 16;
    memmove(p, p + 16, (size_t)(count - index - 1) * 16);
    put(s->data + 16, count - 1, 4);
}
static int dictionary(const struct ArrowSchema *s) {
    for (int64_t i = 0; i < s->n_children; ++i)
        if (s->children[i]->dictionary) return 1;
    return 0;
}
static int64_t view_size(const struct ArrowArrayView *v) {
    int64_t total = 0;
    for (int i = 0; i < 3; ++i) {
        int64_t n = v->buffer_views[i].size_bytes;
        if (n > 0) {
            if (n > INT64_MAX - total) return INT64_MAX;
            total += n;
        }
    }
    for (int64_t i = 0; i < v->n_children; ++i) {
        int64_t n = view_size(v->children[i]);
        if (n > INT64_MAX - total) return INT64_MAX;
        total += n;
    }
    if (v->dictionary) {
        int64_t n = view_size(v->dictionary);
        if (n > INT64_MAX - total) return INT64_MAX;
        total += n;
    }
    return total;
}
static int schema_equal(const struct ArrowSchema *a, const struct ArrowSchema *b) {
    if (strcmp(a->format, b->format) || strcmp(a->name ? a->name : "", b->name ? b->name : "") ||
        a->flags != b->flags || a->n_children != b->n_children ||
        !!a->dictionary != !!b->dictionary)
        return 0;
    for (int64_t i = 0; i < a->n_children; ++i)
        if (!schema_equal(a->children[i], b->children[i])) return 0;
    return !a->dictionary || schema_equal(a->dictionary, b->dictionary);
}
int VgiShmResolve(struct VgiShm *s, struct VgiBatch *b, struct ArrowBuffer *md,
                  struct ArrowError *error) {
    const char *metadata = (const char *)md->data;
    struct ArrowStringView off = VgiMetadataGet(metadata, OFFSET);
    if (!off.data || VgiMetadataGet(metadata, VGI_RPC_LOG_LEVEL_KEY).data) return 0;
    uint64_t offset, length, index;
    if (!s || b->array.length || number(off, &offset) ||
        number(VgiMetadataGet(metadata, LENGTH), &length) || region(s, offset, length, &index)) {
        ArrowErrorSet(error, "Invalid shared-memory pointer");
        return EINVAL;
    }
    struct ArrowBuffer bytes, inner;
    ArrowBufferInit(&bytes);
    ArrowBufferInit(&inner);
    int rc = 0;
    if (dictionary(&b->schema)) {
        rc = VgiIpcWriteSchema(&b->schema, &bytes, error);
        if (!rc) bytes.size_bytes -= 8;
    }
    if (!rc && dictionary(&b->schema))
        rc = ArrowBufferAppend(&bytes, s->data + offset, (int64_t)length);
    static const uint8_t eos[8] = {255, 255, 255, 255, 0, 0, 0, 0};
    if (!rc && dictionary(&b->schema)) rc = ArrowBufferAppend(&bytes, eos, 8);
    struct VgiBatch resolved = {0};
    struct ArrowBufferView input =
        dictionary(&b->schema) ? (struct ArrowBufferView){{bytes.data}, bytes.size_bytes}
                               : (struct ArrowBufferView){{s->data + offset}, (int64_t)length};
    if (!rc) rc = VgiIpcReadBatch(input, &resolved.schema, &resolved.array, &inner, error);
    if (!rc && (!schema_equal(&b->schema, &resolved.schema) ||
                VgiMetadataGet((const char *)inner.data, OFFSET).data))
        rc = EINVAL;
    /* IPC reads own their buffers, so no mmap alias escapes the lockstep turn. */
    release_region(s, index);
    if (!rc) rc = ArrowMetadataBuilderRemove(md, ArrowCharView(OFFSET));
    if (!rc) rc = ArrowMetadataBuilderRemove(md, ArrowCharView(LENGTH));
    if (!rc) rc = VgiMetadataSet(md, "vgi_rpc.shm_source", s->name);
    if (!rc) {
        VgiBatchReset(b);
        *b = resolved;
        memset(&resolved, 0, sizeof(resolved));
        s->stats.batches_read++;
        s->stats.bytes_read += length;
    }
    VgiBatchReset(&resolved);
    ArrowBufferReset(&inner);
    ArrowBufferReset(&bytes);
    return rc;
}
int VgiShmWrite(struct VgiShm *s, struct VgiIpcWriter *writer, const struct ArrowArray *array,
                const char *metadata, struct ArrowError *error) {
    if (!s || !array->length || VgiMetadataGet(metadata, VGI_RPC_LOG_LEVEL_KEY).data ||
        VgiMetadataGet(metadata, OFFSET).data ||
        VgiMetadataGet(metadata, VGI_RPC_LOCATION_KEY).data)
        return VgiIpcWriterNext(writer, array, metadata, error);
    if (s->threshold > 0) {
        struct ArrowArrayView view;
        ArrowArrayViewInitFromType(&view, NANOARROW_TYPE_UNINITIALIZED);
        int rc = ArrowArrayViewInitFromSchema(&view, &writer->schema, error);
        if (!rc) rc = ArrowArrayViewSetArray(&view, array, error);
        int small = !rc && view_size(&view) < s->threshold;
        ArrowArrayViewReset(&view);
        if (rc) return rc;
        if (small) {
            s->stats.inline_fallbacks++;
            return VgiIpcWriterNext(writer, array, metadata, error);
        }
    }
    struct ArrowBuffer bytes, md;
    ArrowBufferInit(&bytes);
    ArrowBufferInit(&md);
    int rc = VgiIpcWriteBatch(&writer->schema, array, NULL, &bytes, error);
    int64_t start = 0, length = bytes.size_bytes;
    if (!rc && dictionary(&writer->schema)) {
        start = (int64_t)get(bytes.data + 4, 4) + 8;
        length -= start + 8;
    }
    uint64_t offset = 0;
    if (!rc && length < s->threshold) rc = ENOSPC;
    if (!rc) rc = allocate(s, (uint64_t)length, &offset);
    if (rc == ENOSPC) {
        s->stats.inline_fallbacks++;
        rc = VgiIpcWriterNext(writer, array, metadata, error);
    } else if (!rc) {
        memcpy(s->data + offset, bytes.data + start, (size_t)length);
        struct VgiBatch pointer = {0};
        rc = VgiBatchInit(&pointer, &writer->schema, error);
        if (!rc) rc = VgiBatchFinish(&pointer, error);
        if (!rc) rc = ArrowMetadataBuilderInit(&md, metadata);
        char value[32];
        snprintf(value, sizeof(value), "%" PRIu64, offset);
        if (!rc) rc = VgiMetadataSet(&md, OFFSET, value);
        snprintf(value, sizeof(value), "%" PRId64, length);
        if (!rc) rc = VgiMetadataSet(&md, LENGTH, value);
        if (!rc) rc = VgiIpcWriterNext(writer, &pointer.array, (const char *)md.data, error);
        if (!rc) {
            s->stats.batches_written++;
            s->stats.bytes_written += (uint64_t)length;
        } else {
            uint64_t index;
            if (!region(s, offset, (uint64_t)length, &index)) release_region(s, index);
        }
        VgiBatchReset(&pointer);
    }
    ArrowBufferReset(&bytes);
    ArrowBufferReset(&md);
    return rc;
}
