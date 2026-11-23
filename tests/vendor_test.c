/* Blob-vendor client tests. Offline checks always run; the round trips run
 * against vgi-rpc-python's fake vendor when VGI_RPC_FAKE_VENDOR_URL names one
 * started with `--test-routes --token test-token=tenant-a`
 * (tests/check_vendor.py starts it). */
#define _POSIX_C_SOURCE 200809L
#include "vgi_rpc/vendor.h"

#include <errno.h>
#include <json-c/json.h>
#include <sodium.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "external_internal.h"
#include "rpc_internal.h"
#include "vgi_rpc/http.h"
#include "vgi_rpc/rpc.h"

#define CHECK(x)                                                                  \
    do {                                                                          \
        if (!(x)) {                                                               \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #x); \
            exit(1);                                                              \
        }                                                                         \
    } while (0)

static const char *base;

static void clear_env(void) {
    const char *names[] = {VGI_BLOB_VENDOR_URL_ENV,
                           VGI_BLOB_VENDOR_TOKEN_ENV,
                           VGI_BLOB_VENDOR_TOKEN_FALLBACK_ENV,
                           VGI_RPC_EXTERNALIZE_THRESHOLD_ENV,
                           VGI_RPC_EXTERNAL_COMPRESSION_ENV,
                           VGI_RPC_MAX_UPLOAD_BYTES_ENV,
                           VGI_RPC_MAX_FETCH_BYTES_ENV,
                           VGI_RPC_MAX_REQUEST_BYTES_ENV};
    for (size_t i = 0; i < sizeof(names) / sizeof(*names); ++i) unsetenv(names[i]);
}

static void offline_tests(void) {
    struct VgiVendor *v = NULL;
    struct ArrowError error = {{0}};
    /* The token would cross the network in clear text. */
    CHECK(VgiVendorNew("http://vendor.example", "t", NULL, &v, &error) == EINVAL && !v);
    CHECK(VgiVendorNew("ftp://vendor.example", "t", NULL, &v, &error) == EINVAL);
    CHECK(VgiVendorNew("https://vendor.example", "", NULL, &v, &error) == EINVAL);
    CHECK(VgiVendorNew("https://u:p@vendor.example", "t", NULL, &v, &error) == EINVAL);
    CHECK(VgiVendorNew("http://127.0.0.1:9", "t", NULL, &v, &error) == 0 && v);
    VgiVendorFree(v);

    clear_env();
    struct VgiExternalOptions external;
    memset(&external, 0x5a, sizeof(external));
    struct VgiVendor *none = (struct VgiVendor *)1;
    CHECK(VgiExternalOptionsFromEnv(&external, &none, &error) == 0 && !none);
    setenv(VGI_BLOB_VENDOR_URL_ENV, "http://127.0.0.1:9", 1);
    CHECK(VgiExternalOptionsFromEnv(&external, &v, &error) == EINVAL); /* no token */
    setenv(VGI_BLOB_VENDOR_TOKEN_FALLBACK_ENV, "t", 1);
    CHECK(VgiExternalOptionsFromEnv(&external, &v, &error) == 0 && v);
    CHECK(external.max_fetch_bytes == 256LL * 1024 * 1024 && external.max_upload_bytes == 0);
    CHECK(external.threshold_bytes == 1024 * 1024 && external.allocate &&
          external.allocate_client_upload && external.put_object && external.get_object);
    VgiVendorFree(v);
    /* Fix 1: an upload cap raises an unset fetch cap... */
    setenv(VGI_RPC_MAX_UPLOAD_BYTES_ENV, "536870912", 1);
    CHECK(VgiExternalOptionsFromEnv(&external, &v, &error) == 0);
    CHECK(external.max_fetch_bytes == 536870912 && external.max_upload_bytes == 536870912);
    VgiVendorFree(v);
    /* ...and contradicting an explicit one is refused. */
    setenv(VGI_RPC_MAX_FETCH_BYTES_ENV, "1048576", 1);
    CHECK(VgiExternalOptionsFromEnv(&external, &v, &error) == EINVAL);
    setenv(VGI_RPC_MAX_UPLOAD_BYTES_ENV, "4096", 1);
    setenv(VGI_RPC_EXTERNALIZE_THRESHOLD_ENV, "100", 1);
    setenv(VGI_RPC_EXTERNAL_COMPRESSION_ENV, "zstd:3", 1);
    CHECK(VgiExternalOptionsFromEnv(&external, &v, &error) == 0);
    CHECK(external.max_fetch_bytes == 1048576 && external.max_upload_bytes == 4096 &&
          external.threshold_bytes == 100 && !strcmp(external.compression, "zstd"));
    VgiVendorFree(v);
    setenv(VGI_RPC_EXTERNAL_COMPRESSION_ENV, "brotli", 1);
    CHECK(VgiExternalOptionsFromEnv(&external, &v, &error) == EINVAL);
    unsetenv(VGI_RPC_EXTERNAL_COMPRESSION_ENV);
    setenv(VGI_RPC_EXTERNALIZE_THRESHOLD_ENV, "-1", 1);
    CHECK(VgiExternalOptionsFromEnv(&external, &v, &error) == EINVAL);
    clear_env();

    /* The server configuration hook: request cap only, without a vendor. */
    struct VgiRpcServer *server = VgiRpcServerNew();
    CHECK(server);
    struct VgiHttpOptions options;
    VgiHttpOptionsInit(&options);
    setenv(VGI_RPC_MAX_REQUEST_BYTES_ENV, "32768", 1);
    CHECK(VgiHttpConfigureFromEnv(server, &options, &v, &error) == 0 && !v);
    CHECK(options.max_request_bytes == 32768 && !VgiRpcServerExternal(server));
    setenv(VGI_BLOB_VENDOR_URL_ENV, "http://127.0.0.1:9", 1);
    setenv(VGI_BLOB_VENDOR_TOKEN_ENV, "t", 1);
    CHECK(VgiHttpConfigureFromEnv(server, &options, &v, &error) == 0 && v);
    CHECK(VgiRpcServerExternal(server) && VgiRpcServerExternal(server)->get_object);
    VgiVendorFree(v);
    VgiRpcServerFree(server);
    clear_env();
}

/* GET a test route of the fake vendor and return one integer field. */
static int64_t admin(const char *path, const char *key) {
    struct VgiExternalOptions plain;
    VgiExternalOptionsInit(&plain);
    plain.validate_url = NULL;
    char url[512];
    snprintf(url, sizeof(url), "%s%s", base, path);
    struct ArrowBuffer body;
    ArrowBufferInit(&body);
    struct ArrowError error = {{0}};
    CHECK(VgiExternalHttp(&plain, "GET", url, (struct ArrowBufferView){{NULL}, 0}, NULL, NULL, 0,
                          &body, &error) == 0);
    CHECK(ArrowBufferAppend(&body, "", 1) == 0);
    struct json_object *o = json_tokener_parse((const char *)body.data), *field = NULL;
    CHECK(o);
    int64_t value = -1;
    if (key && json_object_object_get_ex(o, key, &field)) value = json_object_get_int64(field);
    json_object_put(o);
    ArrowBufferReset(&body);
    return value;
}
static int64_t vstat(const char *key) { return admin("/__stats__", key); }

static void vendor_tests(void) {
    struct ArrowError error = {{0}};
    clear_env();
    setenv(VGI_BLOB_VENDOR_URL_ENV, base, 1);
    setenv(VGI_BLOB_VENDOR_TOKEN_ENV, "test-token", 1);
    setenv(VGI_RPC_EXTERNALIZE_THRESHOLD_ENV, "4096", 1);
    struct VgiExternalOptions external;
    struct VgiVendor *v = NULL;
    CHECK(VgiExternalOptionsFromEnv(&external, &v, &error) == 0 && v);
    admin("/__admin__/reset", NULL);

    /* A client-upload grant carries origin=client_upload; storage verifies the
     * signed size and checksum. */
    int64_t puts_rpc = vstat("puts_rpc"), puts_upload = vstat("puts_upload"),
            client_uploads = vstat("client_uploads");
    const char body[] = "request-body";
    char sha256[65];
    CHECK(VgiExternalSha256Hex(body, strlen(body), sha256) == 0);
    struct VgiExternalUploadUrl grant = {0};
    CHECK(external.allocate_client_upload(external.storage_state, (int64_t)strlen(body), sha256,
                                          &grant, &error) == 0);
    CHECK(grant.upload_url && grant.download_url && grant.object_id && grant.expires_at_us > 0);
    int checksum = 0;
    for (size_t i = 0; i < grant.header_count; ++i) {
        CHECK(strcasecmp(grant.headers[i].name, "Content-Length"));
        if (!strcasecmp(grant.headers[i].name, "x-amz-checksum-sha256")) checksum = 1;
    }
    CHECK(checksum);
    CHECK(vstat("puts_upload") - puts_upload == 1 && vstat("puts_rpc") == puts_rpc);
    struct ArrowBuffer out;
    ArrowBufferInit(&out);
    CHECK(VgiExternalHttp(&external, "PUT", grant.upload_url,
                          (struct ArrowBufferView){{"request-bodx"}, 12}, NULL, grant.headers,
                          grant.header_count, &out, &error) != 0);
    CHECK(VgiExternalHttp(&external, "PUT", grant.upload_url,
                          (struct ArrowBufferView){{body}, (int64_t)strlen(body)}, NULL,
                          grant.headers, grant.header_count, &out, &error) == 0);
    ArrowBufferReset(&out);
    CHECK(vstat("client_uploads") - client_uploads == 1);
    /* get_object fetches it back by id (an argument reference's path). */
    int64_t gets_upload = vstat("gets_upload");
    ArrowBufferInit(&out);
    CHECK(external.get_object(external.storage_state, grant.object_id, 1 << 20, &out, &error) == 0);
    CHECK(out.size_bytes == (int64_t)strlen(body) && !memcmp(out.data, body, strlen(body)));
    ArrowBufferReset(&out);
    CHECK(vstat("gets_upload") - gets_upload == 1);
    ArrowBufferInit(&out);
    CHECK(external.get_object(external.storage_state, grant.object_id, 4, &out, &error) == EFBIG);
    ArrowBufferReset(&out);

    /* Pinning: the vendor's hosts pass, anything else is refused. */
    CHECK(VgiVendorValidateUrl(v, grant.download_url, &error) == 0);
    CHECK(VgiVendorValidateUrl(v, "https://evil.example/object", &error) == EACCES);
    CHECK(VgiVendorValidateUrl(v, "http://127.0.0.1:1/object", &error) == EACCES);
    CHECK(VgiVendorValidateUrl(v, "http://user@127.0.0.1/object", &error) != 0);

    /* The object lifecycle (the fake's delete route) makes it missing. */
    admin("/__admin__/delete?origin=client_upload", NULL);
    ArrowBufferInit(&out);
    CHECK(external.get_object(external.storage_state, grant.object_id, 1 << 20, &out, &error) ==
          ENOENT);
    ArrowBufferReset(&out);
    VgiExternalUploadUrlReset(&grant);

    /* Result externalization: a server write, no origin. */
    puts_rpc = vstat("puts_rpc");
    puts_upload = vstat("puts_upload");
    unsigned char payload[6000];
    for (size_t i = 0; i < sizeof(payload); ++i) payload[i] = (unsigned char)(i * 7);
    struct VgiExternalRef ref = {0};
    CHECK(VgiExternalPublish(&external, NULL, (struct ArrowBufferView){{payload}, sizeof(payload)},
                             1, &ref, &error) == 0);
    CHECK(ref.location && strlen(ref.sha256) == 64);
    CHECK(vstat("puts_rpc") - puts_rpc == 1 && vstat("puts_upload") == puts_upload);
    ArrowBufferInit(&out);
    CHECK(VgiExternalFetch(&external, ref.location, ref.sha256, &out, &error) == 0);
    CHECK(out.size_bytes == (int64_t)sizeof(payload) &&
          !memcmp(out.data, payload, sizeof(payload)));
    ArrowBufferReset(&out);
    VgiExternalRefReset(&ref);

    /* The object store contract: put_object (origin state_ref), get_object. */
    int64_t puts_state = vstat("puts_state_ref"), gets_state = vstat("gets_state_ref");
    char *object_id = NULL;
    CHECK(external.put_object(external.storage_state,
                              (struct ArrowBufferView){{payload}, sizeof(payload)}, &object_id,
                              &error) == 0 &&
          object_id && *object_id);
    ArrowBufferInit(&out);
    CHECK(external.get_object(external.storage_state, object_id, 1 << 20, &out, &error) == 0);
    CHECK(out.size_bytes == (int64_t)sizeof(payload) &&
          !memcmp(out.data, payload, sizeof(payload)));
    ArrowBufferReset(&out);
    CHECK(vstat("puts_state_ref") - puts_state == 1 && vstat("gets_state_ref") - gets_state == 1);
    free(object_id);
    ArrowBufferInit(&out);
    CHECK(external.get_object(external.storage_state, "0123456789abcdef0123456789abcdef", 1 << 20,
                              &out, &error) == ENOENT);
    ArrowBufferReset(&out);
    VgiVendorFree(v);

    /* A wrong token is refused by the vendor, never retried into success. */
    CHECK(VgiVendorNew(base, "wrong-token", NULL, &v, &error) == 0);
    struct VgiExternalOptions bad;
    VgiExternalOptionsInit(&bad);
    VgiVendorBindExternal(v, &bad);
    CHECK(bad.allocate(bad.storage_state, 12, sha256, &grant, &error) != 0);
    CHECK(!strstr(error.message, "wrong-token"));
    VgiExternalUploadUrlReset(&grant);
    VgiVendorFree(v);
    clear_env();
}

int main(void) {
    CHECK(sodium_init() >= 0);
    offline_tests();
    base = getenv("VGI_RPC_FAKE_VENDOR_URL");
    if (!base || !*base) {
        puts("vendor: offline checks passed; set VGI_RPC_FAKE_VENDOR_URL for the round trips");
        return 0;
    }
    vendor_tests();
    puts("vendor: offline checks and fake-vendor round trips passed");
    return 0;
}
