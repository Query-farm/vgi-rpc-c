#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "rpc_internal.h"
#include "session.h"
#include "token.h"
#include "vgi_rpc/proof.h"

#define CHECK(condition)                                                    \
    do {                                                                    \
        if (!(condition)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition); \
            abort();                                                        \
        }                                                                   \
    } while (0)

static const char secret[] =
    "proxy:1111111111111111111111111111111111111111111111111111111111111111";
/* Independently minted by vgi_rpc.http.mint_proof, not by the verifier. */
static const char proof_now[] =
    "v1.proxy.1000.AAAAAAAAAAAAAAAAAAAAAA.YkXLF5Zw83sEJZr02ESa4ZHPuJ7ligKc8nsrbinTcxw";
static const char proof_future[] =
    "v1.proxy.1010.Q0ZPUk1BTkNFTk9OQ0UyMg.bAikFAIeUjkic-ysWj35kVf60ax1vlJNrI8u4pHtYZE";

static void proof_tests(void) {
    struct VgiProofOptions options;
    VgiProofOptionsInit(&options);
    options.mode = VGI_PROOF_REQUIRE;
    options.origin_id = "worker";
    options.secrets = secret;
    options.skew_seconds = 10;
    options.replay_capacity = 2;
    struct ArrowError error = {{0}};
    struct VgiProxyProof *proof = NULL;
    CHECK(VgiProxyProofNew(&options, &proof, &error) == 0);
    struct VgiProofResult result;
    CHECK(VgiProxyProofCheck(proof, NULL, 0, 1000, &result) == EACCES);
    CHECK(!strcmp(result.reason, "no_proof"));
    CHECK(VgiProxyProofCheck(proof, proof_future, 2, 1000, &result) == EACCES);
    CHECK(!strcmp(result.reason, "malformed"));
    CHECK(VgiProxyProofCheck(proof, proof_future, 1, 1000, &result) == 0);
    CHECK(result.verified && !strcmp(result.kid, "proxy"));
    /* A future-dated proof must remain in the cache through its whole window. */
    CHECK(VgiProxyProofCheck(proof, proof_future, 1, 1015, &result) == EACCES);
    CHECK(!strcmp(result.reason, "replayed"));
    CHECK(VgiProxyProofCheck(proof, proof_future, 1, 1021, &result) == EACCES);
    CHECK(!strcmp(result.reason, "expired"));
    CHECK(VgiProxyProofCheck(proof, proof_now, 1, 1000, &result) == 0);
    CHECK(VgiProxyProofCheck(proof, "v1.proxy.+1000.bad.bad", 1, 1000, &result) == EACCES);
    CHECK(!strcmp(result.reason, "malformed"));
    VgiProxyProofFree(proof);
    proof = NULL;
    options.secrets = "proxy:11";
    CHECK(VgiProxyProofNew(&options, &proof, &error) == EINVAL && !proof);
    options.secrets = secret;
    options.origin_id = "invalid origin";
    CHECK(VgiProxyProofNew(&options, &proof, &error) == EINVAL && !proof);
    options.origin_id = "other-worker";
    CHECK(VgiProxyProofNew(&options, &proof, &error) == 0);
    CHECK(VgiProxyProofCheck(proof, proof_future, 1, 1000, &result) == EACCES);
    CHECK(!strcmp(result.reason, "bad_mac"));
    VgiProxyProofFree(proof);
    proof = NULL;
    options.origin_id = "worker";
    options.mode = VGI_PROOF_ALLOW;
    CHECK(VgiProxyProofNew(&options, &proof, &error) == 0);
    CHECK(VgiProxyProofCheck(proof, "bad-token", 1, 1000, &result) == 0 && !result.verified);
    VgiProxyProofFree(proof);
}

static int releases;
static void release_counter(void *state) {
    ++releases;
    free(state);
}
static int *counter(int value) {
    int *p = malloc(sizeof(*p));
    CHECK(p);
    *p = value;
    return p;
}
struct WaitingCall {
    struct VgiSessions *sessions;
    struct VgiRpcServer *server;
    const char *token;
    pthread_mutex_t ready_mutex;
    pthread_cond_t ready_cond;
    int ready;
    int value;
};
static void *resume_thread(void *arg) {
    struct WaitingCall *wait = arg;
    struct VgiRpcCall call;
    VgiRpcCallInit(&call, wait->server);
    call.principal = "alice";
    call.auth_domain = "users";
    struct VgiSessionBinding binding;
    pthread_mutex_lock(&wait->ready_mutex);
    wait->ready = 1;
    pthread_cond_signal(&wait->ready_cond);
    pthread_mutex_unlock(&wait->ready_mutex);
    CHECK(VgiSessionBind(wait->sessions, &call, wait->token, 1, &binding) == 0);
    wait->value = *(int *)VgiRpcSessionGet(&call);
    CHECK(VgiRpcSessionClose(&call) == 0);
    VgiSessionUnbind(&binding);
    VgiRpcCallReset(&call);
    return NULL;
}
static void session_tests(void) {
    struct VgiRpcServer *server = VgiRpcServerNew();
    CHECK(server);
    unsigned char key[32] = {1};
    struct VgiSessions *sessions = NULL, *other = NULL;
    CHECK(VgiSessionsNew(key, "worker-one", 300, 1, &sessions) == 0);
    CHECK(VgiSessionsNew(key, "worker-two", 300, 1, &other) == 0);
    struct VgiRpcCall call;
    VgiRpcCallInit(&call, server);
    call.principal = "alice";
    call.auth_domain = "users";
    struct VgiSessionBinding binding;
    CHECK(VgiSessionBind(sessions, &call, NULL, 0, &binding) == 0);
    int *state = counter(7);
    CHECK(VgiRpcSessionOpen(&call, state, release_counter, 0) != 0);
    CHECK(strstr(call.error.message, "VGI-Session-Accept") && releases == 0);
    VgiSessionUnbind(&binding);
    CHECK(VgiSessionBind(sessions, &call, NULL, 1, &binding) == 0);
    CHECK(VgiRpcSessionOpen(&call, state, release_counter, 0) == 0);
    char token[512];
    strcpy(token, binding.token);
    CHECK(*(int *)VgiRpcSessionGet(&call) == 7);
    VgiSessionUnbind(&binding);
    call.principal = "bob";
    CHECK(VgiSessionBind(sessions, &call, token, 1, &binding) != 0);
    CHECK(!strcmp(call.error_kind, "session_lost"));
    call.principal = "alice";
    CHECK(VgiSessionBind(other, &call, token, 1, &binding) != 0);
    CHECK(!strcmp(call.error_kind, "session_lost"));
    CHECK(VgiSessionBind(sessions, &call, token, 1, &binding) == 0);
    struct WaitingCall wait = {
        sessions, server, token, PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, 0};
    pthread_t thread;
    CHECK(pthread_create(&thread, NULL, resume_thread, &wait) == 0);
    pthread_mutex_lock(&wait.ready_mutex);
    while (!wait.ready) pthread_cond_wait(&wait.ready_cond, &wait.ready_mutex);
    pthread_mutex_unlock(&wait.ready_mutex);
    *(int *)VgiRpcSessionGet(&call) = 23;
    VgiSessionUnbind(&binding);
    CHECK(pthread_join(thread, NULL) == 0);
    CHECK(wait.value == 23 && releases == 1);
    pthread_cond_destroy(&wait.ready_cond);
    pthread_mutex_destroy(&wait.ready_mutex);
    CHECK(VgiSessionBind(sessions, &call, token, 1, &binding) != 0);
    CHECK(VgiSessionBind(sessions, &call, NULL, 1, &binding) == 0);
    state = counter(9);
    CHECK(VgiRpcSessionsSetDraining(&call, 1) == 0);
    CHECK(VgiRpcSessionOpen(&call, state, release_counter, 0) != 0);
    CHECK(!strcmp(call.error_kind, "server_draining") && releases == 1);
    CHECK(VgiRpcSessionsSetDraining(&call, 0) == 0);
    CHECK(VgiRpcSessionOpen(&call, state, release_counter, 0) == 0);
    VgiSessionUnbind(&binding);
    VgiSessionsFree(sessions);
    CHECK(releases == 2);
    VgiSessionsFree(other);
    VgiRpcCallReset(&call);
    VgiRpcServerFree(server);
}
static void token_tests(void) {
    unsigned char key[32] = {1};
    struct VgiTokenContext context = {key, "call", "Protocol", "method", "domain", "alice"};
    uint8_t content[8192];
    memset(content, 'x', sizeof(content));
    struct ArrowBufferView input = {{content}, sizeof(content)};
    struct ArrowBuffer output;
    ArrowBufferInit(&output);
    char *encoded = NULL;
    CHECK(VgiTokenSeal(&context, input, &encoded) == 0);
    CHECK(strlen(encoded) < sizeof(content) / 4); /* compression is inside the seal */
    CHECK(VgiTokenOpen(&context, ArrowCharView(encoded), &output) == 0);
    CHECK(output.size_bytes == sizeof(content) && !memcmp(content, output.data, sizeof(content)));
    ArrowBufferReset(&output);
    context.principal = "bob";
    CHECK(VgiTokenOpen(&context, ArrowCharView(encoded), &output) != 0 && !output.size_bytes);
    context.principal = "alice";
    context.purpose = "cursor";
    CHECK(VgiTokenOpen(&context, ArrowCharView(encoded), &output) != 0 && !output.size_bytes);
    context.purpose = "call";
    context.method = "other";
    CHECK(VgiTokenOpen(&context, ArrowCharView(encoded), &output) != 0 && !output.size_bytes);
    context.method = "method";
    encoded[20] = encoded[20] == 'A' ? 'B' : 'A';
    CHECK(VgiTokenOpen(&context, ArrowCharView(encoded), &output) != 0 && !output.size_bytes);
    free(encoded);
    encoded = NULL;
    input.size_bytes = 1;
    CHECK(VgiTokenSeal(&context, input, &encoded) == 0);
    CHECK(strlen(encoded) == 58); /* 25-byte envelope + 16-byte tag + codec + byte */
    CHECK(VgiTokenOpen(&context, ArrowCharView(encoded), &output) == 0);
    CHECK(output.size_bytes == 1 && output.data[0] == 'x');
    ArrowBufferReset(&output);
    free(encoded);
}
int main(void) {
    token_tests();
    proof_tests();
    session_tests();
    return 0;
}
