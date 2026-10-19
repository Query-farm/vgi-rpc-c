#ifndef VGI_RPC_INTERNAL_H
#define VGI_RPC_INTERNAL_H
#include "vgi_rpc/rpc.h"
void VgiRpcCallInit(struct VgiRpcCall *, struct VgiRpcServer *);
void VgiRpcCallReset(struct VgiRpcCall *);
void VgiRpcReplyReset(struct VgiRpcReply *);
int VgiRpcPrepare(struct VgiRpcCall *, const char *protocol, const char *method);
int VgiRpcInvoke(struct VgiRpcCall *, struct VgiRpcReply *);
int VgiRpcCanResume(struct VgiRpcCall *);
int VgiRpcResume(struct VgiRpcCall *, struct ArrowBufferView, const struct ArrowSchema *,
                 struct VgiRpcReply *);
int VgiRpcErrorMetadata(struct VgiRpcCall *, struct ArrowBuffer *);
int VgiRpcWriteLogs(struct VgiRpcCall *, struct VgiIpcWriter *, const struct ArrowSchema *,
                    struct ArrowError *);
void VgiRpcDrainLogs(struct VgiRpcCall *, void (*)(void *, const char *), void *);
const char *VgiRpcServerId(struct VgiRpcServer *);
const char *VgiRpcPrimaryProtocol(struct VgiRpcServer *);
const struct VgiExternalOptions *VgiRpcServerExternal(struct VgiRpcServer *);
int VgiRpcServerAddManagedProtocol(struct VgiRpcServer *, const struct VgiRpcProtocol *,
                                   void (*)(void *), struct ArrowError *);
void VgiRpcServerFreeze(struct VgiRpcServer *);
int VgiRpcBatchInitNamed(struct VgiBatch *, const char *, struct ArrowError *);
int VgiIdentitySubjectOk(const char *);
const char *VgiRpcProtocolHash(struct VgiRpcServer *, const char *);
void VgiRpcServerBindTransport(struct VgiRpcServer *, enum VgiTransportKind);
int VgiRpcServerEnsureStarted(struct VgiRpcServer *, struct ArrowError *);
#endif
