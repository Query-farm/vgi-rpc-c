#ifndef VGI_SESSION_INTERNAL_H
#define VGI_SESSION_INTERNAL_H
#include "vgi_rpc/rpc.h"
struct VgiSessions;
struct VgiSessionEntry;
struct VgiSessionBinding {
    struct VgiSessions *registry;
    struct VgiSessionEntry *entry;
    int accepted, closed;
    char token[512];
    char id[25];
    const char *action;
};
int VgiSessionsNew(const unsigned char *key, const char *server_id, unsigned ttl, size_t capacity,
                   struct VgiSessions **out);
void VgiSessionsFree(struct VgiSessions *);
void VgiSessionsSweep(struct VgiSessions *);
void VgiSessionsDrain(struct VgiSessions *, int draining);
int VgiSessionBind(struct VgiSessions *, struct VgiRpcCall *, const char *token, int accepted,
                   struct VgiSessionBinding *);
void VgiSessionUnbind(struct VgiSessionBinding *);
#endif
