#ifndef VGI_CLIENT_TRANSPORT_H
#define VGI_CLIENT_TRANSPORT_H

#include <stdint.h>

struct VgiClient;

int VgiClientTransportOpen(struct VgiClient *);
int VgiClientTransportReusable(struct VgiClient *);
void VgiClientTransportClose(struct VgiClient *);
int VgiClientTransportReady(struct VgiClient *, int writing);
int VgiClientTransportRead(struct VgiClient *, uint8_t *, int64_t, int64_t *);
int VgiClientTransportWrite(struct VgiClient *, const uint8_t *, int64_t, int64_t *);

#endif
