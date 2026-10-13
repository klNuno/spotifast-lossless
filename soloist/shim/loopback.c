/*
 * bind for Spotify Soloist: everything it serves stays on this computer.
 * Like dirsync.c, this definition takes the place of glibc's because
 * libatomic.so.1 comes first in Soloist's list of libraries.
 *
 * Soloist opens a ZeroConf login server on every interface and announces
 * it over mDNS, so any device on the network could sign an account into
 * it. Spotifast pairs it itself and needs nothing else. So a stream socket
 * bound to the unspecified address, and the mDNS socket (UDP port 5353),
 * are bound to loopback instead: the server answers Spotifast only and the
 * announcement never leaves the computer. Specific addresses, other
 * datagram sockets and other families pass through unchanged.
 *
 * On Windows, Soloist runs in a virtual machine of Spotifast's own whose
 * only network is QEMU's private one, and Spotifast reaches it through
 * QEMU's port forwarding, which needs a known port on the machine's own
 * address. There PULSE_SHIM_LISTEN=ADDRESS:PORT (IPv4) replaces loopback:
 * stream sockets bound to the unspecified address go to ADDRESS, and the
 * first one that asks the system for a port gets PORT.
 */
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

#define EXPORT __attribute__((visibility("default")))
#define MDNS_PORT 5353

/* PULSE_SHIM_LISTEN, read once. The port is in network order and goes back
 * to 0 once a socket has taken it. */
static struct {
    int set;
    struct in_addr address;
    in_port_t port;
} listen_at;
static pthread_once_t listen_read = PTHREAD_ONCE_INIT;

static void read_listen(void) {
    const char *value = getenv("PULSE_SHIM_LISTEN");
    const char *colon = value ? strrchr(value, ':') : NULL;
    char host[INET_ADDRSTRLEN];
    if (!colon || (size_t)(colon - value) >= sizeof host)
        return;
    memcpy(host, value, (size_t)(colon - value));
    host[colon - value] = '\0';
    char *end;
    unsigned long port = strtoul(colon + 1, &end, 10);
    if (*end != '\0' || port == 0 || port > 65535 || inet_pton(AF_INET, host, &listen_at.address) != 1)
        return;
    listen_at.port = htons((in_port_t)port);
    listen_at.set = 1;
}

/* The port for a socket that asked for `requested`: PULSE_SHIM_LISTEN's the
 * first time the system is asked to choose. */
static in_port_t listen_port(in_port_t requested) {
    if (requested != 0)
        return requested;
    return __atomic_exchange_n(&listen_at.port, 0, __ATOMIC_SEQ_CST);
}

/* Whether a socket bound to the unspecified address at this port should be
 * bound to loopback. */
static int kept_local(int fd, in_port_t port) {
    int type = 0;
    socklen_t size = sizeof type;
    if (getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &size) != 0)
        return 0;
    return type == SOCK_STREAM || (type == SOCK_DGRAM && ntohs(port) == MDNS_PORT);
}

static int is_stream(int fd) {
    int type = 0;
    socklen_t size = sizeof type;
    return getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &size) == 0 && type == SOCK_STREAM;
}

EXPORT int bind(int fd, const struct sockaddr *addr, socklen_t len) {
    pthread_once(&listen_read, read_listen);
    if (addr && addr->sa_family == AF_INET && len >= sizeof(struct sockaddr_in)) {
        struct sockaddr_in in;
        memcpy(&in, addr, sizeof in);
        if (in.sin_addr.s_addr == htonl(INADDR_ANY) && kept_local(fd, in.sin_port)) {
            if (listen_at.set && is_stream(fd)) {
                in.sin_addr = listen_at.address;
                in.sin_port = listen_port(in.sin_port);
            } else {
                in.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            }
            return (int)syscall(SYS_bind, fd, &in, (socklen_t)sizeof in);
        }
    } else if (addr && addr->sa_family == AF_INET6 && len >= sizeof(struct sockaddr_in6)) {
        struct sockaddr_in6 in6;
        memcpy(&in6, addr, sizeof in6);
        if (IN6_IS_ADDR_UNSPECIFIED(&in6.sin6_addr) && kept_local(fd, in6.sin6_port)) {
            /* A dual-stack socket keeps taking IPv4 through the IPv4
             * address mapped into IPv6, which is how Spotifast connects; a
             * socket limited to IPv6 gets ::1. */
            int only = 0;
            socklen_t size = sizeof only;
            getsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &only, &size);
            if (only) {
                in6.sin6_addr = in6addr_loopback;
            } else {
                unsigned char mapped[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 127, 0, 0, 1};
                if (listen_at.set && is_stream(fd)) {
                    memcpy(mapped + 12, &listen_at.address, 4);
                    in6.sin6_port = listen_port(in6.sin6_port);
                }
                memcpy(&in6.sin6_addr, mapped, sizeof mapped);
            }
            return (int)syscall(SYS_bind, fd, &in6, (socklen_t)sizeof in6);
        }
    }
    return (int)syscall(SYS_bind, fd, addr, len);
}
