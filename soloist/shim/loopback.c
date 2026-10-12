/*
 * bind for Spotify Soloist: everything it serves stays on this computer.
 * Like dirsync.c, this definition takes the place of glibc's because
 * libatomic.so.1 comes first in Soloist's list of libraries.
 *
 * Soloist opens a ZeroConf login server on every interface and announces
 * it over mDNS, so any device on the network could sign an account into
 * it, and on Windows its first start raises a firewall prompt. Spotifast
 * pairs it over loopback and needs nothing else. So a stream socket bound
 * to the unspecified address, and the mDNS socket (UDP port 5353), are
 * bound to loopback instead: the server answers Spotifast only and the
 * announcement never leaves the computer. Specific addresses, other
 * datagram sockets and other families pass through unchanged.
 */
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

#define EXPORT __attribute__((visibility("default")))
#define MDNS_PORT 5353

/* Whether a socket bound to the unspecified address at this port should be
 * bound to loopback. */
static int kept_local(int fd, in_port_t port) {
    int type = 0;
    socklen_t size = sizeof type;
    if (getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &size) != 0)
        return 0;
    return type == SOCK_STREAM || (type == SOCK_DGRAM && ntohs(port) == MDNS_PORT);
}

EXPORT int bind(int fd, const struct sockaddr *addr, socklen_t len) {
    if (addr && addr->sa_family == AF_INET && len >= sizeof(struct sockaddr_in)) {
        struct sockaddr_in in;
        memcpy(&in, addr, sizeof in);
        if (in.sin_addr.s_addr == htonl(INADDR_ANY) && kept_local(fd, in.sin_port)) {
            in.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            return (int)syscall(SYS_bind, fd, &in, (socklen_t)sizeof in);
        }
    } else if (addr && addr->sa_family == AF_INET6 && len >= sizeof(struct sockaddr_in6)) {
        struct sockaddr_in6 in6;
        memcpy(&in6, addr, sizeof in6);
        if (IN6_IS_ADDR_UNSPECIFIED(&in6.sin6_addr) && kept_local(fd, in6.sin6_port)) {
            /* A dual-stack socket keeps taking IPv4 through 127.0.0.1
             * mapped into IPv6, which is how Spotifast connects; a socket
             * limited to IPv6 gets ::1. */
            int only = 0;
            socklen_t size = sizeof only;
            getsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &only, &size);
            if (only) {
                in6.sin6_addr = in6addr_loopback;
            } else {
                static const unsigned char mapped[16] = {0, 0, 0, 0, 0, 0, 0, 0,
                                                         0, 0, 0xff, 0xff, 127, 0, 0, 1};
                memcpy(&in6.sin6_addr, mapped, sizeof mapped);
            }
            return (int)syscall(SYS_bind, fd, &in6, (socklen_t)sizeof in6);
        }
    }
    return (int)syscall(SYS_bind, fd, addr, len);
}
