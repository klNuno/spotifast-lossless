/*
 * fsync and fdatasync for Spotify Soloist. libatomic.so.1 comes before
 * libc.so.6 in Soloist's list of libraries, so these definitions take
 * the place of glibc's for every call Soloist makes. They carry glibc's
 * version name because Soloist asks for that version, and a definition
 * under any other name would not match.
 *
 * WSL1 answers EINVAL when a directory is synced, on its own filesystem
 * and on the Windows drives alike. LevelDB syncs a database's directory
 * after it writes the manifest and gives up on that error, so every
 * database Soloist opens there fails. Syncing a directory only makes a
 * rename durable; a filesystem that cannot do it still did the rename.
 * So EINVAL on a directory counts as success, and everything else,
 * including every result on Linux, passes through unchanged.
 */
#include <errno.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#define EXPORT __attribute__((visibility("default")))

static int settle(int fd, long result) {
    if (result == 0)
        return 0;
    int error = errno;
    struct stat st;
    if (error == EINVAL && fstat(fd, &st) == 0 && S_ISDIR(st.st_mode))
        return 0;
    errno = error;
    return -1;
}

EXPORT int fsync(int fd) {
    return settle(fd, syscall(SYS_fsync, fd));
}

EXPORT int fdatasync(int fd) {
    return settle(fd, syscall(SYS_fdatasync, fd));
}
