/*
 * libatomic.so.1 for Spotify Soloist: the five functions it imports from
 * GCC's libatomic, so Spotifast does not depend on the system having it.
 *
 * Every operation takes a lock picked by address. That is how libatomic
 * itself implements the generic, size-parameterised functions, and using the
 * same locks for the 16-byte ones keeps all accesses to one object mutually
 * atomic. Soloist calls these rarely: the compiler inlines every atomic it
 * can do lock-free.
 */
#include <pthread.h>
#include <stdint.h>
#include <string.h>

#define EXPORT __attribute__((visibility("default")))
#define LOCKS 64

/* The compiler reserves these names for its builtins, so the C functions
 * take other names and assembler labels give them the real ones. */
EXPORT void libat_load(size_t, void *, void *, int) __asm__("__atomic_load");
EXPORT void libat_store(size_t, void *, void *, int) __asm__("__atomic_store");
EXPORT void libat_exchange(size_t, void *, void *, void *, int) __asm__("__atomic_exchange");
EXPORT unsigned __int128 libat_load_16(const volatile void *, int) __asm__("__atomic_load_16");
EXPORT void libat_store_16(volatile void *, unsigned __int128, int) __asm__("__atomic_store_16");

static pthread_mutex_t locks[LOCKS] = {[0 ... LOCKS - 1] = PTHREAD_MUTEX_INITIALIZER};

static pthread_mutex_t *lock_for(const volatile void *mem) {
    return &locks[((uintptr_t)mem >> 4) % LOCKS];
}

void libat_load(size_t size, void *mem, void *ret, int model) {
    (void)model;
    pthread_mutex_t *lock = lock_for(mem);
    pthread_mutex_lock(lock);
    memcpy(ret, mem, size);
    pthread_mutex_unlock(lock);
}

void libat_store(size_t size, void *mem, void *val, int model) {
    (void)model;
    pthread_mutex_t *lock = lock_for(mem);
    pthread_mutex_lock(lock);
    memcpy(mem, val, size);
    pthread_mutex_unlock(lock);
}

void libat_exchange(size_t size, void *mem, void *val, void *ret, int model) {
    (void)model;
    pthread_mutex_t *lock = lock_for(mem);
    pthread_mutex_lock(lock);
    memcpy(ret, mem, size);
    memcpy(mem, val, size);
    pthread_mutex_unlock(lock);
}

unsigned __int128 libat_load_16(const volatile void *mem, int model) {
    (void)model;
    unsigned __int128 out;
    pthread_mutex_t *lock = lock_for(mem);
    pthread_mutex_lock(lock);
    memcpy(&out, (const void *)mem, sizeof out);
    pthread_mutex_unlock(lock);
    return out;
}

void libat_store_16(volatile void *mem, unsigned __int128 val, int model) {
    (void)model;
    pthread_mutex_t *lock = lock_for(mem);
    pthread_mutex_lock(lock);
    memcpy((void *)mem, &val, sizeof val);
    pthread_mutex_unlock(lock);
}
