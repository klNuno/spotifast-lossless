/*
 * pulse-shim: a minimal libpulse.so.0 for Spotify Soloist.
 *
 * Soloist dlopens libpulse.so.0 and calls about fifty functions of the
 * threaded mainloop, context and stream API. This library implements those,
 * with no PulseAudio server: the PCM Soloist writes goes, unmixed and before
 * any volume, to a TCP receiver (Spotifast), and the volume Soloist sets on
 * its sink input is forwarded as a number. Audio is never written to disk.
 *
 * Environment:
 *   PULSE_SHIM_OUT=tcp:HOST:PORT  the receiver; anything else plays nowhere
 *                                 at real time (for tests)
 *   PULSE_SHIM_LOG=PATH           appends diagnostics and PCM statistics
 *   PULSE_SHIM_LIFELINE=1         stdin is a pipe nobody writes to: when it
 *                                 closes, the parent is gone and so is Soloist
 *   PULSE_SHIM_HIDE=OPTION        blanks the argument after OPTION in
 *                                 /proc/self/cmdline once Soloist has read it
 *   PULSE_SHIM_CHUNK_MS=N         audio per message, 5 to 100 ms instead of
 *                                 CHUNK_MS, where each message costs more, as
 *                                 through a virtual machine's network
 *
 * Wire format, little endian. Every message is a type byte, a u32 payload
 * length, then the payload.
 *   shim to receiver:
 *     1 FORMAT  u32 pa_sample_format, u32 rate, u32 channels
 *     2 PCM     interleaved samples in that format
 *     3 FLUSH   drop everything queued (seek, skip)
 *     4 CORK    u8: 1 paused, 0 playing
 *     5 VOLUME  u32 average pa_volume_t, 65536 is 100 %
 *   receiver to shim:
 *     6 ACK     u64 PCM bytes the receiver has taken since it connected,
 *               u32 audio the receiver still holds, in microseconds; a
 *               receiver that paces sends one as soon as it reads FORMAT
 *
 * Flow: the ring Soloist writes into is the device buffer. A receiver that
 * acknowledges sets the pace: the shim keeps at most WINDOW_MS of audio, or
 * three messages if that is more, in flight, so the output device's own clock drives Soloist and drift never
 * builds up. A receiver that never acknowledges, or none at all, is fed at
 * the wall clock's rate.
 */
#define _GNU_SOURCE
#include "pa-abi.h"

#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define EXPORT __attribute__((visibility("default")))

enum { MSG_FORMAT = 1, MSG_PCM = 2, MSG_FLUSH = 3, MSG_CORK = 4, MSG_VOLUME = 5, MSG_ACK = 6 };

#define CHUNK_MS 10          /* PCM per message, unless PULSE_SHIM_CHUNK_MS */
#define WINDOW_MS 100        /* unacknowledged audio a receiver may hold */
#define DEFAULT_TARGET_MS 250 /* Soloist asks for the server's default buffer */

/* ---------- diagnostics ---------- */

static FILE *g_log;
static pthread_once_t g_log_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t g_log_mutex = PTHREAD_MUTEX_INITIALIZER;

static void log_open(void) {
    const char *path = getenv("PULSE_SHIM_LOG");
    if (path && *path) g_log = fopen(path, "a");
}

static int logging(void) {
    pthread_once(&g_log_once, log_open);
    return g_log != NULL;
}

static double mono_now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void slog(const char *fmt, ...) {
    if (!logging()) return;
    pthread_mutex_lock(&g_log_mutex);
    fprintf(g_log, "[%.3f] ", mono_now());
    va_list ap;
    va_start(ap, fmt);
    vfprintf(g_log, fmt, ap);
    va_end(ap);
    fputc('\n', g_log);
    fflush(g_log);
    pthread_mutex_unlock(&g_log_mutex);
}

/* ---------- process hooks ---------- */

static void *lifeline(void *arg) {
    (void)arg;
    char drain[64];
    for (;;) {
        ssize_t r = read(0, drain, sizeof drain);
        if (r == 0 || (r < 0 && errno != EINTR)) {
            slog("lifeline: parent gone, exiting");
            _exit(0);
        }
    }
    return NULL;
}

/*
 * Runs when Soloist loads the library, after it parsed its arguments. glibc
 * hands constructors the process arguments, also under dlopen.
 */
__attribute__((constructor)) static void shim_init(int argc, char **argv, char **envp) {
    (void)envp;
    const char *hide = getenv("PULSE_SHIM_HIDE");
    if (hide && *hide && argv) {
        for (int i = 1; i + 1 < argc; i++) {
            if (argv[i] && argv[i + 1] && strcmp(argv[i], hide) == 0) memset(argv[i + 1], '*', strlen(argv[i + 1]));
        }
    }
    if (getenv("PULSE_SHIM_LIFELINE")) {
        pthread_attr_t attr;
        pthread_t thread;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_attr_setstacksize(&attr, 64 * 1024);
        if (pthread_create(&thread, &attr, lifeline, NULL) != 0) slog("lifeline: no thread");
        pthread_attr_destroy(&attr);
    }
}

/* ---------- threaded mainloop ---------- */

struct job {
    void (*run)(struct job *);
    void *a, *b, *c, *d;
    int i;
    struct job *next;
};

/*
 * Lock order: mainloop mutex, then stream lock, then jobs lock. The jobs
 * queue has its own lock and a pipe, so the sender thread can post events
 * without ever waiting for the mainloop mutex (Soloist may hold it while it
 * joins the sender in pa_stream_disconnect).
 */
struct pa_threaded_mainloop {
    pthread_mutex_t mutex; /* recursive, like libpulse */
    pthread_cond_t cond;   /* pa_threaded_mainloop_wait/signal */
    pthread_mutex_t jobs_lock;
    int wake_pipe[2];
    pthread_t thread;
    int started, quit;
    struct job *head, *tail;
    pa_mainloop_api api;
};

static void *api_unexpected(const char *what) {
    slog("unexpected mainloop api call: %s", what);
    return NULL;
}
static pa_io_event *api_io_new(pa_mainloop_api *a, int fd, pa_io_event_flags_t e, pa_io_event_cb_t cb, void *u) {
    (void)a; (void)fd; (void)e; (void)cb; (void)u;
    return api_unexpected("io_new");
}
static pa_time_event *api_time_new(pa_mainloop_api *a, const struct timeval *tv, pa_time_event_cb_t cb, void *u) {
    (void)a; (void)tv; (void)cb; (void)u;
    return api_unexpected("time_new");
}
static pa_defer_event *api_defer_new(pa_mainloop_api *a, pa_defer_event_cb_t cb, void *u) {
    (void)a; (void)cb; (void)u;
    return api_unexpected("defer_new");
}
static void api_quit(pa_mainloop_api *a, int r) {
    (void)a; (void)r;
    api_unexpected("quit");
}

static void enqueue(pa_threaded_mainloop *m, struct job *j) {
    j->next = NULL;
    pthread_mutex_lock(&m->jobs_lock);
    if (m->tail) m->tail->next = j; else m->head = j;
    m->tail = j;
    pthread_mutex_unlock(&m->jobs_lock);
    char b = 1;
    (void)!write(m->wake_pipe[1], &b, 1);
}

static struct job *job_new(void (*run)(struct job *)) {
    struct job *j = calloc(1, sizeof *j);
    if (!j) abort();
    j->run = run;
    return j;
}

static void *loop_thread(void *arg) {
    pa_threaded_mainloop *m = arg;
    char drain[64];
    for (;;) {
        if (read(m->wake_pipe[0], drain, sizeof drain) < 0 && errno != EINTR) break;
        pthread_mutex_lock(&m->mutex);
        int quit = m->quit;
        for (;;) {
            pthread_mutex_lock(&m->jobs_lock);
            struct job *j = m->head;
            if (j) {
                m->head = j->next;
                if (!m->head) m->tail = NULL;
            }
            pthread_mutex_unlock(&m->jobs_lock);
            if (!j) break;
            j->run(j); /* with the mainloop lock held, as libpulse callbacks run */
            free(j);
        }
        pthread_mutex_unlock(&m->mutex);
        if (quit) break;
    }
    return NULL;
}

EXPORT pa_threaded_mainloop *pa_threaded_mainloop_new(void) {
    pa_threaded_mainloop *m = calloc(1, sizeof *m);
    if (!m) return NULL;
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&m->mutex, &attr);
    pthread_mutexattr_destroy(&attr);
    pthread_cond_init(&m->cond, NULL);
    pthread_mutex_init(&m->jobs_lock, NULL);
    if (pipe2(m->wake_pipe, O_CLOEXEC) != 0) {
        free(m);
        return NULL;
    }
    m->api.userdata = m;
    m->api.io_new = api_io_new;
    m->api.time_new = api_time_new;
    m->api.defer_new = api_defer_new;
    m->api.quit = api_quit;
    return m;
}

EXPORT int pa_threaded_mainloop_start(pa_threaded_mainloop *m) {
    m->quit = 0;
    if (pthread_create(&m->thread, NULL, loop_thread, m) != 0) return -1;
    m->started = 1;
    return 0;
}

EXPORT void pa_threaded_mainloop_stop(pa_threaded_mainloop *m) {
    if (!m->started) return;
    pthread_mutex_lock(&m->mutex);
    m->quit = 1;
    pthread_mutex_unlock(&m->mutex);
    char b = 1;
    (void)!write(m->wake_pipe[1], &b, 1);
    pthread_join(m->thread, NULL);
    m->started = 0;
}

EXPORT void pa_threaded_mainloop_free(pa_threaded_mainloop *m) {
    if (!m) return;
    pa_threaded_mainloop_stop(m);
    while (m->head) {
        struct job *j = m->head;
        m->head = j->next;
        free(j);
    }
    close(m->wake_pipe[0]);
    close(m->wake_pipe[1]);
    pthread_mutex_destroy(&m->mutex);
    pthread_mutex_destroy(&m->jobs_lock);
    pthread_cond_destroy(&m->cond);
    free(m);
}

EXPORT void pa_threaded_mainloop_lock(pa_threaded_mainloop *m) { pthread_mutex_lock(&m->mutex); }
EXPORT void pa_threaded_mainloop_unlock(pa_threaded_mainloop *m) { pthread_mutex_unlock(&m->mutex); }
EXPORT void pa_threaded_mainloop_wait(pa_threaded_mainloop *m) { pthread_cond_wait(&m->cond, &m->mutex); }
EXPORT void pa_threaded_mainloop_signal(pa_threaded_mainloop *m, int wait_for_accept) {
    (void)wait_for_accept;
    pthread_cond_broadcast(&m->cond);
}
EXPORT pa_mainloop_api *pa_threaded_mainloop_get_api(pa_threaded_mainloop *m) { return &m->api; }

/* ---------- operations ---------- */

struct pa_operation {
    int refs;
    pa_operation_state_t state;
};

static pa_operation *op_new(void) {
    pa_operation *o = calloc(1, sizeof *o);
    if (!o) abort();
    o->refs = 2; /* one for the caller, one for the pending job */
    o->state = PA_OPERATION_RUNNING;
    return o;
}
static void op_done(pa_operation *o) {
    o->state = PA_OPERATION_DONE;
    if (--o->refs == 0) free(o);
}
EXPORT pa_operation_state_t pa_operation_get_state(const pa_operation *o) { return o->state; }
EXPORT void pa_operation_unref(pa_operation *o) {
    if (o && --o->refs == 0) free(o);
}

/* ---------- proplist ---------- */

struct pa_proplist {
    int unused;
};
EXPORT pa_proplist *pa_proplist_new(void) { return calloc(1, sizeof(pa_proplist)); }
EXPORT void pa_proplist_free(pa_proplist *p) { free(p); }
EXPORT int pa_proplist_sets(pa_proplist *p, const char *key, const char *value) {
    (void)p; (void)key; (void)value;
    return 0;
}

/* ---------- small helpers ---------- */

static size_t sample_bytes(pa_sample_format_t f) {
    switch (f) {
    case PA_SAMPLE_U8: case PA_SAMPLE_ALAW: case PA_SAMPLE_ULAW: return 1;
    case PA_SAMPLE_S16LE: case PA_SAMPLE_S16BE: return 2;
    case PA_SAMPLE_S24LE: case PA_SAMPLE_S24BE: return 3;
    default: return 4;
    }
}
static size_t frame_bytes(const pa_sample_spec *s) { return sample_bytes(s->format) * s->channels; }

EXPORT size_t pa_usec_to_bytes(pa_usec_t t, const pa_sample_spec *spec) {
    return (size_t)((t * spec->rate) / PA_USEC_PER_SEC) * frame_bytes(spec);
}
EXPORT struct timeval *pa_gettimeofday(struct timeval *tv) {
    gettimeofday(tv, NULL);
    return tv;
}
EXPORT pa_usec_t pa_timeval_diff(const struct timeval *a, const struct timeval *b) {
    int64_t ua = (int64_t)a->tv_sec * 1000000 + a->tv_usec;
    int64_t ub = (int64_t)b->tv_sec * 1000000 + b->tv_usec;
    return (pa_usec_t)(ua > ub ? ua - ub : ub - ua);
}
EXPORT pa_volume_t pa_cvolume_avg(const pa_cvolume *a) {
    uint64_t sum = 0;
    if (!a || a->channels == 0) return PA_VOLUME_MUTED;
    for (unsigned i = 0; i < a->channels && i < PA_CHANNELS_MAX; i++) sum += a->values[i];
    return (pa_volume_t)(sum / a->channels);
}
EXPORT pa_cvolume *pa_cvolume_set(pa_cvolume *a, unsigned channels, pa_volume_t v) {
    if (channels > PA_CHANNELS_MAX) channels = PA_CHANNELS_MAX;
    a->channels = (uint8_t)channels;
    for (unsigned i = 0; i < channels; i++) a->values[i] = v;
    return a;
}
EXPORT const char *pa_strerror(int error) {
    (void)error;
    return "pulse-shim error";
}

/* ---------- transport to the receiver ---------- */

struct link {
    int fd; /* -1 when not connected */
    char host[256];
    char port[16];
    int is_tcp;
    double next_retry;
    /* Flow control, counted since this connection opened. */
    int acks;            /* the receiver acknowledges */
    uint64_t sent, acked;
    uint32_t held_us;    /* audio the receiver reported holding */
    uint8_t in[64];
    size_t in_len;
    const int *run;      /* the stream's sender_run: a stalled send gives up when it clears */
};

static int send_all(int fd, const void *buf, size_t n, const int *run) {
    const uint8_t *p = buf;
    while (n) {
        ssize_t w = send(fd, p, n, MSG_NOSIGNAL);
        if (w < 0) {
            if (errno == EINTR) continue;
            /* SO_SNDTIMEO expired: the receiver is stalled; keep waiting unless the stream stopped. */
            if ((errno == EAGAIN || errno == EWOULDBLOCK) && (!run || __atomic_load_n(run, __ATOMIC_ACQUIRE))) continue;
            return -1;
        }
        p += w;
        n -= (size_t)w;
    }
    return 0;
}

static void link_close(struct link *l, const char *why) {
    if (l->fd < 0) return;
    slog("link: closed (%s)", why);
    close(l->fd);
    l->fd = -1;
}

static int link_send(struct link *l, uint8_t type, const void *payload, uint32_t len) {
    if (l->fd < 0) return -1;
    uint8_t head[5] = {type, (uint8_t)len, (uint8_t)(len >> 8), (uint8_t)(len >> 16), (uint8_t)(len >> 24)};
    if (send_all(l->fd, head, 5, l->run) < 0 || (len && send_all(l->fd, payload, len, l->run) < 0)) {
        link_close(l, strerror(errno));
        return -1;
    }
    return 0;
}

/* Reads whatever acknowledgements have arrived, without waiting. */
static void link_read_acks(struct link *l) {
    while (l->fd >= 0) {
        ssize_t r = recv(l->fd, l->in + l->in_len, sizeof l->in - l->in_len, MSG_DONTWAIT);
        if (r == 0) {
            link_close(l, "receiver closed");
            return;
        }
        if (r < 0) {
            if (errno == EINTR) continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK) link_close(l, strerror(errno));
            return;
        }
        l->in_len += (size_t)r;
        size_t at = 0;
        while (l->in_len - at >= 5) {
            uint8_t *m = l->in + at;
            uint32_t len = m[1] | m[2] << 8 | m[3] << 16 | (uint32_t)m[4] << 24;
            if (m[0] != MSG_ACK || len != 12) {
                link_close(l, "unexpected message from the receiver");
                return;
            }
            if (l->in_len - at < 5 + len) break;
            uint64_t acked = 0;
            uint32_t held = 0;
            memcpy(&acked, m + 5, 8);
            memcpy(&held, m + 13, 4);
            l->acked = acked;
            l->held_us = held;
            if (!l->acks) slog("link: receiver acknowledges");
            l->acks = 1;
            at += 5 + len;
        }
        memmove(l->in, l->in + at, l->in_len - at);
        l->in_len -= at;
    }
}

static void link_try_connect(struct link *l, const pa_sample_spec *ss) {
    if (!l->is_tcp || l->fd >= 0 || mono_now() < l->next_retry) return;
    l->next_retry = mono_now() + 0.5;
    struct addrinfo hints = {0}, *res = NULL;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(l->host, l->port, &hints, &res) != 0 || !res) return;
    int fd = socket(res->ai_family, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd >= 0 && connect(fd, res->ai_addr, res->ai_addrlen) == 0) {
        int one = 1;
        struct timeval tick = {0, 100000};
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tick, sizeof tick);
        l->fd = fd;
        l->acks = 0;
        l->sent = l->acked = 0;
        l->held_us = 0;
        l->in_len = 0;
        uint32_t fmt[3] = {(uint32_t)ss->format, ss->rate, ss->channels};
        link_send(l, MSG_FORMAT, fmt, sizeof fmt);
        slog("link: connected to %s:%s", l->host, l->port);
    } else if (fd >= 0) {
        close(fd);
    }
    freeaddrinfo(res);
}

/* ---------- PCM statistics, only while logging; never stores audio ---------- */

struct stats {
    uint64_t samples, below16, below24;
    double peak;
};

static void stats_feed(struct stats *st, const pa_sample_spec *ss, const uint8_t *p, size_t n) {
    if (ss->format != PA_SAMPLE_FLOAT32LE) return;
    for (size_t i = 0; i + 4 <= n; i += 4) {
        float f;
        memcpy(&f, p + i, 4);
        double s16 = f * 32768.0, s24 = f * 8388608.0;
        st->samples++;
        st->below16 += s16 != floor(s16);
        st->below24 += s24 != floor(s24);
        if (fabs(f) > st->peak) st->peak = fabs(f);
    }
}

static void stats_report(struct stats *st, const char *why) {
    if (!st->samples) return;
    slog("pcm %s: samples=%llu peak=%.1f dBFS below16=%.2f%% below24=%.2f%%",
         why, (unsigned long long)st->samples, st->peak > 0 ? 20 * log10(st->peak) : -999.0,
         100.0 * (double)st->below16 / (double)st->samples, 100.0 * (double)st->below24 / (double)st->samples);
    memset(st, 0, sizeof *st);
}

/* ---------- context ---------- */

struct pa_context {
    int refs;
    pa_threaded_mainloop *m;
    pa_context_state_t state;
    pa_context_notify_cb_t state_cb;
    void *state_ud;
    pa_context_subscribe_cb_t sub_cb;
    void *sub_ud;
    pa_cvolume volume;
    pa_stream *stream; /* Soloist uses one playback stream */
};

EXPORT pa_context *pa_context_new(pa_mainloop_api *api, const char *name) {
    pa_context *c = calloc(1, sizeof *c);
    if (!c) return NULL;
    c->refs = 1;
    c->m = api->userdata;
    c->state = PA_CONTEXT_UNCONNECTED;
    pa_cvolume_set(&c->volume, 2, PA_VOLUME_NORM);
    slog("context_new name=%s", name ? name : "?");
    return c;
}

static void ctx_set_state(pa_context *c, pa_context_state_t s) {
    c->state = s;
    if (c->state_cb) c->state_cb(c, c->state_ud);
}

static void job_ctx_connect(struct job *j) {
    pa_context *c = j->a;
    ctx_set_state(c, PA_CONTEXT_AUTHORIZING);
    ctx_set_state(c, PA_CONTEXT_SETTING_NAME);
    ctx_set_state(c, PA_CONTEXT_READY);
}

EXPORT int pa_context_connect(pa_context *c, const char *server, pa_context_flags_t flags, const pa_spawn_api *api) {
    (void)server; (void)flags; (void)api;
    /* libpulse is CONNECTING when this returns; Soloist treats UNCONNECTED as a failure. */
    ctx_set_state(c, PA_CONTEXT_CONNECTING);
    struct job *j = job_new(job_ctx_connect);
    j->a = c;
    enqueue(c->m, j);
    return 0;
}

EXPORT void pa_context_disconnect(pa_context *c) {
    if (c->state != PA_CONTEXT_TERMINATED) ctx_set_state(c, PA_CONTEXT_TERMINATED);
}
EXPORT void pa_context_unref(pa_context *c) {
    if (c && --c->refs == 0) free(c);
}
EXPORT pa_context_state_t pa_context_get_state(const pa_context *c) { return c->state; }
EXPORT int pa_context_errno(const pa_context *c) {
    (void)c;
    return 0;
}
EXPORT void pa_context_set_state_callback(pa_context *c, pa_context_notify_cb_t cb, void *u) {
    c->state_cb = cb;
    c->state_ud = u;
}
EXPORT void pa_context_set_subscribe_callback(pa_context *c, pa_context_subscribe_cb_t cb, void *u) {
    c->sub_cb = cb;
    c->sub_ud = u;
}

static void job_ctx_success(struct job *j) {
    pa_context_success_cb_t cb = (pa_context_success_cb_t)j->b;
    if (cb) cb(j->a, 1, j->c);
    op_done(j->d);
}

EXPORT pa_operation *pa_context_subscribe(pa_context *c, pa_subscription_mask_t m, pa_context_success_cb_t cb, void *u) {
    (void)m;
    pa_operation *o = op_new();
    struct job *j = job_new(job_ctx_success);
    j->a = c;
    j->b = (void *)cb;
    j->c = u;
    j->d = o;
    enqueue(c->m, j);
    return o;
}

/* ---------- stream ---------- */

struct pa_stream {
    int refs;
    pa_context *c;
    pa_sample_spec ss;
    pa_channel_map map;
    pa_stream_state_t state;
    uint32_t index;
    pa_buffer_attr attr;
    pa_stream_notify_cb_t state_cb, started_cb, underflow_cb;
    void *state_ud, *started_ud, *underflow_ud;

    pthread_mutex_t lock; /* guards everything below */
    pthread_cond_t have_data;
    uint8_t *ring;
    size_t cap, head, len; /* ring buffer: read position and fill */
    size_t target;         /* bytes Soloist may keep queued */
    int64_t write_index;   /* bytes Soloist wrote */
    int64_t taken;         /* bytes the sender took from the ring */
    int64_t in_flight;     /* of those, not yet acknowledged */
    uint32_t held_us;      /* audio the receiver holds */
    int corked, playing, flush_pending, cork_changed, volume_changed;
    int space_posted;      /* a space-freed event is queued; coalesces them */
    pa_timing_info ti;
    struct link link;      /* the sender thread's own */
    struct stats stats;
    pthread_t sender;
    int sender_run;
};

static uint32_t g_stream_index = 1;

static pa_stream *stream_new(pa_context *c, const char *name, const pa_sample_spec *ss, const pa_channel_map *map) {
    pa_stream *s = calloc(1, sizeof *s);
    if (!s) return NULL;
    s->refs = 1;
    s->c = c;
    s->ss = *ss;
    if (map) s->map = *map;
    s->state = PA_STREAM_UNCONNECTED;
    s->index = __atomic_fetch_add(&g_stream_index, 1, __ATOMIC_RELAXED);
    pthread_mutex_init(&s->lock, NULL);
    pthread_cond_init(&s->have_data, NULL);
    s->link.fd = -1;
    s->link.run = &s->sender_run;
    c->stream = s;
    slog("stream_new name=%s format=%d rate=%u channels=%u", name ? name : "?", (int)ss->format, ss->rate, ss->channels);
    return s;
}

EXPORT pa_stream *pa_stream_new(pa_context *c, const char *name, const pa_sample_spec *ss, const pa_channel_map *map) {
    return stream_new(c, name, ss, map);
}
EXPORT pa_stream *pa_stream_new_with_proplist(pa_context *c, const char *name, const pa_sample_spec *ss, const pa_channel_map *map, pa_proplist *p) {
    (void)p;
    return stream_new(c, name, ss, map);
}

EXPORT void pa_stream_set_state_callback(pa_stream *s, pa_stream_notify_cb_t cb, void *u) { s->state_cb = cb; s->state_ud = u; }
EXPORT void pa_stream_set_started_callback(pa_stream *s, pa_stream_notify_cb_t cb, void *u) { s->started_cb = cb; s->started_ud = u; }
EXPORT void pa_stream_set_underflow_callback(pa_stream *s, pa_stream_notify_cb_t cb, void *u) { s->underflow_cb = cb; s->underflow_ud = u; }
EXPORT pa_stream_state_t pa_stream_get_state(const pa_stream *s) { return s->state; }
EXPORT uint32_t pa_stream_get_index(const pa_stream *s) { return s->index; }

static void stream_set_state(pa_stream *s, pa_stream_state_t st) {
    s->state = st;
    if (s->state_cb) s->state_cb(s, s->state_ud);
}

static void stream_release(pa_stream *s);

static void job_stream_notify(struct job *j) {
    pa_stream *s = j->a;
    if (j->i == 0 && s->started_cb) s->started_cb(s, s->started_ud);
    if (j->i == 1 && s->underflow_cb) s->underflow_cb(s, s->underflow_ud);
    if (j->i == 2) {
        __atomic_store_n(&s->space_posted, 0, __ATOMIC_RELEASE);
        pthread_cond_broadcast(&s->c->m->cond); /* space freed: wake writers */
    }
    stream_release(s);
}

/* Never called with the stream lock held (see the lock order above). */
static void post_stream_event(pa_stream *s, int kind) {
    __atomic_add_fetch(&s->refs, 1, __ATOMIC_RELAXED);
    struct job *j = job_new(job_stream_notify);
    j->a = s;
    j->i = kind;
    enqueue(s->c->m, j);
}

static int sender_running(pa_stream *s) { return __atomic_load_n(&s->sender_run, __ATOMIC_ACQUIRE); }

/*
 * Holds a chunk until the receiver has room for it, reading acknowledgements
 * as they come. Returns at once for a receiver that does not acknowledge.
 */
static void await_window(pa_stream *s, size_t n, uint64_t window) {
    struct link *l = &s->link;
    for (;;) {
        link_read_acks(l);
        if (l->fd < 0 || !l->acks || !sender_running(s)) return;
        if (l->sent - l->acked + n <= window) return;
        struct pollfd p = {.fd = l->fd, .events = POLLIN};
        poll(&p, 1, 50); /* also rechecks sender_run while a receiver is stalled */
    }
}

static unsigned chunk_ms(void) {
    const char *value = getenv("PULSE_SHIM_CHUNK_MS");
    long ms = value && *value ? strtol(value, NULL, 10) : 0;
    return ms >= 5 && ms <= 100 ? (unsigned)ms : CHUNK_MS;
}

/* Feeds the ring to the receiver. Network calls and event posts happen with no lock held. */
static void *sender_thread(void *arg) {
    pa_stream *s = arg;
    size_t fb = frame_bytes(&s->ss);
    double byte_rate = (double)s->ss.rate * (double)fb;
    unsigned ms = chunk_ms();
    size_t chunk = (s->ss.rate * ms / 1000) * fb;
    unsigned window_ms = ms * 3 > WINDOW_MS ? ms * 3 : WINDOW_MS;
    uint64_t window = (uint64_t)(byte_rate * window_ms / 1000.0);
    uint8_t *buf = malloc(chunk);
    if (!buf) return NULL;
    double epoch = 0;
    uint64_t paced = 0;
    double last_report = mono_now();
    int log = logging();

    for (;;) {
        size_t n = 0;
        int flush = 0, cork = -1, volume = 0, started = 0, underflow = 0;

        pthread_mutex_lock(&s->lock);
        if (!s->sender_run) {
            pthread_mutex_unlock(&s->lock);
            break;
        }
        if (s->flush_pending) { s->flush_pending = 0; flush = 1; }
        if (s->cork_changed) { s->cork_changed = 0; cork = s->corked; }
        if (s->volume_changed) { s->volume_changed = 0; volume = 1; }
        if (s->corked || s->len == 0) {
            if (s->playing && !s->corked) {
                s->playing = 0;
                underflow = 1;
            }
            if (!flush && cork < 0 && !volume && !underflow) pthread_cond_wait(&s->have_data, &s->lock);
        } else {
            n = s->len < chunk ? s->len : chunk;
            n -= n % fb;
            if (n == 0) n = s->len;
            size_t first = s->cap - s->head < n ? s->cap - s->head : n;
            memcpy(buf, s->ring + s->head, first);
            memcpy(buf + first, s->ring, n - first);
            s->head = (s->head + n) % s->cap;
            s->len -= n;
            s->taken += (int64_t)n;
            if (!s->playing) { s->playing = 1; started = 1; }
            if (log) stats_feed(&s->stats, &s->ss, buf, n);
        }
        if (log && mono_now() - last_report >= 5.0) {
            last_report = mono_now();
            stats_report(&s->stats, "5s");
        }
        pthread_mutex_unlock(&s->lock);

        link_try_connect(&s->link, &s->ss);
        if (flush) { link_send(&s->link, MSG_FLUSH, NULL, 0); epoch = 0; }
        if (cork >= 0) { uint8_t v = (uint8_t)cork; link_send(&s->link, MSG_CORK, &v, 1); epoch = 0; }
        if (volume) {
            uint32_t v = pa_cvolume_avg(&s->c->volume);
            link_send(&s->link, MSG_VOLUME, &v, sizeof v);
        }
        if (underflow) {
            slog("underflow");
            post_stream_event(s, 1);
        }
        if (started) post_stream_event(s, 0);
        if (n == 0) continue;

        await_window(s, n, window);
        int acked_flow = s->link.fd >= 0 && s->link.acks;
        if (link_send(&s->link, MSG_PCM, buf, (uint32_t)n) == 0) s->link.sent += n;
        if (!acked_flow) {
            /* Nobody sets the pace: keep the wall clock's, with no catch-up burst after a stall. */
            double now = mono_now();
            if (epoch == 0 || now > epoch + (double)paced / byte_rate + 0.2) {
                epoch = now;
                paced = 0;
            }
            paced += n;
            double due = epoch + (double)paced / byte_rate;
            if (due > now) usleep((useconds_t)((due - now) * 1e6));
        }
        link_read_acks(&s->link);
        pthread_mutex_lock(&s->lock);
        s->in_flight = s->link.fd >= 0 && s->link.acks ? (int64_t)(s->link.sent - s->link.acked) : 0;
        s->held_us = s->link.fd >= 0 && s->link.acks ? s->link.held_us : 0;
        pthread_mutex_unlock(&s->lock);
        if (!__atomic_exchange_n(&s->space_posted, 1, __ATOMIC_ACQ_REL)) post_stream_event(s, 2);
    }
    if (log) stats_report(&s->stats, "end");
    link_close(&s->link, "stream ended");
    free(buf);
    return NULL;
}

static void job_stream_ready(struct job *j) {
    pa_stream *s = j->a;
    stream_set_state(s, PA_STREAM_READY);
}

EXPORT int pa_stream_connect_playback(pa_stream *s, const char *dev, const pa_buffer_attr *attr, pa_stream_flags_t flags, const pa_cvolume *volume, pa_stream *sync) {
    (void)sync; (void)dev;
    size_t fb = frame_bytes(&s->ss);
    size_t second = (size_t)s->ss.rate * fb;
    s->attr = attr ? *attr : (pa_buffer_attr){(uint32_t)-1, (uint32_t)-1, (uint32_t)-1, (uint32_t)-1, (uint32_t)-1};
    s->target = s->attr.tlength != (uint32_t)-1 ? s->attr.tlength : second * DEFAULT_TARGET_MS / 1000;
    if (s->target < second / 50) s->target = second / 50;
    if (s->target > 2 * second) s->target = 2 * second;
    s->target -= s->target % fb;
    s->cap = 4 * second;
    s->ring = malloc(s->cap);
    if (!s->ring) return -1;
    s->corked = (flags & PA_STREAM_START_CORKED) != 0;
    if (volume) s->c->volume = *volume;
    slog("connect_playback target=%zu bytes (%.0f ms) corked=%d", s->target, 1000.0 * (double)s->target / (double)second, s->corked);

    const char *out = getenv("PULSE_SHIM_OUT");
    if (out && strncmp(out, "tcp:", 4) == 0) {
        const char *hp = out + 4, *colon = strrchr(hp, ':');
        if (colon) {
            snprintf(s->link.host, sizeof s->link.host, "%.*s", (int)(colon - hp), hp);
            snprintf(s->link.port, sizeof s->link.port, "%s", colon + 1);
            s->link.is_tcp = 1;
        }
    }
    __atomic_store_n(&s->sender_run, 1, __ATOMIC_RELEASE);
    __atomic_add_fetch(&s->refs, 1, __ATOMIC_RELAXED); /* the sender's reference, dropped in disconnect */
    if (pthread_create(&s->sender, NULL, sender_thread, s) != 0) {
        s->sender_run = 0;
        stream_release(s);
        return -1;
    }
    stream_set_state(s, PA_STREAM_CREATING); /* synchronous, as in libpulse */
    struct job *j = job_new(job_stream_ready);
    j->a = s;
    enqueue(s->c->m, j);
    return 0;
}

EXPORT size_t pa_stream_writable_size(const pa_stream *cs) {
    pa_stream *s = (pa_stream *)cs;
    if (s->state != PA_STREAM_READY) return (size_t)-1;
    pthread_mutex_lock(&s->lock);
    size_t w = s->len >= s->target ? 0 : s->target - s->len;
    pthread_mutex_unlock(&s->lock);
    return w;
}

EXPORT int pa_stream_write(pa_stream *s, const void *data, size_t nbytes, pa_free_cb_t free_cb, int64_t offset, pa_seek_mode_t seek) {
    if (offset != 0 || seek != PA_SEEK_RELATIVE) slog("write with offset=%lld seek=%d (ignored)", (long long)offset, (int)seek);
    pthread_mutex_lock(&s->lock);
    const uint8_t *p = data;
    size_t n = nbytes;
    if (s->len + n > s->cap) {
        slog("ring overflow: dropping %zu bytes", s->len + n - s->cap);
        n = s->cap - s->len;
    }
    size_t tail = (s->head + s->len) % s->cap;
    size_t first = s->cap - tail < n ? s->cap - tail : n;
    memcpy(s->ring + tail, p, first);
    memcpy(s->ring, p + first, n - first);
    s->len += n;
    s->write_index += (int64_t)nbytes;
    pthread_cond_signal(&s->have_data);
    pthread_mutex_unlock(&s->lock);
    if (free_cb) free_cb((void *)data);
    return 0;
}

static void job_stream_success(struct job *j) {
    pa_stream_success_cb_t cb = (pa_stream_success_cb_t)j->b;
    if (cb) cb(j->a, 1, j->c);
    op_done(j->d);
}

static pa_operation *stream_op(pa_stream *s, pa_stream_success_cb_t cb, void *u) {
    pa_operation *o = op_new();
    struct job *j = job_new(job_stream_success);
    j->a = s;
    j->b = (void *)cb;
    j->c = u;
    j->d = o;
    enqueue(s->c->m, j);
    return o;
}

EXPORT pa_operation *pa_stream_cork(pa_stream *s, int b, pa_stream_success_cb_t cb, void *u) {
    pthread_mutex_lock(&s->lock);
    if (s->corked != !!b) {
        s->corked = !!b;
        s->cork_changed = 1;
        if (s->corked) s->playing = 0;
        pthread_cond_signal(&s->have_data);
    }
    pthread_mutex_unlock(&s->lock);
    return stream_op(s, cb, u);
}
EXPORT int pa_stream_is_corked(const pa_stream *s) { return s->corked; }

EXPORT pa_operation *pa_stream_flush(pa_stream *s, pa_stream_success_cb_t cb, void *u) {
    pthread_mutex_lock(&s->lock);
    s->head = s->len = 0;
    s->taken = s->write_index;
    s->in_flight = 0;
    s->flush_pending = 1;
    s->playing = 0;
    pthread_cond_signal(&s->have_data);
    pthread_mutex_unlock(&s->lock);
    return stream_op(s, cb, u);
}

static void fill_timing(pa_stream *s) {
    pthread_mutex_lock(&s->lock);
    pa_timing_info *t = &s->ti;
    memset(t, 0, sizeof *t);
    gettimeofday(&t->timestamp, NULL);
    t->synchronized_clocks = 1;
    double byte_rate = (double)s->ss.rate * (double)frame_bytes(&s->ss);
    /* Played is what the receiver acknowledged; what it still holds is the sink's latency. */
    t->sink_usec = (pa_usec_t)((double)s->in_flight * 1e6 / byte_rate) + s->held_us;
    t->configured_sink_usec = t->sink_usec;
    t->playing = s->playing;
    t->write_index = s->write_index;
    t->read_index = s->taken - s->in_flight;
    pthread_mutex_unlock(&s->lock);
}

EXPORT const pa_timing_info *pa_stream_get_timing_info(pa_stream *s) {
    fill_timing(s);
    return &s->ti;
}

static void job_timing(struct job *j) {
    pa_stream *s = j->a;
    fill_timing(s);
    pa_stream_success_cb_t cb = (pa_stream_success_cb_t)j->b;
    if (cb) cb(s, 1, j->c);
    op_done(j->d);
}

EXPORT pa_operation *pa_stream_update_timing_info(pa_stream *s, pa_stream_success_cb_t cb, void *u) {
    pa_operation *o = op_new();
    struct job *j = job_new(job_timing);
    j->a = s;
    j->b = (void *)cb;
    j->c = u;
    j->d = o;
    enqueue(s->c->m, j);
    return o;
}

EXPORT int pa_stream_disconnect(pa_stream *s) {
    pthread_mutex_lock(&s->lock);
    int running = s->sender_run;
    __atomic_store_n(&s->sender_run, 0, __ATOMIC_RELEASE);
    pthread_cond_signal(&s->have_data);
    pthread_mutex_unlock(&s->lock);
    if (running) {
        pthread_join(s->sender, NULL);
        stream_release(s); /* the sender's reference */
    }
    if (s->c->stream == s) s->c->stream = NULL;
    stream_set_state(s, PA_STREAM_TERMINATED);
    return 0;
}

static void stream_release(pa_stream *s) {
    if (!s || __atomic_sub_fetch(&s->refs, 1, __ATOMIC_ACQ_REL) != 0) return;
    free(s->ring);
    pthread_mutex_destroy(&s->lock);
    pthread_cond_destroy(&s->have_data);
    free(s);
}

EXPORT void pa_stream_unref(pa_stream *s) { stream_release(s); }

/* ---------- sink input introspection (volume) ---------- */

static void job_sink_input_info(struct job *j) {
    pa_context *c = j->a;
    pa_sink_input_info_cb_t cb = (pa_sink_input_info_cb_t)j->b;
    pa_sink_input_info info;
    memset(&info, 0, sizeof info);
    static pa_proplist empty;
    info.index = (uint32_t)j->i;
    info.name = "Soloist";
    if (c->stream) {
        info.sample_spec = c->stream->ss;
        info.channel_map = c->stream->map;
        info.corked = c->stream->corked;
    }
    info.volume = c->volume;
    info.driver = "pulse-shim";
    info.proplist = &empty;
    info.has_volume = 1;
    info.volume_writable = 1;
    if (cb) {
        cb(c, &info, 0, j->c);
        cb(c, NULL, 1, j->c);
    }
    op_done(j->d);
}

EXPORT pa_operation *pa_context_get_sink_input_info(pa_context *c, uint32_t idx, pa_sink_input_info_cb_t cb, void *u) {
    pa_operation *o = op_new();
    struct job *j = job_new(job_sink_input_info);
    j->a = c;
    j->b = (void *)cb;
    j->c = u;
    j->d = o;
    j->i = (int)idx;
    enqueue(c->m, j);
    return o;
}

static void job_volume_set(struct job *j) {
    pa_context *c = j->a;
    pa_context_success_cb_t cb = (pa_context_success_cb_t)j->b;
    if (cb) cb(c, 1, j->c);
    if (c->sub_cb) c->sub_cb(c, PA_SUBSCRIPTION_EVENT_SINK_INPUT | PA_SUBSCRIPTION_EVENT_CHANGE, (uint32_t)j->i, c->sub_ud);
    op_done(j->d);
}

EXPORT pa_operation *pa_context_set_sink_input_volume(pa_context *c, uint32_t idx, const pa_cvolume *volume, pa_context_success_cb_t cb, void *u) {
    c->volume = *volume;
    if (c->stream) {
        pthread_mutex_lock(&c->stream->lock);
        c->stream->volume_changed = 1;
        pthread_cond_signal(&c->stream->have_data);
        pthread_mutex_unlock(&c->stream->lock);
    }
    pa_operation *o = op_new();
    struct job *j = job_new(job_volume_set);
    j->a = c;
    j->b = (void *)cb;
    j->c = u;
    j->d = o;
    j->i = (int)idx;
    enqueue(c->m, j);
    return o;
}
