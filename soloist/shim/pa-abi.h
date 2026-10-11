/*
 * The part of libpulse's public ABI that Spotify Soloist uses, declared
 * here so the shim builds without PulseAudio's development headers. Layouts
 * and values match libpulse 0.9.16 and later; `check-abi.c` compares every
 * size and offset against the real headers.
 */
#ifndef PULSE_SHIM_PA_ABI_H
#define PULSE_SHIM_PA_ABI_H

#include <stddef.h>
#include <stdint.h>
#include <sys/time.h>

#define PA_CHANNELS_MAX 32U
#define PA_VOLUME_MUTED ((pa_volume_t)0U)
#define PA_VOLUME_NORM ((pa_volume_t)0x10000U)
#define PA_USEC_PER_SEC ((pa_usec_t)1000000ULL)

typedef uint64_t pa_usec_t;
typedef uint32_t pa_volume_t;

typedef enum pa_sample_format {
    PA_SAMPLE_U8,
    PA_SAMPLE_ALAW,
    PA_SAMPLE_ULAW,
    PA_SAMPLE_S16LE,
    PA_SAMPLE_S16BE,
    PA_SAMPLE_FLOAT32LE,
    PA_SAMPLE_FLOAT32BE,
    PA_SAMPLE_S32LE,
    PA_SAMPLE_S32BE,
    PA_SAMPLE_S24LE,
    PA_SAMPLE_S24BE,
    PA_SAMPLE_S24_32LE,
    PA_SAMPLE_S24_32BE,
    PA_SAMPLE_MAX,
    PA_SAMPLE_INVALID = -1
} pa_sample_format_t;

typedef struct pa_sample_spec {
    pa_sample_format_t format;
    uint32_t rate;
    uint8_t channels;
} pa_sample_spec;

typedef int pa_channel_position_t; /* an enum in libpulse */

typedef struct pa_channel_map {
    uint8_t channels;
    pa_channel_position_t map[PA_CHANNELS_MAX];
} pa_channel_map;

typedef struct pa_cvolume {
    uint8_t channels;
    pa_volume_t values[PA_CHANNELS_MAX];
} pa_cvolume;

typedef struct pa_buffer_attr {
    uint32_t maxlength;
    uint32_t tlength;
    uint32_t prebuf;
    uint32_t minreq;
    uint32_t fragsize;
} pa_buffer_attr;

typedef struct pa_timing_info {
    struct timeval timestamp;
    int synchronized_clocks;
    pa_usec_t sink_usec;
    pa_usec_t source_usec;
    pa_usec_t transport_usec;
    int playing;
    int write_index_corrupt;
    int64_t write_index;
    int read_index_corrupt;
    int64_t read_index;
    pa_usec_t configured_sink_usec;
    pa_usec_t configured_source_usec;
    int64_t since_underrun;
} pa_timing_info;

typedef enum pa_context_state {
    PA_CONTEXT_UNCONNECTED,
    PA_CONTEXT_CONNECTING,
    PA_CONTEXT_AUTHORIZING,
    PA_CONTEXT_SETTING_NAME,
    PA_CONTEXT_READY,
    PA_CONTEXT_FAILED,
    PA_CONTEXT_TERMINATED
} pa_context_state_t;

typedef enum pa_stream_state {
    PA_STREAM_UNCONNECTED,
    PA_STREAM_CREATING,
    PA_STREAM_READY,
    PA_STREAM_FAILED,
    PA_STREAM_TERMINATED
} pa_stream_state_t;

typedef enum pa_operation_state {
    PA_OPERATION_RUNNING,
    PA_OPERATION_DONE,
    PA_OPERATION_CANCELLED
} pa_operation_state_t;

typedef enum pa_context_flags { PA_CONTEXT_NOFLAGS = 0 } pa_context_flags_t;

typedef enum pa_stream_flags {
    PA_STREAM_NOFLAGS = 0x0000U,
    PA_STREAM_START_CORKED = 0x0001U
} pa_stream_flags_t;

typedef enum pa_seek_mode { PA_SEEK_RELATIVE = 0 } pa_seek_mode_t;

typedef enum pa_subscription_mask { PA_SUBSCRIPTION_MASK_NULL = 0 } pa_subscription_mask_t;

typedef enum pa_subscription_event_type {
    PA_SUBSCRIPTION_EVENT_SINK_INPUT = 0x0002U,
    PA_SUBSCRIPTION_EVENT_CHANGE = 0x0010U
} pa_subscription_event_type_t;

typedef enum pa_io_event_flags { PA_IO_EVENT_NULL = 0 } pa_io_event_flags_t;

typedef struct pa_threaded_mainloop pa_threaded_mainloop;
typedef struct pa_context pa_context;
typedef struct pa_stream pa_stream;
typedef struct pa_operation pa_operation;
typedef struct pa_proplist pa_proplist;
typedef struct pa_spawn_api pa_spawn_api;
typedef struct pa_format_info pa_format_info;
typedef struct pa_io_event pa_io_event;
typedef struct pa_time_event pa_time_event;
typedef struct pa_defer_event pa_defer_event;
typedef struct pa_mainloop_api pa_mainloop_api;

typedef void (*pa_io_event_cb_t)(pa_mainloop_api *, pa_io_event *, int, pa_io_event_flags_t, void *);
typedef void (*pa_io_event_destroy_cb_t)(pa_mainloop_api *, pa_io_event *, void *);
typedef void (*pa_time_event_cb_t)(pa_mainloop_api *, pa_time_event *, const struct timeval *, void *);
typedef void (*pa_time_event_destroy_cb_t)(pa_mainloop_api *, pa_time_event *, void *);
typedef void (*pa_defer_event_cb_t)(pa_mainloop_api *, pa_defer_event *, void *);
typedef void (*pa_defer_event_destroy_cb_t)(pa_mainloop_api *, pa_defer_event *, void *);

struct pa_mainloop_api {
    void *userdata;
    pa_io_event *(*io_new)(pa_mainloop_api *, int, pa_io_event_flags_t, pa_io_event_cb_t, void *);
    void (*io_enable)(pa_io_event *, pa_io_event_flags_t);
    void (*io_free)(pa_io_event *);
    void (*io_set_destroy)(pa_io_event *, pa_io_event_destroy_cb_t);
    pa_time_event *(*time_new)(pa_mainloop_api *, const struct timeval *, pa_time_event_cb_t, void *);
    void (*time_restart)(pa_time_event *, const struct timeval *);
    void (*time_free)(pa_time_event *);
    void (*time_set_destroy)(pa_time_event *, pa_time_event_destroy_cb_t);
    pa_defer_event *(*defer_new)(pa_mainloop_api *, pa_defer_event_cb_t, void *);
    void (*defer_enable)(pa_defer_event *, int);
    void (*defer_free)(pa_defer_event *);
    void (*defer_set_destroy)(pa_defer_event *, pa_defer_event_destroy_cb_t);
    void (*quit)(pa_mainloop_api *, int);
};

typedef struct pa_sink_input_info {
    uint32_t index;
    const char *name;
    uint32_t owner_module;
    uint32_t client;
    uint32_t sink;
    pa_sample_spec sample_spec;
    pa_channel_map channel_map;
    pa_cvolume volume;
    pa_usec_t buffer_usec;
    pa_usec_t sink_usec;
    const char *resample_method;
    const char *driver;
    int mute;
    pa_proplist *proplist;
    int corked;
    int has_volume;
    int volume_writable;
    pa_format_info *format;
} pa_sink_input_info;

typedef void (*pa_free_cb_t)(void *);
typedef void (*pa_context_notify_cb_t)(pa_context *, void *);
typedef void (*pa_context_success_cb_t)(pa_context *, int, void *);
typedef void (*pa_context_subscribe_cb_t)(pa_context *, pa_subscription_event_type_t, uint32_t, void *);
typedef void (*pa_stream_notify_cb_t)(pa_stream *, void *);
typedef void (*pa_stream_success_cb_t)(pa_stream *, int, void *);
typedef void (*pa_sink_input_info_cb_t)(pa_context *, const pa_sink_input_info *, int, void *);

#endif
