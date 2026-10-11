/*
 * Compares pa-abi.h with libpulse's own headers: build it twice, once with
 * -DOURS, and diff the two outputs. Run by build.sh before every build.
 */
#ifdef OURS
#include "pa-abi.h"
#else
#include <pulse/pulseaudio.h>
#endif
#include <stddef.h>
#include <stdio.h>

#define S(t) printf("sizeof %s %zu\n", #t, sizeof(t))
#define O(t, f) printf("offsetof %s.%s %zu\n", #t, #f, offsetof(t, f))
#define V(v) printf("value %s %lld\n", #v, (long long)(v))

int main(void) {
    S(pa_sample_spec); O(pa_sample_spec, format); O(pa_sample_spec, rate); O(pa_sample_spec, channels);
    S(pa_channel_map); O(pa_channel_map, map);
    S(pa_cvolume); O(pa_cvolume, values);
    S(pa_buffer_attr); O(pa_buffer_attr, tlength); O(pa_buffer_attr, fragsize);
    S(pa_timing_info);
    O(pa_timing_info, synchronized_clocks); O(pa_timing_info, sink_usec); O(pa_timing_info, source_usec);
    O(pa_timing_info, transport_usec); O(pa_timing_info, playing); O(pa_timing_info, write_index_corrupt);
    O(pa_timing_info, write_index); O(pa_timing_info, read_index_corrupt); O(pa_timing_info, read_index);
    O(pa_timing_info, configured_sink_usec); O(pa_timing_info, configured_source_usec);
    O(pa_timing_info, since_underrun);
    S(pa_mainloop_api);
    O(pa_mainloop_api, io_new); O(pa_mainloop_api, time_new); O(pa_mainloop_api, defer_new);
    O(pa_mainloop_api, defer_set_destroy); O(pa_mainloop_api, quit);
    S(pa_sink_input_info);
    O(pa_sink_input_info, name); O(pa_sink_input_info, sink); O(pa_sink_input_info, sample_spec);
    O(pa_sink_input_info, channel_map); O(pa_sink_input_info, volume); O(pa_sink_input_info, buffer_usec);
    O(pa_sink_input_info, sink_usec); O(pa_sink_input_info, resample_method); O(pa_sink_input_info, driver);
    O(pa_sink_input_info, mute); O(pa_sink_input_info, proplist); O(pa_sink_input_info, corked);
    O(pa_sink_input_info, has_volume); O(pa_sink_input_info, volume_writable); O(pa_sink_input_info, format);
    S(pa_sample_format_t); S(pa_context_state_t); S(pa_stream_state_t); S(pa_operation_state_t);
    S(pa_stream_flags_t); S(pa_seek_mode_t); S(pa_subscription_event_type_t); S(pa_usec_t); S(pa_volume_t);
    V(PA_SAMPLE_S16LE); V(PA_SAMPLE_FLOAT32LE); V(PA_SAMPLE_S32LE); V(PA_SAMPLE_S24LE); V(PA_SAMPLE_S24_32LE);
    V(PA_CONTEXT_CONNECTING); V(PA_CONTEXT_READY); V(PA_CONTEXT_TERMINATED);
    V(PA_STREAM_CREATING); V(PA_STREAM_READY); V(PA_STREAM_TERMINATED);
    V(PA_OPERATION_RUNNING); V(PA_OPERATION_DONE);
    V(PA_STREAM_START_CORKED); V(PA_SEEK_RELATIVE);
    V(PA_SUBSCRIPTION_EVENT_SINK_INPUT); V(PA_SUBSCRIPTION_EVENT_CHANGE);
    V(PA_VOLUME_NORM); V(PA_VOLUME_MUTED); V(PA_CHANNELS_MAX); V(PA_USEC_PER_SEC);
    return 0;
}
