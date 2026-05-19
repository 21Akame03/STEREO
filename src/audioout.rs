use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ringbuf::{HeapRb, traits::*};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

/// Snapshot of the input (BlackHole) clock: how many input frames have been
/// captured and the host-monotonic time at the most recent capture. Written
/// from BlackHole's input callback (close to the hardware capture instant for
/// minimal timing jitter); read by the output callback to compute the true
/// input-vs-output clock ratio.
///
/// Seqlock pattern: writers bump `seq` to odd before writing, back to even
/// after. Readers retry if `seq` changes or is odd. Avoids locks in the audio
/// callback path.
#[derive(Default)]
pub struct ClockSnapshot {
    seq: AtomicU64,
    frames: AtomicU64,
    host_nanos: AtomicU64,
}

impl ClockSnapshot {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn write(&self, frames: u64, host_nanos: u64) {
        let s = self.seq.load(Ordering::Relaxed).wrapping_add(1);
        self.seq.store(s, Ordering::Release);
        self.frames.store(frames, Ordering::Relaxed);
        self.host_nanos.store(host_nanos, Ordering::Relaxed);
        self.seq.store(s.wrapping_add(1), Ordering::Release);
    }

    pub fn read(&self) -> Option<(u64, u64)> {
        for _ in 0..4 {
            let s1 = self.seq.load(Ordering::Acquire);
            if s1 & 1 != 0 {
                continue;
            }
            let frames = self.frames.load(Ordering::Relaxed);
            let host_nanos = self.host_nanos.load(Ordering::Relaxed);
            let s2 = self.seq.load(Ordering::Acquire);
            if s1 == s2 {
                return Some((frames, host_nanos));
            }
        }
        None
    }
}

pub struct AudioOut {
    _stream: cpal::Stream, // Keep Alive
    pub producer: ringbuf::HeapProd<f32>,
    channels: usize,
    sample_rate: usize,
    push_count: u64,
    /// Target playback delay, in input (mono) sample frames.
    /// Updated by the RTCP-RR loop; the output callback nudges the ring-buffer
    /// fill toward this value by inserting silence or dropping frames.
    target_delay_frames: Arc<AtomicUsize>,
}

impl AudioOut {
    pub fn new(initial_delay_ms: u32, input_clock: Arc<ClockSnapshot>) -> Self {
        let host = cpal::default_host();
        let device = host
            .output_devices()
            .expect("no output devices")
            .find(|d| {
                d.name()
                    .map(|n| n.contains("MacBook Pro Speakers"))
                    .unwrap_or(false)
            })
            .expect("MacBook Pro Speakers not found");
        if let Ok(name) = device.name() {
            println!("Output device: {}", name);
        }

        let supported = device
            .default_output_config()
            .expect("no default output config");
        let channels = supported.channels() as usize;
        let config: cpal::StreamConfig = supported.into();

        println!("Output: {} ch @ {} Hz", config.channels, config.sample_rate);

        let sample_rate = config.sample_rate as usize;
        let delay_frames = (sample_rate * initial_delay_ms as usize) / 1000;
        // Ring buffer must hold the priming silence plus ~1s of audio headroom.
        // Stored in output samples (frames * channels).
        let rb_capacity = (delay_frames + sample_rate) * channels;
        let rb = HeapRb::<f32>::new(rb_capacity);
        let (mut producer, mut consumer) = rb.split();

        // Pre-fill with silence so the local speakers run `initial_delay_ms`
        // behind the RTP stream.
        for _ in 0..(delay_frames * channels) {
            producer.try_push(0.0).ok();
        }
        println!(
            "audioout: priming {} ms of silence ({} frames) for sync",
            initial_delay_ms, delay_frames
        );

        let target_delay_frames = Arc::new(AtomicUsize::new(delay_frames));
        let target_cb = Arc::clone(&target_delay_frames);
        let epoch = Instant::now();
        let input_clock_cb = Arc::clone(&input_clock);

        // Drift correction by variable-rate linear-interpolation resampling.
        // `step` = input frames consumed per output frame. We split it into:
        //   • `nominal_step` — the learned BlackHole:speakers clock ratio.
        //     Re-estimated every 5 s from (popped + fill_delta) / output_frames.
        //   • `trim` — small proportional term that nudges fill toward target.
        // Once nominal_step converges, trim → 0 and there is no steady-state
        // drift. Linear interp between consecutive input frames keeps the
        // resample click-free.
        // To hold drift to ±10 frames we need a tight controller:
        //   • No dead-band — even tiny errors must pull fill back.
        //   • trim_gain set so MAX_TRIM saturates around err = 100 frames,
        //     giving ~10 s time constant. At err = 10 frames, trim = 0.01%
        //     which corrects fill at ~5 frames/sec.
        //   • MAX_TRIM lowered to 0.1% to keep pitch shift inaudible
        //     (~1.7 cents — well below the ~5 cent JND).
        const MAX_TRIM: f32 = 0.001; // 0.1%
        const MAX_NOMINAL_DEV: f32 = 0.02; // 2% — cap on learned clock ratio
        let trim_gain: f32 = MAX_TRIM / 100.0; // saturate at err = 100 frames
        // Warmup schedule: short windows with high gain so we lock onto the
        // true clock ratio in ~10 s instead of minutes. Each entry is
        // (window_seconds, ema_gain). After the schedule we use the last entry.
        // Warmup schedule: (window_seconds, ema_gain). Gain 0.0 discards the
        // window — the first one captures the startup transient (network
        // burst draining queued packets after priming), which would otherwise
        // bias nominal_step high.
        const SCHEDULE: &[(u64, f32)] = &[
            (2, 0.0),  // discard: startup burst
            (3, 1.0),  // seed from first clean window
            (5, 0.5),  // coarse refine
            (5, 0.3),  // fine refine
            (30, 0.1), // steady state
        ];
        let mut sched_idx: usize = 0;
        let mut window_size: u64 = (sample_rate as u64) * SCHEDULE[0].0;
        let mut nominal_step: f32 = 1.0;
        let mut window_output_frames: u64 = 0;
        let mut window_start_target: usize = delay_frames;
        // CPAL-clock-based ratio measurement state. Replaces the fill-delta
        // estimator: ratio = (Δinput_frames/Δinput_host_ns)
        //                   / (Δoutput_frames/Δoutput_host_ns), so packet
        // quantization in the ring buffer no longer shows up in the metric.
        let mut output_frames_total: u64 = 0;
        let mut last_snapshot: Option<(u64, u64, u64, u64)> = None;
        // (input_frames, input_host_ns, output_frames, output_host_ns)
        let mut prev_frame = vec![0f32; channels];
        let mut next_frame = vec![0f32; channels];
        let mut phase: f32 = 1.0; // force initial pop

        let stream = device
            .build_output_stream(
                &config,
                move |out: &mut [f32], _| {
                    let target = target_cb.load(Ordering::Relaxed);
                    let mut i = 0;
                    while i + channels <= out.len() {
                        let fill_frames = consumer.occupied_len() / channels;
                        let err = fill_frames as i64 - target as i64;
                        let trim = (err as f32 * trim_gain).clamp(-MAX_TRIM, MAX_TRIM);
                        let step = nominal_step + trim;

                        while phase >= 1.0 {
                            std::mem::swap(&mut prev_frame, &mut next_frame);
                            for c in 0..channels {
                                // On underflow, hold last sample (click-free).
                                next_frame[c] = consumer.try_pop().unwrap_or(prev_frame[c]);
                            }
                            phase -= 1.0;
                        }

                        for c in 0..channels {
                            out[i + c] = prev_frame[c] + (next_frame[c] - prev_frame[c]) * phase;
                        }

                        phase += step;
                        window_output_frames += 1;
                        output_frames_total += 1;
                        i += channels;
                    }

                    // Re-estimate the BlackHole↔speakers clock ratio at the end
                    // of each window. Uses CPAL host-time differentials of both
                    // streams' frame counters; immune to ring-buffer fill
                    // packet quantization.
                    if window_output_frames >= window_size {
                        let fill_now = (consumer.occupied_len() / channels) as i64;
                        let fill_err = fill_now - target as i64;
                        let in_snap = input_clock_cb.read();
                        let out_host_ns =
                            Instant::now().duration_since(epoch).as_nanos() as u64;
                        if target == window_start_target {
                            if let Some((in_frames, in_host_ns)) = in_snap {
                                if let Some(prev) = last_snapshot {
                                    let d_in_f = in_frames.wrapping_sub(prev.0) as f64;
                                    let d_in_ns = in_host_ns.wrapping_sub(prev.1) as f64;
                                    let d_out_f =
                                        output_frames_total.wrapping_sub(prev.2) as f64;
                                    let d_out_ns = out_host_ns.wrapping_sub(prev.3) as f64;
                                    if d_in_ns > 0.0 && d_out_ns > 0.0 && d_out_f > 0.0 {
                                        let input_rate = d_in_f / d_in_ns;
                                        let output_rate = d_out_f / d_out_ns;
                                        let observed = (input_rate / output_rate) as f32;
                                        let gain = SCHEDULE[sched_idx].1;
                                        nominal_step = (gain * observed
                                            + (1.0 - gain) * nominal_step)
                                            .clamp(
                                                1.0 - MAX_NOMINAL_DEV,
                                                1.0 + MAX_NOMINAL_DEV,
                                            );
                                        eprintln!(
                                            "audioout: clock ratio = {:.6} (obs {:.6}, fill_err {:+} over {} out)",
                                            nominal_step, observed, fill_err, window_output_frames
                                        );
                                    }
                                }
                                last_snapshot = Some((
                                    in_frames,
                                    in_host_ns,
                                    output_frames_total,
                                    out_host_ns,
                                ));
                            }
                        } else {
                            eprintln!(
                                "audioout: target changed during window; skipping ratio update"
                            );
                            // Don't anchor: keep waiting for a clean window.
                        }
                        window_start_target = target;
                        window_output_frames = 0;
                        if sched_idx + 1 < SCHEDULE.len() {
                            sched_idx += 1;
                        }
                        window_size = (sample_rate as u64) * SCHEDULE[sched_idx].0;
                    }
                },
                |e| eprintln!("Output error: {e}"),
                None,
            )
            .unwrap();

        stream.play().unwrap();
        // input_clock is now owned externally (written from BlackHole's input
        // callback). epoch only needs to live in the output callback closure,
        // captured above by reference.
        let _ = epoch;
        AudioOut {
            _stream: stream,
            producer,
            channels,
            sample_rate,
            push_count: 0,
            target_delay_frames,
        }
    }

    pub fn target_handle(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.target_delay_frames)
    }

    pub fn sample_rate(&self) -> usize {
        self.sample_rate
    }

    pub fn push(&mut self, samples: &[i16]) {
        const GAIN: f32 = 1.0;
        let mut peak: i16 = 0;
        for s in samples {
            if s.unsigned_abs() as i16 > peak.unsigned_abs() as i16 {
                peak = *s;
            }
            let f = (*s as f32 / i16::MAX as f32 * GAIN).clamp(-1.0, 1.0);
            for _ in 0..self.channels {
                self.producer.try_push(f).ok();
            }
        }
        self.push_count += 1;
        if self.push_count % 100 == 0 {
            let target = self.target_delay_frames.load(Ordering::Relaxed);
            // println!(
            //     "audioout: {} pushes, peak={}, free={}, target_delay={} frames ({} ms)",
            //     self.push_count,
            //     peak,
            //     self.producer.vacant_len(),
            //     target,
            //     target * 1000 / self.sample_rate,
            // );
        }
    }
}
