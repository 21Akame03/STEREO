//! Playout-report receiver.
//!
//! Listens for periodic UDP reports from the iPad describing what it is
//! currently playing, and uses those reports to set `target_delay_frames`
//! so that the local speakers' playout aligns with the iPad's DAC output.
//!
//! Wire format (big-endian, 24 bytes):
//!   0..4   magic               = 0x53544552 ("STER")
//!   4      version             = 1
//!   5..8   pad                 = 0
//!   8..16  ipad_ntp            u64 NTP timestamp at moment of measurement
//!   16..20 last_rtp_played     u32 RTP timestamp of most recent sample to DAC
//!   20..24 dejitter_depth      u32 iPad de-jitter buffer fill (samples)
//!
//! ## Clock-offset estimation (v2)
//!
//! Each report gives us (ipad_ntp at measurement, mac_ntp at recv, RTT).
//! Single-sample one-way delay ≈ RTT/2 is biased on asymmetric paths, but
//! the sample with the *lowest* RTT in a recent window has the most
//! symmetric path. We use min-RTT filtering across the last few seconds
//! of reports to produce `offset = mac_ntp − ipad_ntp` with bias bounded
//! by the asymmetry of the best-observed packet, not every packet.
//!
//! Drift slope (mac_clock_rate / ipad_clock_rate − 1) is fit by linear
//! regression on the same window and logged so we can see if the iPad's
//! DAC is running fast or slow. We don't currently use it to predict
//! future offset — at 4 Hz reports the EMA absorbs it.

use crate::rtp::SenderStats;
use std::collections::VecDeque;
use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

pub const PORT: u16 = 5005;
const MAGIC: u32 = 0x5354_4552;
const PROTO_VERSION: u8 = 1;
const NTP_UNIX_OFFSET: u64 = 2_208_988_800;
const WINDOW_SAMPLES: usize = 40; // ~10 s at 4 Hz reports
const MIN_SAMPLES_FOR_DRIFT: usize = 8;

/// Compensation for the asymmetric DAC pipelines on the two devices.
/// `d_secs` is measured from "Mac sent RTP" to "iPad reported it as played"
/// (i.e. handed to iPad DAC). It doesn't include the iPad's DAC→air latency
/// and doesn't subtract the Mac's ring→air latency. Net: Mac plays early by
/// roughly (ipad_dac − mac_dac). Bump this constant to push Mac later until
/// Mac and iPad speakers align by ear. Positive = add delay on Mac side.
/// Override at runtime with env STEREO_PLAYOUT_OFFSET_MS=<float>.
const DEFAULT_PLAYOUT_OFFSET_MS: f64 = 0.0;

fn ntp_now() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs() + NTP_UNIX_OFFSET;
    let frac = ((now.subsec_nanos() as u64) << 32) / 1_000_000_000;
    (secs << 32) | frac
}

fn ntp_to_secs(ntp: u64) -> f64 {
    (ntp >> 32) as f64 + ((ntp & 0xFFFF_FFFF) as f64) / (1u64 << 32) as f64
}

/// One observation: when the mac received a report, when the iPad measured,
/// and the RTT we currently believe is in effect (in seconds).
#[derive(Clone, Copy)]
struct Sample {
    mac_s: f64,
    ipad_s: f64,
    rtt_s: f64,
}

struct ClockEstimator {
    samples: VecDeque<Sample>,
}

impl ClockEstimator {
    fn new() -> Self {
        Self {
            samples: VecDeque::with_capacity(WINDOW_SAMPLES),
        }
    }

    fn push(&mut self, s: Sample) {
        if self.samples.len() == WINDOW_SAMPLES {
            self.samples.pop_front();
        }
        self.samples.push_back(s);
    }

    /// Best-symmetry offset estimate. Picks the sample with the lowest RTT
    /// (closest to a symmetric path) and assumes one-way = RTT/2 on that
    /// sample only.
    fn offset_s(&self) -> Option<f64> {
        self.samples
            .iter()
            .min_by(|a, b| a.rtt_s.partial_cmp(&b.rtt_s).unwrap())
            .map(|s| s.mac_s - s.ipad_s - s.rtt_s / 2.0)
    }

    /// Drift slope in parts-per-million: how fast (mac_clock − ipad_clock)
    /// grows per ipad-second. Positive = mac clock running faster.
    fn drift_ppm(&self) -> Option<f64> {
        if self.samples.len() < MIN_SAMPLES_FOR_DRIFT {
            return None;
        }
        // Normalize x to avoid catastrophic cancellation: NTP seconds are
        // ~4e9, squaring blows past f64 precision.
        let x0 = self.samples[0].ipad_s;
        let n = self.samples.len() as f64;
        let (mut sx, mut sy, mut sxx, mut sxy) = (0.0, 0.0, 0.0, 0.0);
        for s in &self.samples {
            let x = s.ipad_s - x0;
            let y = s.mac_s - s.ipad_s; // mac-ipad gap; slope is drift
            sx += x;
            sy += y;
            sxx += x * x;
            sxy += x * y;
        }
        let den = n * sxx - sx * sx;
        if den.abs() < 1e-12 {
            return None;
        }
        Some((n * sxy - sx * sy) / den * 1e6)
    }
}

pub fn spawn(
    sample_rate: u32,
    target_delay_frames: Arc<AtomicUsize>,
    stats: Arc<SenderStats>,
    ipad_ready: Arc<AtomicBool>,
) {
    let socket = match UdpSocket::bind(("0.0.0.0", PORT)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("playout: bind 0.0.0.0:{PORT} failed: {e}");
            return;
        }
    };
    println!("playout: listening on 0.0.0.0:{PORT}");

    let offset_ms: f64 = std::env::var("STEREO_PLAYOUT_OFFSET_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PLAYOUT_OFFSET_MS);
    println!("playout: pipeline compensation = {:+.1} ms", offset_ms);

    thread::spawn(move || {
        let mut buf = [0u8; 64];
        let mut estimator = ClockEstimator::new();
        let mut report_count: u64 = 0;
        let pipeline_offset_s = offset_ms / 1000.0;
        // Track the minimum dejitter depth seen between log prints — a dip
        // toward 0 means the iPad's playout buffer underran (audible as
        // glitches/dropouts on iPad).
        let mut min_depth_window: u32 = u32::MAX;

        loop {
            let n = match socket.recv(&mut buf) {
                Ok(n) => n,
                Err(_) => continue,
            };
            if n < 24 {
                continue;
            }
            let magic = u32::from_be_bytes(buf[0..4].try_into().unwrap());
            if magic != MAGIC || buf[4] != PROTO_VERSION {
                continue;
            }
            // Any valid STER packet means the iPad's listener is up. Used by
            // main to gate the start of BlackHole consumption / RTP sending,
            // so we don't burn capture into an absent receiver and overshoot
            // the buffer target before ASRC engages.
            if !ipad_ready.swap(true, Ordering::AcqRel) {
                println!("playout: iPad ready (first STER received)");
            }
            let ipad_ntp = u64::from_be_bytes(buf[8..16].try_into().unwrap());
            let last_rtp_played = u32::from_be_bytes(buf[16..20].try_into().unwrap());
            let dejitter_depth = u32::from_be_bytes(buf[20..24].try_into().unwrap());

            // Pre-sync sentinel: iPad sends zero-body STERs from boot so the
            // Mac can detect liveness and start streaming. These must not
            // feed the clock-offset estimator or RTT/drift logic.
            if last_rtp_played == 0 || dejitter_depth == 0 {
                continue;
            }

            let mac_recv_ntp = ntp_now();
            let rtt_ns = stats.last_rtt_nanos.load(Ordering::Relaxed);
            if rtt_ns == 0 {
                // Need at least one SR/RR exchange before we have RTT.
                continue;
            }

            estimator.push(Sample {
                mac_s: ntp_to_secs(mac_recv_ntp),
                ipad_s: ntp_to_secs(ipad_ntp),
                rtt_s: rtt_ns as f64 / 1e9,
            });

            let Some(offset_s) = estimator.offset_s() else {
                continue;
            };

            // Convert ipad_ntp → mac wall-clock via learned offset. This
            // replaces the per-report `mac_recv − RTT/2` estimate, removing
            // the bias contributed by any single report's network jitter
            // and by iPad-side measurement-to-send latency.
            let t_played_mac_s = ntp_to_secs(ipad_ntp) + offset_s;

            let (anchor_ntp, anchor_rtp) = *stats.last_pair.lock().unwrap();
            if anchor_ntp == 0 {
                continue;
            }
            // Extrapolate sender NTP for the RTP timestamp the iPad just
            // played. RTP advances at exactly sample_rate per second; signed
            // delta handles RTP wraparound.
            let delta_samples = last_rtp_played.wrapping_sub(anchor_rtp) as i32 as f64;
            let t_sent_mac_s = ntp_to_secs(anchor_ntp) + delta_samples / sample_rate as f64;

            let d_secs = t_played_mac_s - t_sent_mac_s + pipeline_offset_s;
            if !(0.005..=1.0).contains(&d_secs) {
                eprintln!(
                    "playout: ignoring report (d={:.1} ms, rtp={}, depth={})",
                    d_secs * 1000.0,
                    last_rtp_played,
                    dejitter_depth
                );
                continue;
            }

            let target_frames = (d_secs * sample_rate as f64) as i64;
            let prev = target_delay_frames.load(Ordering::Relaxed) as i64;
            let next = (prev + (target_frames - prev) / 4).max(0) as usize;
            target_delay_frames.store(next, Ordering::Relaxed);

            min_depth_window = min_depth_window.min(dejitter_depth);
            report_count += 1;
            // Log every ~2 s (every 8th report at 4 Hz) to keep stderr quiet.
            if report_count % 8 == 0 {
                let drift = estimator
                    .drift_ppm()
                    .map(|p| format!("{:+.1} ppm", p))
                    .unwrap_or_else(|| "—".to_string());
                let depth_ms =
                    (dejitter_depth as f64 / sample_rate as f64) * 1000.0;
                let min_depth_ms =
                    (min_depth_window as f64 / sample_rate as f64) * 1000.0;
                println!(
                    "playout: ipad delay={:.1}ms depth={} ({:.1}ms, min {:.1}ms) offset={:+.3}ms drift={} target={}",
                    d_secs * 1000.0,
                    dejitter_depth,
                    depth_ms,
                    min_depth_ms,
                    offset_s * 1000.0,
                    drift,
                    next
                );
                min_depth_window = u32::MAX;
            }
        }
    });
}
