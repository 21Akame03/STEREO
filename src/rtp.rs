use bytes::Bytes;
use rtp::header::Header;
use rtp::packet::Packet;
use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use webrtc_util::Marshal;

const SSRC: u32 = 0x1234;
const RTCP_INTERVAL: Duration = Duration::from_secs(5);
// Seconds between the NTP epoch (1900-01-01) and the Unix epoch (1970-01-01).
const NTP_UNIX_OFFSET: u64 = 2_208_988_800;

pub struct SenderStats {
    packet_count: AtomicU32,
    octet_count: AtomicU32,
    // (ntp_timestamp, rtp_timestamp) captured at the moment the most recent
    // RTP packet was handed to the kernel. The RTCP thread uses this pair to
    // tell the receiver "RTP time R corresponded to wall-clock time T".
    pub last_pair: Mutex<(u64, u32)>,
    // Middle-32-bits of the last SR's NTP, plus the Instant it was sent.
    // The recv loop matches against the LSR field in incoming RRs to compute RTT.
    last_sr: Mutex<Option<(u32, Instant)>>,
    // Last-measured RTT in nanoseconds. Updated by rtcp_recv_loop, read by
    // the playout-report loop to estimate one-way delay (≈ RTT/2).
    pub last_rtt_nanos: AtomicU64,
}

pub struct RtpSender {
    socket: UdpSocket,
    seq: u16,
    timestamp: u32,
    stats: Arc<SenderStats>,
}

impl RtpSender {
    /// `target_delay_frames` is shared with `AudioOut`; the RTCP-RR receive loop
    /// updates it based on measured RTT + remote jitter so the local-speaker
    /// delay tracks the iPad's actual playout latency.
    pub fn new(
        dest: &str,
        target_delay_frames: Arc<AtomicUsize>,
        sample_rate: u32,
        ipad_ready: Arc<AtomicBool>,
    ) -> Self {
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        socket.connect(dest).unwrap();

        // RTCP runs on RTP port + 1 (RFC 3550 convention).
        let rtcp_dest = bump_port(dest);
        let rtcp_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").unwrap());
        rtcp_socket.connect(&rtcp_dest).unwrap();

        let stats = Arc::new(SenderStats {
            packet_count: AtomicU32::new(0),
            octet_count: AtomicU32::new(0),
            last_pair: Mutex::new((0, 0)),
            last_sr: Mutex::new(None),
            last_rtt_nanos: AtomicU64::new(0),
        });

        // SR sender
        {
            let stats = Arc::clone(&stats);
            let sock = Arc::clone(&rtcp_socket);
            thread::spawn(move || rtcp_send_loop(sock, stats));
        }
        // RR receiver
        {
            let stats = Arc::clone(&stats);
            let sock = Arc::clone(&rtcp_socket);
            let target = Arc::clone(&target_delay_frames);
            thread::spawn(move || rtcp_recv_loop(sock, stats, target, sample_rate));
        }
        // iPad playout-report receiver (overrides target_delay_frames with a
        // direct measurement of the iPad's actual playout delay).
        crate::playout::spawn(
            sample_rate,
            Arc::clone(&target_delay_frames),
            Arc::clone(&stats),
            ipad_ready,
        );

        RtpSender {
            socket,
            seq: 0,
            timestamp: 0,
            stats,
        }
    }

    pub fn send(&mut self, samples: &[i16]) {
        // build payload bytes
        let mut payload = Vec::with_capacity(samples.len() * 2);
        for s in samples {
            payload.extend_from_slice(&s.to_be_bytes());
        }

        let packet = Packet {
            header: Header {
                version: 2,
                payload_type: 11, // L16 mono
                sequence_number: self.seq,
                timestamp: self.timestamp,
                ssrc: SSRC,
                ..Default::default()
            },
            payload: Bytes::from(payload),
        };

        let buf = packet.marshal().unwrap();
        self.socket.send(&buf).ok();

        // Snapshot the (wall clock, RTP clock) pair *after* the kernel hand-off
        // so the SR maps the timestamp that was just on the wire.
        let ntp_now = ntp_timestamp_now();
        *self.stats.last_pair.lock().unwrap() = (ntp_now, self.timestamp);
        self.stats.packet_count.fetch_add(1, Ordering::Relaxed);
        // Octet count covers only the payload, per RFC 3550.
        self.stats
            .octet_count
            .fetch_add((samples.len() * 2) as u32, Ordering::Relaxed);

        self.seq = self.seq.wrapping_add(1);
        self.timestamp = self.timestamp.wrapping_add(samples.len() as u32);
    }
}

fn bump_port(dest: &str) -> String {
    let (host, port) = dest.rsplit_once(':').expect("dest must be host:port");
    let port: u16 = port.parse().expect("invalid port");
    format!("{host}:{}", port + 1)
}

fn ntp_timestamp_now() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs() + NTP_UNIX_OFFSET;
    // Fractional seconds in 1/2^32-second units.
    let frac = ((now.subsec_nanos() as u64) << 32) / 1_000_000_000;
    (secs << 32) | frac
}

fn rtcp_send_loop(socket: Arc<UdpSocket>, stats: Arc<SenderStats>) {
    loop {
        thread::sleep(RTCP_INTERVAL);
        let (last_ntp, last_rtp) = *stats.last_pair.lock().unwrap();
        if last_ntp == 0 {
            continue;
        }
        let pkts = stats.packet_count.load(Ordering::Relaxed);
        let octs = stats.octet_count.load(Ordering::Relaxed);
        let sr = build_sender_report(SSRC, last_ntp, last_rtp, pkts, octs);
        if socket.send(&sr).is_ok() {
            // LSR = middle 32 bits of the SR's NTP. Record send time so we
            // can compute RTT when the matching RR returns.
            let lsr = ((last_ntp >> 16) & 0xFFFF_FFFF) as u32;
            *stats.last_sr.lock().unwrap() = Some((lsr, Instant::now()));
        }
    }
}

fn rtcp_recv_loop(
    socket: Arc<UdpSocket>,
    stats: Arc<SenderStats>,
    target_delay_frames: Arc<AtomicUsize>,
    sample_rate: u32,
) {
    let mut buf = [0u8; 1500];
    loop {
        let n = match socket.recv(&mut buf) {
            Ok(n) => n,
            Err(_) => continue,
        };
        let Some((rtt, jitter_ts_units)) = parse_rr(&buf[..n], &stats) else {
            continue;
        };

        // Publish RTT for the playout-report loop.
        stats
            .last_rtt_nanos
            .store(rtt.as_nanos() as u64, Ordering::Relaxed);

        // Convert jitter from RTP-timestamp units into milliseconds.
        let jitter_ms = (jitter_ts_units as f64) * 1000.0 / sample_rate as f64;
        let rtt_ms = rtt.as_secs_f64() * 1000.0;

        // Target = one-way delay (~RTT/2) + headroom for jitter + a fixed margin
        // to cover the iPad's de-jitter buffer. Clamp so a bad reading can't
        // produce silly values. Only used as a fallback estimate — the
        // playout-report loop produces a more accurate target when reports
        // are arriving from the iPad.
        let target_ms = (rtt_ms / 2.0 + 3.0 * jitter_ms + 30.0).clamp(50.0, 500.0);
        let target_frames = (target_ms * sample_rate as f64 / 1000.0) as i64;

        // Smooth: nudge by 25% each report to avoid step changes.
        let prev = target_delay_frames.load(Ordering::Relaxed) as i64;
        let next = (prev + (target_frames - prev) / 4).max(0) as usize;
        target_delay_frames.store(next, Ordering::Relaxed);

        println!(
            "rtcp: rtt={:.1}ms jitter={:.2}ms target={:.0}ms (frames {} -> {})",
            rtt_ms, jitter_ms, target_ms, prev, next
        );
    }
}

/// Parse a compound RTCP packet looking for an RR (PT=201) whose LSR matches
/// the last SR we sent. Returns (RTT, interarrival jitter in RTP-ts units).
fn parse_rr(buf: &[u8], stats: &SenderStats) -> Option<(Duration, u32)> {
    if buf.len() < 8 {
        return None;
    }
    let v = buf[0] >> 6;
    let rc = (buf[0] & 0x1F) as usize;
    let pt = buf[1];
    if v != 2 || pt != 201 || rc == 0 {
        return None;
    }
    // Fixed RR header: 4 (hdr) + 4 (reporter SSRC) = 8 bytes; then 24-byte blocks.
    let block = 8;
    if buf.len() < block + 24 {
        return None;
    }
    let b = &buf[block..block + 24];
    let jitter = u32::from_be_bytes([b[8], b[9], b[10], b[11]]);
    let lsr = u32::from_be_bytes([b[12], b[13], b[14], b[15]]);
    let dlsr = u32::from_be_bytes([b[16], b[17], b[18], b[19]]);

    let last_sr = *stats.last_sr.lock().unwrap();
    let (expected_lsr, sent_at) = last_sr?;
    if lsr != expected_lsr {
        return None;
    }
    // DLSR is in units of 1/65536 seconds.
    let dlsr_dur = Duration::from_nanos((dlsr as u64) * 1_000_000_000 / 65536);
    let rtt = sent_at.elapsed().checked_sub(dlsr_dur)?;
    Some((rtt, jitter))
}

// Build an RTCP Sender Report (PT=200) with no report blocks (RC=0).
// Total length = 28 bytes. See RFC 3550 §6.4.1.
fn build_sender_report(
    ssrc: u32,
    ntp_ts: u64,
    rtp_ts: u32,
    packet_count: u32,
    octet_count: u32,
) -> [u8; 28] {
    let mut buf = [0u8; 28];
    // V=2, P=0, RC=0
    buf[0] = 0b10_0_00000;
    // PT = 200 (Sender Report)
    buf[1] = 200;
    // length in 32-bit words minus 1 = (28/4) - 1 = 6
    buf[2..4].copy_from_slice(&6u16.to_be_bytes());
    buf[4..8].copy_from_slice(&ssrc.to_be_bytes());
    buf[8..16].copy_from_slice(&ntp_ts.to_be_bytes());
    buf[16..20].copy_from_slice(&rtp_ts.to_be_bytes());
    buf[20..24].copy_from_slice(&packet_count.to_be_bytes());
    buf[24..28].copy_from_slice(&octet_count.to_be_bytes());
    buf
}
