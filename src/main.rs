mod audioout;
mod blackhole;
mod encoder;
mod playout;
mod rtp;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use cpal::traits::StreamTrait;
use ringbuf::traits::*;

fn main() {
    let (blackhole, mut rb_cons) = blackhole::Blackhole::new();

    // Shared input-clock snapshot. BlackHole's input callback writes it from
    // close to the actual hardware capture instant; AudioOut's output callback
    // reads it to compute the input↔output clock ratio.
    let input_clock = Arc::new(audioout::ClockSnapshot::new());
    let input_clock_for_blackhole = Arc::clone(&input_clock);

    // Flipped by playout.rs on the first valid STER packet from the iPad.
    // The consumer thread blocks on this before popping audio so we don't
    // burn capture into a buffer the iPad isn't yet draining, which would
    // produce a large positive fill overshoot that takes minutes to correct.
    let ipad_ready = Arc::new(AtomicBool::new(false));
    let ipad_ready_for_rtp = Arc::clone(&ipad_ready);

    /*
     * starts CPAL and start streaming the input
     */
    thread::spawn(move || {
        let stream = match blackhole.build_stream(input_clock_for_blackhole) {
            Ok(s) => s,
            Err(e) => {
                println!("Failed to Build Stream: {e}");
                return;
            }
        };
        stream.play().expect("Failed to play stream");
        thread::park();
    });

    let consumer = thread::spawn(move || {
        let mut chunk = vec![0f32; 960];

        // Starting delay. The RTCP-RR loop will retarget this once the iPad
        // reports back; AudioOut's drift loop converges the actual ring-buffer
        // fill toward whatever target is current.
        const INITIAL_SYNC_DELAY_MS: u32 = 200;

        let audio_out = audioout::AudioOut::new(INITIAL_SYNC_DELAY_MS, input_clock);
        let mut rtp = rtp::RtpSender::new(
            "192.168.0.83:5004",
            audio_out.target_handle(),
            audio_out.sample_rate() as u32,
            ipad_ready_for_rtp,
        );
        let mut audio_out = audio_out;

        // Bootstrap probe: send one packet of silence so the iPad learns the
        // Mac's IP from the UDP source address. Without this the iPad has no
        // outbound destination and can't send STER — three-way deadlock with
        // our ipad_ready gate. 480 samples = 10 ms of silence, inaudible.
        let silence = vec![0i16; 480];
        rtp.send(&silence);
        println!("waiting for iPad to come online…");
        // While waiting, keep draining BlackHole so its ring doesn't overflow.
        while !ipad_ready.load(Ordering::Acquire) {
            let stale = rb_cons.occupied_len();
            if stale > 0 {
                rb_cons.skip(stale);
            }
            thread::sleep(Duration::from_millis(20));
        }
        let stale = rb_cons.occupied_len();
        rb_cons.skip(stale);
        println!(
            "iPad ready — dropped {} stale samples from capture ring, starting stream",
            stale
        );

        loop {
            if rb_cons.occupied_len() >= 960 {
                rb_cons.pop_slice(&mut chunk);
                let l16_frame = encoder::encodel16(&chunk);

                // RTP
                rtp.send(&l16_frame.left); // Left Channel to Ipad
                audio_out.push(&l16_frame.right); // right Channel to Speakers
            } else {
                thread::sleep(Duration::from_millis(1));
            }
        }
    });
    consumer.join().expect("cosumer thread panicked");
}
