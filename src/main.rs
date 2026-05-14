mod blackhole;

use std::thread;
use std::time::Duration;

use cpal::traits::StreamTrait;
use ringbuf::traits::*;

fn main() {
    let (blackhole, mut rb_cons) = blackhole::Blackhole::new();

    /*
     * starts CPAL and start streaming the input
     */
    thread::spawn(move || {
        let stream = match blackhole.build_stream() {
            Ok(s) => s,
            Err(e) => {
                println!("Failed to Build Stream: {e}");
                return;
            }
        };
        stream.play().expect("Failed to play stream");
        thread::park();
    });

    // consumer — temporary sanity loop, replace with real DSP later

    let consumer = thread::spawn(move || {
        loop {
            let n = rb_cons.occupied_len();
            if n > 0 {
                println!("samples available: {n}");
            }
            thread::sleep(Duration::from_millis(500));
        }
    });
    consumer.join().expect("cosumer thread panicked");
}
