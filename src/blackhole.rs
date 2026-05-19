use crate::audioout::ClockSnapshot;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Stream, StreamError};
use ringbuf::{HeapCons, HeapProd, HeapRb, traits::*};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

static DROPPED_SAMPLES: AtomicUsize = AtomicUsize::new(0);

pub struct Blackhole {
    device: cpal::Device,
    config: cpal::StreamConfig,
    rb_prod: Arc<Mutex<HeapProd<f32>>>,
    channels: usize,
}

// Error Handling
fn buffer_err(err: StreamError) {
    println!("Error: {}", err.to_string());
}

// data callback
// called repeatedly by the audio driver whenever a new Chunk of PCM Audio is ready.
fn buffer_audio(data: &[f32], _info: &cpal::InputCallbackInfo, rb_prod: &mut HeapProd<f32>) {
    let pushed = rb_prod.push_slice(data);
    if pushed < data.len() {
        let prev = DROPPED_SAMPLES.fetch_add(data.len() - pushed, Ordering::Relaxed);
        let new_total = prev + (data.len() - pushed);
        // Log on power-of-two milestones to avoid spam.
        if new_total.is_power_of_two() || prev / 10000 != new_total / 10000 {
            eprintln!(
                "blackhole: input ring overflow — dropped {} samples total",
                new_total
            );
        }
    }
}

impl Blackhole {
    pub fn new() -> (Blackhole, HeapCons<f32>) {
        let host = cpal::default_host();

        // choosing a device, looking for blackhole
        let device = host
            .input_devices()
            // cannot find any devices
            .expect("No Input Devices Found")
            .find(|d| {
                // using map to find the input device named Blackhole
                d.name()
                    .map(|name| name.contains("BlackHole"))
                    .unwrap_or(false)
            })
            .expect("Blackhole Not Found");

        // setting up Stream Config
        // we ask the device or its default configuration and adopt them
        let supported_stream_config = device
            .default_input_config()
            .expect("Could not get Deffault Config from Blackhole");

        // Quick checks
        // println!("Device Name: {:?}", device.description());
        println!("Sample Rate: {}", supported_stream_config.sample_rate());
        println!("Sample Format: {}", supported_stream_config.sample_format());
        println!("Channels: {}", supported_stream_config.channels());

        let channels = supported_stream_config.channels() as usize;
        // convert from SupportedStreamConfig to a Stream Config
        let config: cpal::StreamConfig = supported_stream_config.into();

        /*
         * Given 48 kHz stereo streaming from BlackHole, the buffer holds
         *   ms = capacity / (sample_rate * channels) * 1000
         * 32768 samples stereo @ 48 kHz ≈ 340 ms of headroom — enough to
         * absorb GC/scheduler pauses on the consumer thread.
         */
        let rb = HeapRb::<f32>::new(32768);
        let (producer, consumer) = rb.split();

        let blackhole = Blackhole {
            device,
            config,
            rb_prod: Arc::new(Mutex::new(producer)),
            channels,
        };
        (blackhole, consumer)
    }

    // use a Ring Buffer to get load the audio
    pub fn build_stream(
        &self,
        input_clock: Arc<ClockSnapshot>,
    ) -> Result<Stream, cpal::BuildStreamError> {
        let prod = Arc::clone(&self.rb_prod);
        let channels = self.channels;
        // The input callback fires once per hardware capture chunk. Timestamp
        // taken here is much closer to actual capture time than what the
        // consumer thread observes downstream, removing the ~10–20 ms jitter
        // that biased the clock-ratio estimator.
        let epoch = Instant::now();
        let mut frames_total: u64 = 0;
        let stream = self.device.build_input_stream(
            &self.config,
            move |data: &[f32], info: &cpal::InputCallbackInfo| {
                if let Ok(mut p) = prod.lock() {
                    buffer_audio(data, info, &mut p);
                }
                frames_total += (data.len() / channels) as u64;
                let host_nanos = epoch.elapsed().as_nanos() as u64;
                input_clock.write(frames_total, host_nanos);
            },
            |err: StreamError| buffer_err(err),
            Some(Duration::from_millis(10)),
        )?;

        Ok(stream)
    }
}
