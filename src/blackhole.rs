use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Stream, StreamError};
use ringbuf::{HeapCons, HeapProd, HeapRb, traits::*};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct Blackhole {
    device: cpal::Device,
    config: cpal::StreamConfig,
    rb_prod: Arc<Mutex<HeapProd<f32>>>,
}

// Error Handling
fn buffer_err(err: StreamError) {
    println!("Error: {}", err.to_string());
}

// data callback
// called repeatedly by the audio driver whenever a new Chunk of PCM Audio is ready.
fn buffer_audio(data: &[f32], _info: &cpal::InputCallbackInfo, rb_prod: &mut HeapProd<f32>) {
    // Push the PCM Audio Chunk into rb
    rb_prod.push_slice(data);
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

        // convert from SupportedStreamConfig to a Stream Config
        let config = supported_stream_config.into();

        /*
         * Given 48KHz Streaming from Blackhole,
         *
         * buffer = (1 / 48 kHz) * No of Samples
         *
         * a Buffer of 2048 Samples = 42 ms
         * 4096 Samples = 85ms
         */
        let rb = HeapRb::<f32>::new(4096);
        let (producer, consumer) = rb.split();

        let blackhole = Blackhole {
            device,
            config,
            rb_prod: Arc::new(Mutex::new(producer)),
        };
        (blackhole, consumer)
    }

    // use a Ring Buffer to get load the audio
    pub fn build_stream(&self) -> Result<Stream, cpal::BuildStreamError> {
        let prod = Arc::clone(&self.rb_prod);
        // Get the PCM audio Bytes from Blackhole
        let stream = self.device.build_input_stream(
            &self.config,
            move |data: &[f32], _info: &cpal::InputCallbackInfo| {
                if let Ok(mut p) = prod.lock() {
                    buffer_audio(data, _info, &mut p);
                }
            },
            |err: StreamError| buffer_err(err),
            Some(Duration::from_millis(10)),
        )?;

        Ok(stream)
    }
}
