mod blackhole;
use bytes::Bytes;
use cpal::Host;
use rtp::packet::Packet;

fn main() {
    let blackhole = blackhole::Blackhole::new();
}
