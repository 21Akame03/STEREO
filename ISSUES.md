# Issues Found

## Fixed

### 1. `UdpSocket::bind` rejected address — `src/rtp.rs:15`
- **Before:** `UdpSocket::bind("0.0.0.0").unwrap()`
- **Problem:** Missing port → `InvalidInput: invalid socket address`. Panicked the consumer thread on startup.
- **Fix:** `UdpSocket::bind("0.0.0.0:0")` — lets the OS pick an ephemeral port.

### 2. RTP destination placeholder — `src/main.rs:33`
- **Before:** `RtpSender::new("192.168.1.X:5004")`
- **Problem:** `X` is not a valid IP octet → DNS lookup fails (`nodename nor servname provided, or not known`).
- **Fix:** Replace with the actual iPad's IP, e.g. `"192.168.1.42:5004"`.

### 3. AudioOut hardcoded mono / sample rate — `src/audioout.rs`
- **Before:** `StreamConfig { channels: 1, sample_rate: 48000, ... }` against `default_output_device()`.
- **Problem:** On macOS the default output is stereo; CoreAudio won't auto-downmix and produced a silent stream.
- **Fix:** Adopt the device's `default_output_config()`, store the channel count, and duplicate each mono sample across all output channels in `push()`.

### 4. Right channel reading left — `src/encoder.rs:12`
- **Before:** `right.push((chunk[0].clamp(...) ...))`
- **Problem:** Both `left` and `right` were built from `chunk[0]`, so the "right channel" sent to speakers was actually left-channel audio.
- **Fix:** `right.push((chunk[1].clamp(...) ...))`.

### 5. Output device was BlackHole (loopback) — `src/audioout.rs`
- **Before:** `host.default_output_device()` returned `BlackHole 2ch` because BlackHole was the system default output.
- **Problem:** Audio captured from BlackHole was being pushed back into BlackHole → nothing reached real speakers.
- **Fix:** Enumerate `host.output_devices()` and pick the device whose name contains `"MacBook Pro Speakers"`, ignoring the system default.

## Outstanding warnings (not bugs, worth cleaning up)

### 6. Unused import — `src/blackhole.rs:1`
- `StreamTrait` is imported but unused. Remove it from the `use` list.

### 7. Deprecated `DeviceTrait::name` — `src/blackhole.rs:36`
- cpal recommends `description()` for human-readable info or `id()` for stable identifiers. `name()` still works but emits a deprecation warning.

## Setup / environment notes (not code bugs)

- **macOS audio routing:** for this program to be useful, the system output must be set to **BlackHole 2ch** (so other apps route audio into it for capture). To also hear audio normally, create a **Multi-Output Device** in Audio MIDI Setup containing BlackHole + your real speakers and use that as the system output.
- **Hard-coded output device name:** `"MacBook Pro Speakers"` is currently hardcoded. If you run this on another Mac or with headphones plugged in, the lookup will fail. Consider making it configurable.
- **No runtime device-change handling:** the output device is bound at startup; plugging in headphones later won't switch playback.
