//
//  RTPReceiver.swift
//  iPad side of STEREO. Receives L16 mono RTP on UDP/5004, plays through
//  AVAudioEngine, and sends RTCP Receiver Reports back on UDP/5005 so the
//  Rust sender can compute RTT and retarget its local-speaker delay.
//
//  Drop this file into an iOS/iPadOS app. Call `RTPReceiver().start()`
//  somewhere after the app has finished launching. Requires the app to
//  configure an AVAudioSession (e.g. .playback) before start().
//
//  RTP:  L16 mono, 48 kHz, 960 samples/packet (20 ms), payload type 11.
//  RTCP: SR (PT=200) inbound, RR (PT=201) outbound every 5 s.
//

import AVFoundation
import Foundation
import Network

final class RTPReceiver {
    // MARK: - Audio
    private let sampleRate: Double = 48_000
    private let engine = AVAudioEngine()
    private let player = AVAudioPlayerNode()
    private let format: AVAudioFormat

    // MARK: - Networking
    private let rtpPort: NWEndpoint.Port = 5004
    private let rtcpPort: NWEndpoint.Port = 5005
    private var rtpListener: NWListener?
    private var rtcpListener: NWListener?
    /// Connection back to the sender's RTCP source port — we capture it on the
    /// first inbound SR and reuse it to send RRs.
    private var rtcpConn: NWConnection?
    private let queue = DispatchQueue(label: "rtp.receiver")

    // MARK: - SR/RR state
    private let ownSSRC: UInt32 = 0xCAFE_BABE
    private var senderSSRC: UInt32 = 0
    /// Middle 32 bits of the most recent SR's NTP timestamp.
    private var lastSrLsr: UInt32 = 0
    /// Mach uptime (seconds) when that SR arrived.
    private var lastSrArrival: TimeInterval = 0

    // RFC 3550 A.8 jitter
    private var lastTransit: Int64 = 0
    private var jitter: Double = 0

    // Sequence / loss bookkeeping
    private var baseSeq: UInt32 = 0
    private var maxSeq: UInt32 = 0
    private var cycles: UInt32 = 0
    private var received: UInt32 = 0
    private var expectedPrior: UInt32 = 0
    private var receivedPrior: UInt32 = 0
    private var seqInitialized = false

    private var rrTimer: DispatchSourceTimer?

    init() {
        format = AVAudioFormat(commonFormat: .pcmFormatFloat32,
                               sampleRate: sampleRate,
                               channels: 1,
                               interleaved: false)!
        engine.attach(player)
        engine.connect(player, to: engine.mainMixerNode, format: format)
    }

    func start() throws {
        try engine.start()
        player.play()

        let params = NWParameters.udp
        params.allowLocalEndpointReuse = true

        rtpListener = try NWListener(using: params, on: rtpPort)
        rtpListener?.newConnectionHandler = { [weak self] conn in
            guard let self else { return }
            conn.start(queue: self.queue)
            self.receive(on: conn, isRTP: true)
        }
        rtpListener?.start(queue: queue)

        rtcpListener = try NWListener(using: params, on: rtcpPort)
        rtcpListener?.newConnectionHandler = { [weak self] conn in
            guard let self else { return }
            // Remember this peer so we can send RRs back on the same flow.
            self.rtcpConn = conn
            conn.start(queue: self.queue)
            self.receive(on: conn, isRTP: false)
        }
        rtcpListener?.start(queue: queue)

        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + 5, repeating: 5)
        t.setEventHandler { [weak self] in self?.sendRR() }
        t.resume()
        rrTimer = t
    }

    // MARK: - Receive loops

    private func receive(on conn: NWConnection, isRTP: Bool) {
        conn.receiveMessage { [weak self] data, _, _, error in
            guard let self else { return }
            if let data, !data.isEmpty {
                if isRTP { self.handleRTP(data) } else { self.handleRTCP(data) }
            }
            if error == nil {
                self.receive(on: conn, isRTP: isRTP)
            }
        }
    }

    // MARK: - RTP

    private func handleRTP(_ data: Data) {
        guard data.count >= 12 else { return }
        let seq = UInt16(data[2]) << 8 | UInt16(data[3])
        let ts = u32(data, 4)
        senderSSRC = u32(data, 8)

        // L16 payload = big-endian Int16 mono samples.
        let payload = data.advanced(by: 12)
        let sampleCount = payload.count / 2
        guard sampleCount > 0,
              let buffer = AVAudioPCMBuffer(pcmFormat: format,
                                            frameCapacity: AVAudioFrameCount(sampleCount))
        else { return }
        buffer.frameLength = AVAudioFrameCount(sampleCount)
        let dst = buffer.floatChannelData![0]
        payload.withUnsafeBytes { raw in
            let p = raw.bindMemory(to: UInt8.self).baseAddress!
            for i in 0..<sampleCount {
                let hi = UInt16(p[2 * i])
                let lo = UInt16(p[2 * i + 1])
                let s = Int16(bitPattern: (hi << 8) | lo)
                dst[i] = Float(s) / Float(Int16.max)
            }
        }
        player.scheduleBuffer(buffer, completionHandler: nil)

        // Sequence tracking
        updateSeq(seq: seq)
        // Jitter per RFC 3550 A.8. The receiver clock here is the local audio
        // clock approximated by mach uptime; we only care about deltas, so the
        // absolute offset doesn't matter.
        let arrival = Int64(ProcessInfo.processInfo.systemUptime * sampleRate)
        let transit = arrival - Int64(ts)
        if lastTransit != 0 {
            let d = abs(transit - lastTransit)
            jitter += (Double(d) - jitter) / 16.0
        }
        lastTransit = transit
        received &+= 1
    }

    // MARK: - RTCP

    private func handleRTCP(_ data: Data) {
        guard data.count >= 28 else { return }
        let pt = data[1]
        guard pt == 200 else { return } // we only consume SRs
        let ntpHi = u32(data, 8)
        let ntpLo = u32(data, 12)
        // Middle 32 bits of NTP = low-16(hi) || high-16(lo)
        lastSrLsr = (ntpHi << 16) | (ntpLo >> 16)
        lastSrArrival = ProcessInfo.processInfo.systemUptime
    }

    private func sendRR() {
        guard lastSrLsr != 0, let conn = rtcpConn else { return }

        // Build a single-block RR. Length in 32-bit words minus 1: 32/4 - 1 = 7.
        var buf = Data(count: 32)
        buf[0] = 0x81                 // V=2, P=0, RC=1
        buf[1] = 201                  // PT = RR
        write16(&buf, 2, 7)           // length

        write32(&buf, 4, ownSSRC)     // reporter SSRC
        write32(&buf, 8, senderSSRC)  // source SSRC

        // fraction lost (1) + cumulative lost (3, signed)
        let expected = (maxSeq &- baseSeq) &+ 1
        let lost = Int32(bitPattern: expected &- received)
        let expectedInterval = expected &- expectedPrior
        let receivedInterval = received &- receivedPrior
        let lostInterval = Int32(bitPattern: expectedInterval &- receivedInterval)
        let fraction: UInt8
        if expectedInterval == 0 || lostInterval <= 0 {
            fraction = 0
        } else {
            fraction = UInt8(truncatingIfNeeded: (lostInterval << 8) / Int32(expectedInterval))
        }
        expectedPrior = expected
        receivedPrior = received
        buf[12] = fraction
        let lostClamped = max(min(lost, 0x7FFFFF), -0x800000)
        buf[13] = UInt8((lostClamped >> 16) & 0xFF)
        buf[14] = UInt8((lostClamped >> 8) & 0xFF)
        buf[15] = UInt8(lostClamped & 0xFF)

        write32(&buf, 16, maxSeq)                       // extended highest seq
        write32(&buf, 20, UInt32(jitter))               // interarrival jitter
        write32(&buf, 24, lastSrLsr)                    // LSR
        let dlsr = UInt32((ProcessInfo.processInfo.systemUptime - lastSrArrival) * 65536)
        write32(&buf, 28, dlsr)                         // DLSR (1/65536 sec)

        conn.send(content: buf, completion: .contentProcessed { _ in })
    }

    // MARK: - Helpers

    private func updateSeq(seq: UInt16) {
        let s = UInt32(seq)
        if !seqInitialized {
            baseSeq = s
            maxSeq = s
            seqInitialized = true
            return
        }
        let prev = maxSeq & 0xFFFF
        if s < prev && (prev - s) > 0x8000 {
            cycles &+= 0x10000
        }
        maxSeq = (cycles & 0xFFFF_0000) | s
    }

    private func u32(_ d: Data, _ o: Int) -> UInt32 {
        return (UInt32(d[o]) << 24)
            | (UInt32(d[o + 1]) << 16)
            | (UInt32(d[o + 2]) << 8)
            | UInt32(d[o + 3])
    }

    private func write16(_ d: inout Data, _ o: Int, _ v: UInt16) {
        d[o] = UInt8(v >> 8)
        d[o + 1] = UInt8(v & 0xFF)
    }

    private func write32(_ d: inout Data, _ o: Int, _ v: UInt32) {
        d[o] = UInt8((v >> 24) & 0xFF)
        d[o + 1] = UInt8((v >> 16) & 0xFF)
        d[o + 2] = UInt8((v >> 8) & 0xFF)
        d[o + 3] = UInt8(v & 0xFF)
    }
}
