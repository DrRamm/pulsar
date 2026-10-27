//! Raw S16LE on the ALSA `speakers` PCM (dmix mixes it with GStreamer). Home Assistant's pipeline
//! TTS, once voice feature SPEAKER is announced, is 16 kHz mono and is paced to stay ~384 ms
//! ahead of a device that starts playing at once. The playbin queue would hold that answer back.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use log::{error, info, warn};
use pulsar_alsa::{Format, HwRequest, PCM_STATE_PREPARED, Pcm, Stream};

use super::{Channel, PlayId, PlayState, Shared};

const QUEUE_CAP: usize = 16_000 * 4;
const WAIT_MS: i32 = 20;

enum Op {
    Start,
    Data,
    Eos,
    Abort,
    Quit,
}

struct Msg {
    op: Op,
    gid: u64,
    epoch: u64,
    ch: Channel,
    rate: i32,
    channels: i32,
    samples: Vec<i16>,
}

struct Q {
    msgs: VecDeque<Msg>,
    /// Samples sitting in DATA messages.
    queued: usize,
}

struct Queue {
    q: Mutex<Q>,
    cv: Condvar,
}

fn is_cut(op: &Op) -> bool {
    matches!(op, Op::Abort | Op::Start | Op::Quit)
}

impl Queue {
    fn new() -> Queue {
        Queue {
            q: Mutex::new(Q {
                msgs: VecDeque::new(),
                queued: 0,
            }),
            cv: Condvar::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Q> {
        self.q.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn post(&self, m: Msg) {
        let mut q = self.lock();
        if matches!(m.op, Op::Data) {
            // HA leads by ~384 ms. Anything past a couple of seconds means we are not draining.
            if q.queued + m.samples.len() > QUEUE_CAP {
                warn!(
                    "player: pcm queue full, dropping {} samples",
                    m.samples.len()
                );
                return;
            }
            q.queued += m.samples.len();
        }
        if matches!(m.op, Op::Abort | Op::Start) {
            let ch = m.ch;
            let mut dropped = 0;
            q.msgs.retain(|old| {
                let drop_it = matches!(old.op, Op::Data) && old.ch == ch;
                if drop_it {
                    dropped += old.samples.len();
                }
                !drop_it
            });
            q.queued = q.queued.saturating_sub(dropped);
        }
        q.msgs.push_back(m);
        drop(q);
        self.cv.notify_one();
    }

    fn pop(&self) -> Msg {
        let mut q = self.lock();
        while q.msgs.is_empty() {
            q = self.cv.wait(q).unwrap_or_else(PoisonError::into_inner);
        }
        let m = q.msgs.pop_front().expect("just checked");
        if matches!(m.op, Op::Data) {
            q.queued = q.queued.saturating_sub(m.samples.len());
        }
        m
    }

    fn interrupted(&self) -> bool {
        self.lock().msgs.front().is_some_and(|m| is_cut(&m.op))
    }

    /// Waits up to 20 ms. true = a newer command is at the front.
    fn wait_interrupt(&self) -> bool {
        let q = self.lock();
        if q.msgs.front().is_some_and(|m| is_cut(&m.op)) {
            return true;
        }
        let (q, _) = self
            .cv
            .wait_timeout(q, Duration::from_millis(20))
            .unwrap_or_else(PoisonError::into_inner);
        q.msgs.front().is_some_and(|m| is_cut(&m.op))
    }
}

fn close_pcm(pcm: &mut Option<Pcm>) {
    if let Some(mut p) = pcm.take() {
        let _ = p.drop_frames();
    }
}

/// 1 = wrote, 0 = a newer command is waiting, -1 = the device failed.
fn write_frames(pcm: &mut Pcm, q: &Queue, samples: &[i16], channels: usize) -> i32 {
    let frames = samples.len() / channels;
    if frames == 0 {
        return 1;
    }
    let mut raw = Vec::with_capacity(samples.len() * 2);
    for s in samples {
        raw.extend_from_slice(&s.to_le_bytes());
    }
    let frame_bytes = channels * 2;
    let mut off = 0;
    while off < frames {
        if q.interrupted() {
            return 0;
        }
        let avail = match pcm.avail_update() {
            Ok(n) => n,
            Err(e) => {
                if pcm.recover(e, true).is_err() {
                    return -1;
                }
                continue;
            }
        };
        if avail == 0 {
            match pcm.wait(WAIT_MS) {
                Ok(_) => continue,
                Err(e) => {
                    if pcm.recover(e, true).is_err() {
                        return -1;
                    }
                    continue;
                }
            }
        }
        let n = avail.min(frames - off);
        let start = off * frame_bytes;
        let end = (off + n) * frame_bytes;
        match pcm.writei(&raw[start..end], n) {
            Ok(w) => off += w,
            Err(e) => {
                if pcm.recover(e, true).is_err() {
                    return -1;
                }
            }
        }
    }
    1
}

/// false = interrupted. A short utterance never fills one period, so start it explicitly.
fn drain(pcm: &mut Pcm, q: &Queue) -> bool {
    if pcm.state() == PCM_STATE_PREPARED && pcm.start().is_err() {
        return true;
    }
    loop {
        if q.interrupted() {
            return false;
        }
        match pcm.delay() {
            Err(e) if e == -libc::EPIPE => {
                let _ = pcm.prepare();
                return true;
            }
            Err(_) => return true,
            Ok(d) if d <= 0 => return true,
            Ok(_) => {
                if q.wait_interrupt() {
                    return false;
                }
            }
        }
    }
}

fn open_pcm(rate: i32, channels: i32) -> Result<(Pcm, u64), i32> {
    let mut pcm = Pcm::open("speakers", Stream::Playback, false)?;
    let req = HwRequest {
        format: Format::S16Le,
        channels: u32::try_from(channels.max(1)).unwrap_or(1),
        rate: u32::try_from(rate.max(1)).unwrap_or(1),
        rate_exact: true,
        // Leave the plug/dmix buffer alone (forcing one fails on some of these PCMs). Playback
        // starts after a single period, not after the whole buffer.
        period_frames: None,
        buffer_frames: None,
    };
    let hw = match pcm.set_hw_params(&req) {
        Ok(hw) => hw,
        Err(e) => {
            let mut near = req;
            near.rate_exact = false;
            pcm.set_hw_params(&near).map_err(|_| e)?
        }
    };
    let period = if hw.period_frames == 0 {
        1
    } else {
        hw.period_frames
    };
    pcm.set_sw_params(period, period)?;
    pcm.prepare()?;
    Ok((pcm, period))
}

fn apply_gain(samples: &mut [i16], gain_bits: u32) {
    let g = f32::from_bits(gain_bits).clamp(0.0, 1.0);
    if g >= 0.999 {
        return;
    }
    for s in samples {
        let v = (f32::from(*s) * g).clamp(-32768.0, 32767.0);
        *s = v as i16;
    }
}

struct Run {
    shared: Arc<Shared>,
    q: Arc<Queue>,
}

impl Run {
    fn publish(&self, epoch: u64, ch: Channel, s: PlayState) {
        if self.shared.audio_epoch[ch.index()].load(Ordering::Acquire) != epoch {
            return;
        }
        self.shared.set_state(ch, s);
    }

    fn fail(&self, gid: u64, epoch: u64, ch: Channel) {
        self.shared.clear_pcm_if(gid);
        self.shared.complete(ch, PlayId(gid), false);
        self.publish(epoch, ch, PlayState::Error);
        self.publish(epoch, ch, PlayState::Idle);
    }

    fn run(&self) {
        let mut pcm: Option<Pcm> = None;
        let mut gid = 0u64;
        let mut epoch = 0u64;
        let mut ch = Channel::Announce;
        let mut channels = 1usize;
        let mut wrote = false;
        loop {
            let mut msg = self.q.pop();
            match msg.op {
                Op::Quit => {
                    close_pcm(&mut pcm);
                    return;
                }
                Op::Abort => {
                    if gid != 0 && msg.ch == ch {
                        close_pcm(&mut pcm);
                        gid = 0;
                        wrote = false;
                    }
                }
                Op::Start => {
                    close_pcm(&mut pcm);
                    wrote = false;
                    gid = msg.gid;
                    epoch = msg.epoch;
                    ch = msg.ch;
                    channels = usize::try_from(msg.channels.max(1)).unwrap_or(1);
                    match open_pcm(msg.rate, msg.channels) {
                        Ok((p, period)) => {
                            info!(
                                "player: ch{} pcm {} Hz/{} ch, period {period} frames",
                                ch.index(),
                                msg.rate,
                                channels
                            );
                            pcm = Some(p);
                            self.publish(epoch, ch, PlayState::Playing);
                        }
                        Err(e) => {
                            error!(
                                "player: ch{} pcm open failed ({e}) ({} Hz/{} ch)",
                                ch.index(),
                                msg.rate,
                                channels
                            );
                            self.fail(gid, epoch, ch);
                            gid = 0;
                        }
                    }
                }
                Op::Data | Op::Eos => {
                    if gid == 0 || msg.gid != gid || msg.ch != ch {
                        continue;
                    }
                    if matches!(msg.op, Op::Data) {
                        let Some(p) = pcm.as_mut() else {
                            continue;
                        };
                        if msg.samples.is_empty() {
                            continue;
                        }
                        apply_gain(
                            &mut msg.samples,
                            self.shared.announce_gain.load(Ordering::Relaxed),
                        );
                        let rc = write_frames(p, &self.q, &msg.samples, channels);
                        if rc < 0 {
                            error!("player: ch{} pcm write failed", ch.index());
                            pcm = None;
                            self.fail(gid, epoch, ch);
                            gid = 0;
                        } else if rc > 0 {
                            wrote = true;
                        }
                        continue;
                    }
                    let played = match pcm.as_mut() {
                        Some(p) if wrote => drain(p, &self.q),
                        _ => false,
                    };
                    if self.q.interrupted() {
                        continue;
                    }
                    close_pcm(&mut pcm);
                    let g = gid;
                    let ep = epoch;
                    let c = ch;
                    gid = 0;
                    wrote = false;
                    self.shared.clear_pcm_if(g);
                    if self.shared.audio_epoch[c.index()].load(Ordering::Acquire) == ep {
                        self.shared.set_state(
                            c,
                            if played {
                                PlayState::Idle
                            } else {
                                PlayState::Error
                            },
                        );
                    }
                    self.shared.complete(c, PlayId(g), played);
                }
            }
        }
    }
}

/// One stream at a time. Drop joins the thread.
pub(super) struct Worker {
    q: Arc<Queue>,
    thread: Option<JoinHandle<()>>,
}

impl Worker {
    pub(super) fn spawn(shared: Arc<Shared>) -> Option<Worker> {
        let q = Arc::new(Queue::new());
        let run = Run {
            shared,
            q: Arc::clone(&q),
        };
        let thread = std::thread::Builder::new()
            .name("pcm".into())
            .spawn(move || run.run())
            .ok();
        match thread {
            Some(thread) => Some(Worker {
                q,
                thread: Some(thread),
            }),
            None => {
                error!("player: pcm thread");
                None
            }
        }
    }

    pub(super) fn abort(&self, ch: Channel) {
        self.q.post(Msg {
            op: Op::Abort,
            gid: 0,
            epoch: 0,
            ch,
            rate: 0,
            channels: 0,
            samples: Vec::new(),
        });
    }

    pub(super) fn start(&self, ch: Channel, gid: u64, epoch: u64, rate: i32, channels: i32) {
        self.q.post(Msg {
            op: Op::Start,
            gid,
            epoch,
            ch,
            rate,
            channels,
            samples: Vec::new(),
        });
    }

    pub(super) fn push(&self, ch: Channel, gid: u64, samples: Vec<i16>) {
        self.q.post(Msg {
            op: Op::Data,
            gid,
            epoch: 0,
            ch,
            rate: 0,
            channels: 0,
            samples,
        });
    }

    pub(super) fn end(&self, ch: Channel, gid: u64) {
        self.q.post(Msg {
            op: Op::Eos,
            gid,
            epoch: 0,
            ch,
            rate: 0,
            channels: 0,
            samples: Vec::new(),
        });
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.q.post(Msg {
            op: Op::Quit,
            gid: 0,
            epoch: 0,
            ch: Channel::Announce,
            rate: 0,
            channels: 0,
            samples: Vec::new(),
        });
        if let Some(t) = self.thread.take()
            && t.join().is_err()
        {
            error!("player: pcm thread panicked");
        }
    }
}
