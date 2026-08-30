//! Fixtures shared by the unit tests of the `log` module tree.
//!
//! The module holds the `ProductInfo` and `AuditEvent` builders, the
//! `AuditWriterParams` factories that wire a writer to a mock timeline, the
//! failure-injecting sink, and the polling helper that replaces a real-time
//! sleep in a test.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::SeqCst},
};

use krabka_units::prelude::{ByteSize, Time, hours, mebibytes, millis};
use qubit_clock::{MockTimeline, sleep::MockSleeper};

use super::AuditWriterParams;
use crate::{
    event::{AuditEvent, LifecycleKind},
    ocsf::ProductInfo,
    signing::FileEd25519Signer,
    sink::{AuditRecord, AuditSink, MemorySink},
    spool::Spool,
    stats::AuditStats,
};

pub fn product() -> ProductInfo {
    ProductInfo {
        vendor_name: "Krabka".into(),
        name: "krabka-broker".into(),
        version: "0".into(),
    }
}

pub fn life(n: i64) -> AuditEvent {
    AuditEvent::Lifecycle {
        kind: LifecycleKind::BrokerStarted,
        node_id: n,
        time_ms: n,
    }
}

pub fn header(rec: &AuditRecord, key: &str) -> Option<String> {
    rec.headers
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| String::from_utf8_lossy(v).into_owned())
}

pub fn test_signer() -> (std::sync::Arc<FileEd25519Signer>, Vec<u8>) {
    use ring::signature::{Ed25519KeyPair, KeyPair};
    let rng = ring::rand::SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
    let kp = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
    let pubkey = kp.public_key().as_ref().to_vec();
    let s = FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), "k1".into()).unwrap();
    (std::sync::Arc::new(s), pubkey)
}

#[derive(Debug)]
pub struct FailableSink {
    fail: AtomicBool,
    indeterminate: AtomicBool,
    indeterminate_after: AtomicI64,
    /// -1 = unlimited; >= 0 = writes remaining before budget error.
    allow: AtomicI64,
    durable_requests: AtomicU64,
    pub inner: MemorySink,
}

impl Default for FailableSink {
    fn default() -> Self {
        Self {
            fail: AtomicBool::new(false),
            indeterminate: AtomicBool::new(false),
            indeterminate_after: AtomicI64::new(-1),
            allow: AtomicI64::new(-1),
            durable_requests: AtomicU64::new(0),
            inner: MemorySink::default(),
        }
    }
}

impl FailableSink {
    pub fn set_fail(&self, v: bool) {
        self.fail.store(v, SeqCst);
    }

    pub fn set_indeterminate(&self, value: bool) {
        self.indeterminate.store(value, SeqCst);
    }

    pub fn set_indeterminate_after(&self, successful_writes: i64) {
        self.indeterminate_after.store(successful_writes, SeqCst);
    }

    pub fn allow_n(&self, n: i64) {
        self.fail.store(false, SeqCst);
        self.allow.store(n, SeqCst);
    }

    pub fn allow_unlimited(&self) {
        self.allow.store(-1, SeqCst);
        self.fail.store(false, SeqCst);
    }

    pub fn durable_requests(&self) -> u64 {
        self.durable_requests.load(SeqCst)
    }
}

#[async_trait::async_trait]
impl AuditSink for FailableSink {
    async fn write(
        &self,
        record: AuditRecord,
        durable: bool,
    ) -> Result<(), crate::sink::AuditError> {
        if durable {
            self.durable_requests.fetch_add(1, SeqCst);
        }
        let indeterminate_after = self.indeterminate_after.load(SeqCst);
        if self.indeterminate.load(SeqCst) || indeterminate_after == 0 {
            return Err(crate::sink::AuditError::Indeterminate("forced".into()));
        }
        if indeterminate_after > 0 {
            self.indeterminate_after.fetch_sub(1, SeqCst);
        }
        if self.fail.load(SeqCst) {
            return Err(crate::sink::AuditError::Sink("forced".into()));
        }
        let allow = self.allow.load(SeqCst);
        if allow >= 0 {
            if allow == 0 {
                return Err(crate::sink::AuditError::Sink("budget exhausted".into()));
            }
            self.allow.fetch_sub(1, SeqCst);
        }
        self.inner.write(record, durable).await
    }
}

/// Replay ticker cadence for the test params. Tests advance the mock
/// timeline by this amount to fire the replay ticker exactly once.
pub const REPLAY_EVERY: Time = millis(20);

/// A cadence that no test reaches. No test advances the mock timeline that
/// far, so the ticker this cadence drives stays dormant.
pub const DORMANT: Time = hours(1);

/// A spool cap that is large enough that no test reaches it by accident.
pub const ROOMY_CAP: ByteSize = mebibytes(1);

pub fn params(sink: Arc<dyn AuditSink>, spool: Spool, stats: Arc<AuditStats>) -> AuditWriterParams {
    AuditWriterParams {
        sink,
        product: product(),
        signer: None,
        checkpoint_every_n: 0,
        checkpoint_every: DORMANT,
        chain: crate::chain::ChainState::new(),
        spool: Some(spool),
        stats,
        replay_every: REPLAY_EVERY,
        // A dormant mock sleeper: its checkpoint/replay tickers only fire
        // when a test advances the shared timeline, so tests that don't
        // exercise the tickers stay quiet and deterministic.
        sleeper: Arc::new(MockSleeper::new()),
    }
}

/// Like [`params`], but also returns the mock [`MockTimeline`].
///
/// The timeline backs the checkpoint and replay tickers. A test can fire
/// them deterministically with `timeline.advance(replay_every)` instead of
/// a sleep in real time.
pub fn params_with_timeline(
    sink: Arc<dyn AuditSink>,
    spool: Spool,
    stats: Arc<AuditStats>,
) -> (AuditWriterParams, MockTimeline) {
    let sleeper = MockSleeper::new();
    let timeline = sleeper.timeline();
    let mut p = params(sink, spool, stats);
    p.sleeper = Arc::new(sleeper);
    (p, timeline)
}

/// Polls `cond` on every executor turn until it holds.
///
/// The function yields, so the spawned writer task can make progress. It
/// replaces the fixed `sleep` calls that waited for the writer to drain the
/// channel. It returns at the instant the observable condition is true,
/// which is deterministic. The large iteration cap is only a hang guard.
pub async fn await_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..1_000_000 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held: {what}");
}
