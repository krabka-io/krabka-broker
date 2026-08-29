//! The background `AuditWriter` task: its parameters, its drain loop, and the
//! hash-chaining and checkpoint steps that run on the healthy path.
//!
//! Every event that reaches the writer is serialized to an `AuditRecord`,
//! chained into the running `ChainState`, and handed to the sink. On a cadence,
//! or after a configured record count, the writer emits a signed `Checkpoint`
//! that commits the chain head. The degraded spool-and-replay path lives in
//! `super::spool_mode`.

use std::sync::Arc;

use krabka_units::prelude::{Time, TimeExt as _};
use qubit_clock::sleep::AsyncSleeper;
use tokio::sync::mpsc;

use crate::{
    chain::ChainState,
    checkpoint::Checkpoint,
    event::AuditEvent,
    ids::{EpochMs, Seq},
    ocsf::ProductInfo,
    signing::FileEd25519Signer,
    sink::{AuditRecord, AuditSink},
    spool::Spool,
    stats::AuditStats,
};

/// Construction parameters for [`AuditWriter`].
pub struct AuditWriterParams {
    pub sink: Arc<dyn AuditSink>,
    pub product: ProductInfo,
    pub signer: Option<Arc<FileEd25519Signer>>,
    /// Emit a checkpoint after the writer chains this many records since the
    /// last checkpoint. This field is a count, not an extent. `0` disables the
    /// count trigger.
    pub checkpoint_every_n: u64,
    pub checkpoint_every: Time,
    /// Chain state, possibly resumed from a recovered position.
    pub chain: ChainState,
    /// Durable spool for the AU-5 degraded path. `None` disables spooling.
    pub spool: Option<Spool>,
    pub stats: Arc<AuditStats>,
    /// How often the writer tries to drain the spool in spool mode.
    pub replay_every: Time,
    /// Relative sleeper that drives the checkpoint and replay cadence.
    /// Production uses [`qubit_clock::sleep::SystemSleeper`]. Tests inject a
    /// [`qubit_clock::sleep::MockSleeper`], so the two tickers fire on a
    /// controlled mock timeline and not on real wall-clock time.
    pub sleeper: Arc<dyn AsyncSleeper>,
}

/// Background task that chains and writes audit events.
///
/// The writer spools records when the sink fails, and it replays them when the
/// sink recovers. It also emits signed checkpoints on a cadence.
pub struct AuditWriter {
    rx: mpsc::Receiver<AuditEvent>,
    pub(super) sink: Arc<dyn AuditSink>,
    product: ProductInfo,
    chain: ChainState,
    signer: Option<Arc<FileEd25519Signer>>,
    checkpoint_every_n: u64,
    checkpoint_every: Time,
    since_checkpoint: u64,
    pub(super) spool: Option<Spool>,
    pub(super) spooling: bool,
    pub(super) stats: Arc<AuditStats>,
    replay_every: Time,
    sleeper: Arc<dyn AsyncSleeper>,
}

impl AuditWriter {
    #[must_use]
    pub fn new(rx: mpsc::Receiver<AuditEvent>, params: AuditWriterParams) -> Self {
        let spooling = params.spool.as_ref().is_some_and(|s| !s.is_empty());
        if let Some(spool) = &params.spool {
            params.stats.set_depth(spool.count(), spool.size());
        }
        Self {
            rx,
            sink: params.sink,
            product: params.product,
            chain: params.chain,
            signer: params.signer,
            checkpoint_every_n: params.checkpoint_every_n,
            checkpoint_every: params.checkpoint_every,
            since_checkpoint: 0,
            spool: params.spool,
            spooling,
            stats: params.stats,
            replay_every: params.replay_every,
            sleeper: params.sleeper,
        }
    }

    /// Drain the channel until all senders drop.
    ///
    /// The writer then emits a final checkpoint for any pending tail.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(checkpoint_every_n = self.checkpoint_every_n, spooling = self.spooling)
    )]
    pub async fn run(mut self) {
        // Drive the checkpoint and replay cadence through the injected
        // `AsyncSleeper` (production: real time; tests: a mock timeline). Each
        // ticker is a single sleep future re-armed only after it fires, which
        // matches `tokio::time::interval` with `MissedTickBehavior::Delay`: a
        // steady stream of events never resets or starves either tick. The
        // sleeper is cloned into a local so the futures borrow it rather than
        // `self`, leaving `self` free for the `&mut self` handlers below.
        let sleeper = self.sleeper.clone();
        let mut ckpt = sleeper.sleep_for_async(self.checkpoint_every.to_std());
        let mut replay = sleeper.sleep_for_async(self.replay_every.to_std());

        loop {
            tokio::select! {
                maybe = self.rx.recv() => {
                    match maybe {
                        Some(event) => {
                            self.write_chained(&event).await;
                            if self.checkpoint_every_n > 0
                                && self.since_checkpoint >= self.checkpoint_every_n
                            {
                                self.emit_checkpoint().await;
                            }
                        }
                        None => break,
                    }
                }
                () = &mut ckpt => {
                    if self.since_checkpoint > 0 {
                        self.emit_checkpoint().await;
                    }
                    ckpt = sleeper.sleep_for_async(self.checkpoint_every.to_std());
                }
                () = &mut replay => {
                    if self.spooling {
                        self.try_replay().await;
                    }
                    replay = sleeper.sleep_for_async(self.replay_every.to_std());
                }
            }
        }
        if self.since_checkpoint > 0 {
            self.emit_checkpoint().await;
        }
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(class = ?event.class(), seq = tracing::field::Empty)
    )]
    async fn write_chained(&mut self, event: &AuditEvent) {
        let mut record = AuditRecord::from_event(event, &self.product);
        let (seq, prev) = self.chain.extend(&record.value);
        tracing::Span::current().record("seq", seq);
        record.push_chain_headers(seq, &prev);
        self.write_or_spool(record).await;
        self.since_checkpoint += 1;
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(since_checkpoint = self.since_checkpoint, seq_high = tracing::field::Empty)
    )]
    async fn emit_checkpoint(&mut self) {
        let Some(signer) = self.signer.clone() else {
            self.since_checkpoint = 0;
            return;
        };
        let seq_high = Seq(self.chain.next_seq().saturating_sub(1));
        tracing::Span::current().record("seq_high", seq_high.0);
        let head = self.chain.head();
        let cp = Checkpoint::signed(signer.as_ref(), seq_high, &head, EpochMs(now_ms()));
        self.write_or_spool(cp.to_record()).await;
        self.since_checkpoint = 0;
    }
}

/// Epoch-millisecond clock for the checkpoint timestamps.
// cargo-mutants: wall-clock read; no deterministic assertion.
#[cfg_attr(test, mutants::skip)]
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use qubit_clock::sleep::MockSleeper;

    use super::*;
    use crate::{
        event::AuditEventClass,
        log::{
            AuditLog,
            test_support::{DORMANT, ROOMY_CAP, header, life, product, test_signer},
        },
        sink::MemorySink,
    };

    #[tokio::test]
    async fn emitted_events_reach_the_sink_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let sink = Arc::new(MemorySink::default());
        let stats = Arc::new(AuditStats::new());
        let (log, rx) = AuditLog::new(16);
        let writer = AuditWriter::new(
            rx,
            AuditWriterParams {
                sink: sink.clone(),
                product: product(),
                signer: None,
                checkpoint_every_n: 1_000_000,
                checkpoint_every: DORMANT,
                chain: ChainState::new(),
                spool: Some(spool),
                stats,
                replay_every: DORMANT,
                sleeper: Arc::new(MockSleeper::new()),
            },
        );
        let handle = tokio::spawn(writer.run());

        log.emit(life(1));
        log.emit(life(2));
        log.emit(life(3));

        // Dropping the only sender ends the writer loop cleanly.
        drop(log);
        handle.await.unwrap();

        let recs = sink.records();
        check!((recs.len(), recs[0].class) == (3, AuditEventClass::ApplicationLifecycle));
        // node_id 1,2,3 preserved in order via the OCSF "device.uid" field.
        let v0: serde_json::Value = serde_json::from_slice(&recs[0].value).unwrap();
        check!(v0["device"]["uid"] == "1");
    }

    #[tokio::test]
    async fn chained_records_carry_seq_and_prev_hash() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (log, rx) = AuditLog::new(16);
        let sink = Arc::new(MemorySink::default());
        // no signer, huge interval => no checkpoints, just chaining
        let writer = AuditWriter::new(
            rx,
            AuditWriterParams {
                sink: sink.clone(),
                product: product(),
                signer: None,
                checkpoint_every_n: 1_000_000,
                checkpoint_every: DORMANT,
                chain: ChainState::new(),
                spool: Some(spool),
                stats: Arc::new(AuditStats::new()),
                replay_every: DORMANT,
                sleeper: Arc::new(MockSleeper::new()),
            },
        );
        let h = tokio::spawn(writer.run());
        log.emit(life(1));
        log.emit(life(2));
        drop(log);
        h.await.unwrap();

        let recs = sink.records();
        check!(recs.len() == 2); // no checkpoints (no signer)
        // seq headers present and monotonic from 0
        let seq0 = header(&recs[0], "seq");
        let seq1 = header(&recs[1], "seq");
        check!(
            (seq0, seq1, header(&recs[0], "prev_hash"))
                == (
                    Some("0".to_string()),
                    Some("1".to_string()),
                    Some("0".repeat(64)),
                )
        );
        // record 1 prev_hash == chain_hash(genesis, 0, value0)
        let expect = crate::chain::to_hex(&crate::chain::chain_hash(
            &crate::chain::GENESIS_HEAD,
            0,
            &recs[0].value,
        ));
        check!(header(&recs[1], "prev_hash") == Some(expect));
    }

    #[tokio::test]
    async fn checkpoints_emitted_by_count_and_verify_against_recomputed_head() {
        let (signer, pubkey) = test_signer();
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (log, rx) = AuditLog::new(64);
        let sink = Arc::new(MemorySink::default());
        // checkpoint every 2 records; long interval so only count triggers
        let writer = AuditWriter::new(
            rx,
            AuditWriterParams {
                sink: sink.clone(),
                product: product(),
                signer: Some(signer),
                checkpoint_every_n: 2,
                checkpoint_every: DORMANT,
                chain: ChainState::new(),
                spool: Some(spool),
                stats: Arc::new(AuditStats::new()),
                replay_every: DORMANT,
                sleeper: Arc::new(MockSleeper::new()),
            },
        );
        let h = tokio::spawn(writer.run());
        for i in 0..4 {
            log.emit(life(i));
        }
        drop(log); // closes channel -> final checkpoint (none pending here: 4 % 2 == 0)
        h.await.unwrap();

        let recs = sink.records();
        // 4 chained + 2 checkpoints (after record 2 and record 4)
        let checkpoints: Vec<_> = recs
            .iter()
            .filter(|r| r.class == AuditEventClass::Checkpoint)
            .collect();
        check!(checkpoints.len() == 2);

        // recompute the chain over the non-checkpoint records and verify each checkpoint
        let mut head = crate::chain::GENESIS_HEAD;
        let mut seq = 0u64;
        for r in &recs {
            if r.class == AuditEventClass::Checkpoint {
                let v: serde_json::Value = serde_json::from_slice(&r.value).unwrap();
                let cp = Checkpoint::from_value(&v).expect("cp");
                check!(
                    (cp.verify(&pubkey), cp.chain_head, cp.seq_high) == (true, head, Seq(seq - 1))
                );
            } else {
                head = crate::chain::chain_hash(&head, seq, &r.value);
                seq += 1;
            }
        }
    }

    #[tokio::test]
    async fn shutdown_emits_final_checkpoint_for_pending_tail() {
        let (signer, pubkey) = test_signer();
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (log, rx) = AuditLog::new(16);
        let sink = Arc::new(MemorySink::default());
        // every_n large so only the shutdown path emits
        let writer = AuditWriter::new(
            rx,
            AuditWriterParams {
                sink: sink.clone(),
                product: product(),
                signer: Some(signer),
                checkpoint_every_n: 1_000_000,
                checkpoint_every: DORMANT,
                chain: ChainState::new(),
                spool: Some(spool),
                stats: Arc::new(AuditStats::new()),
                replay_every: DORMANT,
                sleeper: Arc::new(MockSleeper::new()),
            },
        );
        let h = tokio::spawn(writer.run());
        log.emit(life(1));
        log.emit(life(2));
        log.emit(life(3));
        drop(log);
        h.await.unwrap();

        let recs = sink.records();
        let cps: Vec<_> = recs
            .iter()
            .filter(|r| r.class == AuditEventClass::Checkpoint)
            .collect();
        check!(cps.len() == 1); // single final checkpoint at shutdown
        let v: serde_json::Value = serde_json::from_slice(&cps[0].value).unwrap();
        let cp = Checkpoint::from_value(&v).unwrap();
        check!((cp.verify(&pubkey), cp.seq_high) == (true, Seq(2)));
    }
}
