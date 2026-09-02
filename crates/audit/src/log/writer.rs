//! The background `AuditWriter` task: its parameters, its drain loop, and the
//! hash-chaining and checkpoint steps that run on the healthy path.
//!
//! Every event that reaches the writer is serialized to an `AuditRecord`,
//! chained into the running `ChainState`, and handed to the sink. On a cadence,
//! or after a configured record count, the writer emits a signed `Checkpoint`
//! that commits the chain head. The degraded spool-and-replay path lives in
//! `super::spool_mode`.

use std::{sync::Arc, time::Duration};

use krabka_units::prelude::{Time, TimeExt as _};
use qubit_clock::{TimeError, Timer, TimerFuture};

use super::handle::AuditReceiver;
use crate::{
    chain::ChainState,
    checkpoint::Checkpoint,
    event::AuditEvent,
    ids::{EpochMs, Seq},
    ocsf::ProductInfo,
    signing::FileEd25519Signer,
    sink::{AuditError, AuditRecord, AuditSink},
    spool::{PendingLosses, Spool},
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
    /// Timer that drives the checkpoint and replay cadence. Production uses
    /// [`qubit_clock::StdTimer`]. Tests inject a timer taken from a
    /// [`qubit_clock::ManualMonotonicClock`], so the two tickers fire on a
    /// manually advanced timeline and not on real wall-clock time.
    pub timer: Arc<dyn Timer>,
}

/// Background task that chains and writes audit events.
///
/// The writer spools records when the sink fails, and it replays them when the
/// sink recovers. It also emits signed checkpoints on a cadence.
pub struct AuditWriter {
    rx: AuditReceiver,
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
    timer: Arc<dyn Timer>,
    pending_losses: Arc<PendingLosses>,
}

impl AuditWriter {
    #[must_use]
    pub fn new(rx: AuditReceiver, params: AuditWriterParams) -> Self {
        let spooling = params.spool.as_ref().is_some_and(|s| !s.is_empty());
        let pending_losses = rx.pending_losses();
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
            timer: params.timer,
            pending_losses,
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
        // `Timer` (production: real time; tests: a manually advanced
        // timeline). Each ticker is a single deadline re-armed only after it
        // fires, which matches `tokio::time::interval` with
        // `MissedTickBehavior::Delay`: a steady stream of events never resets
        // or starves either tick. The timer futures are `'static` and hold
        // their own handle on the backend; the timer is still cloned into a
        // local so that arming the next deadline in an arm below does not
        // borrow `self`, which those `&mut self` handlers need.
        //
        // A ticker that cannot be armed, or that fails once armed, stops the
        // drain loop -- but it never skips [`Self::finish`]. Losing the cadence
        // is not a reason to abandon a chain head that is already committable,
        // so a dead timer takes the same exit a closed channel takes, and only
        // the two indeterminate-write paths still leave without flushing.
        let timer = Arc::clone(&self.timer);
        let Some(mut ckpt) = arm(&*timer, self.checkpoint_every.to_std(), CHECKPOINT_TASK) else {
            return self.finish().await;
        };
        let Some(mut replay) = arm(&*timer, self.replay_every.to_std(), REPLAY_TASK) else {
            return self.finish().await;
        };

        loop {
            tokio::select! {
                maybe = self.rx.recv_message() => {
                    match maybe {
                        Some(message) => {
                            let durable = message.acknowledgement.is_some();
                            let result = self.write_chained(&message.event, durable).await;
                            let fatal = matches!(&result, Err(AuditError::Indeterminate(_)));
                            if result.is_err() {
                                self.stats.inc_dropped();
                                if !durable {
                                    self.pending_losses.add(1);
                                    if let Err(error) = self.pending_losses.persist() {
                                        tracing::error!(%error, "failed to persist pending audit loss count");
                                    }
                                }
                            }
                            if let Some(acknowledgement) = message.acknowledgement {
                                let _ = acknowledgement.send(result);
                            }
                            if fatal {
                                tracing::error!("audit writer stopped after indeterminate durable write");
                                return;
                            }
                            if self.checkpoint_every_n > 0
                                && self.since_checkpoint >= self.checkpoint_every_n
                            {
                                self.emit_checkpoint().await;
                            }
                        }
                        None => break,
                    }
                }
                outcome = &mut ckpt => {
                    if !fired(outcome, CHECKPOINT_TASK) {
                        break;
                    }
                    if self.since_checkpoint > 0 {
                        self.emit_checkpoint().await;
                    }
                    let Some(next) =
                        arm(&*timer, self.checkpoint_every.to_std(), CHECKPOINT_TASK)
                    else {
                        break;
                    };
                    ckpt = next;
                }
                outcome = &mut replay => {
                    if !fired(outcome, REPLAY_TASK) {
                        break;
                    }
                    if self.spooling
                        && let Err(error) = self.try_replay().await
                    {
                        tracing::error!(%error, "audit writer stopped after indeterminate replay");
                        return;
                    }
                    if !self.spooling {
                        let _ = self.write_pending_loss_marker(false).await;
                    }
                    let Some(next) = arm(&*timer, self.replay_every.to_std(), REPLAY_TASK) else {
                        break;
                    };
                    replay = next;
                }
            }
        }
        self.finish().await;
    }

    /// The final flush that every exit short of a lost-write abort takes: one
    /// coalesced marker for the fail-open records this writer dropped, then a
    /// checkpoint committing whatever chain tail is still uncommitted.
    ///
    /// It is a method rather than the tail of [`Self::run`] because a timer
    /// that will not arm has to reach it too, before the loop is ever entered.
    async fn finish(mut self) {
        let _ = self.write_pending_loss_marker(true).await;
        if self.since_checkpoint > 0 {
            self.emit_checkpoint().await;
        }
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(class = ?event.class(), seq = tracing::field::Empty)
    )]
    async fn write_chained(&mut self, event: &AuditEvent, durable: bool) -> Result<(), AuditError> {
        self.write_pending_loss_marker(durable).await?;
        let mut record = AuditRecord::from_event(event, &self.product);
        let seq = self.chain.next_seq();
        let prev = self.chain.head();
        tracing::Span::current().record("seq", seq);
        record.push_chain_headers(seq, &prev);
        self.write_or_spool(&record, durable).await?;
        let _ = self.chain.extend(&record.value);
        self.since_checkpoint += 1;
        Ok(())
    }

    /// Persist one explicit marker for all fail-open records lost so far.
    async fn write_pending_loss_marker(&mut self, durable: bool) -> Result<(), AuditError> {
        if let Some(spool) = &mut self.spool {
            let pending_losses = Arc::clone(&self.pending_losses);
            let marker = pending_losses.persist_with(|batch| {
                let mut marker =
                    AuditRecord::records_lost_with_generation(batch.count, batch.generation);
                marker.push_chain_headers(self.chain.next_seq(), &self.chain.head());
                spool.append_loss_marker(&marker)?;
                Ok(marker)
            })?;
            if let Some(marker) = marker {
                self.stats.inc_spooled();
                self.stats.set_depth(spool.count(), spool.size());
                let _ = self.chain.extend(&marker.value);
                self.since_checkpoint += 1;
                self.spooling = true;
            }
            return Ok(());
        }

        let Some(batch) = self.pending_losses.snapshot() else {
            return Ok(());
        };
        let mut marker = AuditRecord::records_lost_with_generation(batch.count, batch.generation);
        let seq = self.chain.next_seq();
        let prev = self.chain.head();
        marker.push_chain_headers(seq, &prev);
        self.write_or_spool(&marker, durable).await?;
        let _ = self.chain.extend(&marker.value);
        self.since_checkpoint += 1;
        self.pending_losses.commit(batch);
        Ok(())
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
        let record = cp.to_record();
        match self.write_or_spool(&record, false).await {
            Ok(()) => self.since_checkpoint = 0,
            Err(_) => self.stats.inc_dropped(),
        }
    }
}

/// Names the checkpoint cadence in the timer-failure logs.
const CHECKPOINT_TASK: &str = "audit checkpoint";

/// Names the spool-replay cadence in the timer-failure logs.
const REPLAY_TASK: &str = "audit replay";

/// Registers a deadline `delay` from now on `timer`, for the ticker named
/// `task`.
///
/// `None` means the timer refused the registration, and [`AuditWriter::run`]
/// must stop. `krabka-audit` cannot reach the broker's `time_util` guards, so
/// it keeps this pair local, with the same contract.
///
/// Stopping is the right answer here in particular. A broker whose timer
/// backend is gone can no longer checkpoint the audit chain on cadence, and an
/// audit writer that silently keeps running without its cadence is worse than
/// one that stops loudly: the chain would grow with no committed head, and
/// nothing would say so. Returning closes the channel the writer drains, so
/// every sender sees the failure on its next emit instead of writing into a
/// pipeline that has quietly lost half its guarantees. Re-arming in a loop is
/// not an option either, because an unarmable timer would spin the task at
/// full speed.
fn arm(timer: &dyn Timer, delay: Duration, task: &'static str) -> Option<TimerFuture> {
    match timer.after(delay) {
        Ok(future) => Some(future),
        Err(error) => {
            tracing::error!(%error, task, "could not arm the audit timer; stopping the writer");
            None
        }
    }
}

/// Reports whether a deadline armed by [`arm`] completed, for the ticker named
/// `task`.
///
/// `false` means the timer gave up on a registration it had accepted, and the
/// writer must stop for the same reason [`arm`] returning `None` makes it stop.
fn fired(outcome: Result<(), TimeError>, task: &'static str) -> bool {
    match outcome {
        Ok(()) => true,
        Err(error) => {
            tracing::error!(%error, task, "the armed audit timer failed; stopping the writer");
            false
        }
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
    use qubit_clock::{MonotonicClock, MonotonicInstant, StdMonotonicClock};

    use super::*;
    use crate::{
        event::AuditEventClass,
        log::{
            AuditLog,
            test_support::{DORMANT, ROOMY_CAP, dormant_timer, header, life, product, test_signer},
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
                timer: dormant_timer(),
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
                timer: dormant_timer(),
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
                timer: dormant_timer(),
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
                timer: dormant_timer(),
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

    /// A timer whose backend is gone: it refuses every registration.
    struct DeadTimer(StdMonotonicClock);

    impl Timer for DeadTimer {
        fn clock(&self) -> &dyn MonotonicClock {
            &self.0
        }

        fn at(&self, _deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
            Err(TimeError::InstantOverflow)
        }
    }

    #[tokio::test]
    async fn writer_stops_when_a_ticker_cannot_be_armed() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let sink = Arc::new(MemorySink::default());
        let (log, rx) = AuditLog::new(16);
        let mut params =
            crate::log::test_support::params(sink.clone(), spool, Arc::new(AuditStats::new()));
        params.timer = Arc::new(DeadTimer(StdMonotonicClock::new()));
        let handle = tokio::spawn(AuditWriter::new(rx, params).run());

        // The sender is still alive, so nothing but the unarmable ticker can
        // end the run: the writer stops rather than run on without a cadence,
        // and the event emitted before it noticed never reaches the sink.
        log.emit(life(1));
        handle.await.unwrap();
        check!(sink.records().is_empty());
        drop(log);
    }

    #[tokio::test]
    async fn failed_checkpoint_increments_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let sink = Arc::new(crate::log::test_support::FailableSink::default());
        sink.allow_n(1);
        let stats = Arc::new(AuditStats::new());
        let (log, rx) = AuditLog::new(4);
        let mut params = crate::log::test_support::params(sink, spool, Arc::clone(&stats));
        params.signer = Some(test_signer().0);
        params.checkpoint_every_n = 1;
        params.spool = None;
        let handle = tokio::spawn(AuditWriter::new(rx, params).run());

        log.emit(life(1));
        drop(log);
        handle.await.unwrap();

        check!(stats.dropped() >= 1);
    }
}
