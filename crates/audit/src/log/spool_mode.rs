//! The AU-5 degraded path of the `AuditWriter`: spool on sink failure, and
//! replay in order once the sink recovers.
//!
//! Spool mode is sticky. Once a sink write fails, every record — chained event
//! and `Checkpoint` alike — goes to the durable `Spool` instead, so the order
//! of the hash chain is preserved. The replay ticker drains the spool back to
//! the sink and leaves spool mode only when the spool is empty.

use super::writer::AuditWriter;
use crate::sink::{AuditError, AuditRecord};

impl AuditWriter {
    /// Write to the sink, or to the spool.
    ///
    /// This method writes to the spool when the writer is in spool mode, which
    /// is sticky, or when the sink write fails.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(class = ?record.class, spooling = self.spooling)
    )]
    pub(super) async fn write_or_spool(
        &mut self,
        record: &AuditRecord,
        durable: bool,
    ) -> Result<(), AuditError> {
        if self.spooling {
            return self.spool_record(record, durable);
        }
        match self.sink.write(record.clone(), durable).await {
            Ok(()) => Ok(()),
            Err(error @ AuditError::Indeterminate(_)) => Err(error),
            Err(error) => {
                tracing::warn!(%error, "audit sink write failed; entering spool mode");
                self.spooling = true;
                self.spool_record(record, durable)
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(class = ?record.class))]
    fn spool_record(&mut self, record: &AuditRecord, durable: bool) -> Result<(), AuditError> {
        let Some(spool) = &mut self.spool else {
            return Err(AuditError::Unavailable(
                "audit sink failed and spool is disabled".into(),
            ));
        };
        match spool.append(record) {
            Ok(true) => {
                if durable {
                    spool.sync()?;
                }
                self.stats.inc_spooled();
                self.stats.set_depth(spool.count(), spool.size());
                Ok(())
            }
            Ok(false) => {
                tracing::warn!("audit spool full; record dropped");
                Err(AuditError::Unavailable("audit spool is full".into()))
            }
            Err(e) => {
                tracing::error!(error = %e, "audit spool write failed; record dropped");
                Err(e)
            }
        }
    }

    /// Drain the spool to the sink in order.
    ///
    /// The writer exits spool mode when the spool is fully drained.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(replayed = tracing::field::Empty, total = tracing::field::Empty)
    )]
    pub(super) async fn try_replay(&mut self) -> Result<(), AuditError> {
        let Some(spool) = &mut self.spool else {
            self.spooling = false;
            return Ok(());
        };
        if spool.is_empty() {
            self.spooling = false;
            return Ok(());
        }
        let records = match spool.read_all() {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "audit spool read failed during replay");
                return Ok(());
            }
        };
        let mut replayed = 0usize;
        while replayed < records.len() {
            let record = &records[replayed];
            spool.begin_replay(record)?;
            match self.sink.write(record.clone(), true).await {
                Ok(()) => {
                    if let Err(error) = spool.commit_replay(record) {
                        return Err(AuditError::Indeterminate(format!(
                            "replay append succeeded but cursor commit failed: {error}"
                        )));
                    }
                    replayed += 1;
                    self.stats.inc_replayed_by(1);
                    self.stats.set_depth(spool.count(), spool.size());
                }
                Err(error @ AuditError::Indeterminate(_)) => return Err(error),
                Err(_) => {
                    spool.abort_replay()?;
                    break;
                }
            }
        }
        let span = tracing::Span::current();
        span.record("replayed", replayed);
        span.record("total", records.len());
        if replayed == records.len() {
            spool.truncate()?;
            self.stats.set_depth(spool.count(), spool.size());
            self.spooling = false;
            tracing::info!(replayed, "audit spool drained; resumed direct topic writes");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use krabka_units::prelude::{ByteSizeExt as _, TimeExt as _, bytes};

    use super::*;
    use crate::{
        event::AuditEventClass,
        log::{
            AuditLog, AuditMode,
            test_support::{
                FailableSink, REPLAY_EVERY, ROOMY_CAP, await_until, header, life, params,
                params_with_timeline, product, test_signer,
            },
        },
        spool::Spool,
        stats::AuditStats,
    };

    #[tokio::test]
    async fn records_spool_on_sink_failure_then_replay_to_sink() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FailableSink::default());
        sink.set_fail(true); // topic "down"
        let stats = Arc::new(AuditStats::new());
        let (log, rx) = AuditLog::new(64);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (p, timeline) = params_with_timeline(sink.clone(), spool, stats.clone());
        let writer = AuditWriter::new(rx, p);
        let h = tokio::spawn(writer.run());

        log.emit(life(1));
        log.emit(life(2));
        log.emit(life(3));
        // wait until the writer has drained all three into the spool
        await_until("3 records spooled", || stats.spooled() >= 3).await;
        check!(stats.depth() >= 3);
        check!(sink.inner.records().is_empty()); // nothing reached the topic yet

        // topic recovers; fire the replay ticker by advancing the mock timeline
        sink.set_fail(false);
        timeline.advance(REPLAY_EVERY.to_std());
        await_until("spool drained after replay", || stats.depth() == 0).await;

        drop(log);
        h.await.unwrap();

        // all three chained records reached the sink, in order, with monotonic seq
        let recs = sink.inner.records();
        let seqs: Vec<String> = recs
            .iter()
            .filter(|r| r.class != AuditEventClass::Checkpoint)
            .map(|r| header(r, "seq").unwrap())
            .collect();
        check!(
            (seqs, stats.replayed() >= 3, stats.depth())
                == (
                    vec!["0".to_string(), "1".to_string(), "2".to_string()],
                    true,
                    0
                )
        );
    }

    #[tokio::test]
    async fn direct_writes_when_sink_healthy_do_not_spool() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FailableSink::default()); // healthy
        let stats = Arc::new(AuditStats::new());
        let (log, rx) = AuditLog::new(16);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let writer = AuditWriter::new(rx, params(sink.clone(), spool, stats.clone()));
        let h = tokio::spawn(writer.run());
        log.emit(life(1));
        log.emit(life(2));
        drop(log);
        h.await.unwrap();
        check!((sink.inner.records().len(), stats.spooled(), stats.depth()) == (2, 0, 0));
    }

    #[tokio::test]
    async fn fail_closed_write_requests_durable_sink_acknowledgement() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FailableSink::default());
        let stats = Arc::new(AuditStats::new());
        let (log, receiver) = AuditLog::new_with_mode(8, AuditMode::FailClosed);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let writer = AuditWriter::new(receiver, params(sink.clone(), spool, Arc::clone(&stats)));
        let handle = tokio::spawn(writer.run());

        log.emit_required(life(1)).await.unwrap();
        drop(log);
        handle.await.unwrap();

        check!(
            (
                sink.durable_requests(),
                sink.inner.records().len(),
                stats.depth()
            ) == (1, 1, 0)
        );
    }

    #[tokio::test]
    async fn indeterminate_durable_write_fails_stopped_without_spool_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FailableSink::default());
        sink.set_indeterminate(true);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (log, receiver) = AuditLog::new_with_mode(8, AuditMode::FailClosed);
        let writer = AuditWriter::new(receiver, params(sink, spool, Arc::new(AuditStats::new())));
        let handle = tokio::spawn(writer.run());

        let error = log.emit_required(life(1)).await.unwrap_err();
        check!(error.to_string().contains("indeterminate"));
        handle.await.unwrap();
        check!(
            log.emit_required(life(2))
                .await
                .unwrap_err()
                .to_string()
                .contains("not running")
        );
        check!(Spool::open(dir.path(), ROOMY_CAP).unwrap().is_empty());
    }

    #[tokio::test]
    async fn indeterminate_replay_poison_survives_after_a_successful_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let mut chain = crate::chain::ChainState::new();
        for event in [life(1), life(2)] {
            let mut record = AuditRecord::from_event(&event, &product());
            let (seq, previous) = chain.extend(&record.value);
            record.push_chain_headers(seq, &previous);
            check!(spool.append(&record).unwrap());
        }
        let sink = Arc::new(FailableSink::default());
        sink.set_indeterminate_after(1);
        let (log, receiver) = AuditLog::new(8);
        let (params, timeline) =
            params_with_timeline(sink.clone(), spool, Arc::new(AuditStats::new()));
        let handle = tokio::spawn(AuditWriter::new(receiver, params).run());

        tokio::task::yield_now().await;
        timeline.advance(REPLAY_EVERY.to_std());
        handle.await.unwrap();
        check!(sink.inner.records().len() == 1);
        check!(matches!(
            Spool::open(dir.path(), ROOMY_CAP),
            Err(AuditError::Poisoned(_))
        ));
        drop(log);
    }

    #[tokio::test]
    async fn checkpoint_is_spooled_in_spool_mode_and_replayed_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let (signer, _pubkey) = test_signer();
        let sink = Arc::new(FailableSink::default());
        sink.set_fail(true); // topic down → everything spools
        let stats = Arc::new(AuditStats::new());
        let (log, rx) = AuditLog::new(64);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (mut p, timeline) = params_with_timeline(sink.clone(), spool, stats.clone());
        p.signer = Some(signer);
        p.checkpoint_every_n = 2; // emit a checkpoint after every 2 records
        let writer = AuditWriter::new(rx, p);
        let h = tokio::spawn(writer.run());
        log.emit(life(0));
        log.emit(life(1)); // 2 records → triggers a checkpoint, all spooled
        // 2 chained records + 1 count-triggered checkpoint all land in the spool
        await_until("2 records + checkpoint spooled", || stats.spooled() >= 3).await;
        check!(sink.inner.records().is_empty()); // nothing on topic yet
        sink.set_fail(false); // recover → replay drains spool in order
        timeline.advance(REPLAY_EVERY.to_std());
        await_until("spool drained after replay", || stats.depth() == 0).await;
        drop(log);
        h.await.unwrap();
        let recs = sink.inner.records();
        // exactly 2 chained records, and at least one checkpoint, and the checkpoint
        // appears AFTER both chained records (it was spooled + replayed in order).
        check!(
            recs.iter()
                .filter(|r| r.class != AuditEventClass::Checkpoint)
                .count()
                == 2
        );
        let cp_idx = recs
            .iter()
            .position(|r| r.class == AuditEventClass::Checkpoint)
            .expect("checkpoint present");
        let chained_before = recs[..cp_idx]
            .iter()
            .filter(|r| r.class != AuditEventClass::Checkpoint)
            .count();
        check!(chained_before == 2); // checkpoint comes after the 2 records it covers
    }

    #[tokio::test]
    async fn spool_overflow_drops_and_updates_stats() {
        let dir = tempfile::tempdir().unwrap();
        // size of one chained record, to cap the spool at ~1 record
        let one = {
            let d2 = tempfile::tempdir().unwrap();
            let mut s = Spool::open(d2.path(), ROOMY_CAP).unwrap();
            let mut rec = AuditRecord::from_event(&life(0), &product());
            rec.push_chain_headers(0, &crate::chain::GENESIS_HEAD);
            s.append(&rec).unwrap();
            s.size()
        };
        let sink = Arc::new(FailableSink::default());
        sink.set_fail(true); // stay in spool mode (no replay), so drops accumulate
        let stats = Arc::new(AuditStats::new());
        let (log, rx) = AuditLog::new(64);
        let spool = Spool::open(dir.path(), one).unwrap();
        let writer = AuditWriter::new(rx, params(sink.clone(), spool, stats.clone()));
        let h = tokio::spawn(writer.run());
        for i in 0..6 {
            log.emit(life(i));
        }
        // wait until all six events are accounted for (each is spooled or dropped)
        await_until("6 events processed", || {
            stats.spooled() + stats.dropped() >= 6
        })
        .await;
        drop(log);
        h.await.unwrap();
        // Strict bounds chosen to also kill the "return constant 1" mutants.
        assert2::check!(stats.dropped() >= 2); // many overflowed (kills inc_dropped/() , dropped->0/1)
        assert2::check!(stats.spool_bytes() > bytes(1)); // ~one record is buffered (kills spool_bytes->0/1)
    }

    #[tokio::test]
    async fn full_spool_refuses_a_fail_closed_record() {
        let probe = tempfile::tempdir().unwrap();
        let mut existing = AuditRecord::from_event(&life(0), &product());
        existing.push_chain_headers(0, &crate::chain::GENESIS_HEAD);
        let one = {
            let mut spool = Spool::open(probe.path(), ROOMY_CAP).unwrap();
            check!(spool.append(&existing).unwrap());
            spool.size()
        };

        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(dir.path(), one).unwrap();
        check!(spool.append(&existing).unwrap());
        let sink = Arc::new(FailableSink::default());
        sink.set_fail(true);
        let (log, receiver) = AuditLog::new_with_mode(8, AuditMode::FailClosed);
        let writer = AuditWriter::new(receiver, params(sink, spool, Arc::new(AuditStats::new())));
        let handle = tokio::spawn(writer.run());

        let error = log.emit_required(life(1)).await.unwrap_err();
        check!(error.to_string().contains("spool is full"));

        drop(log);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn fail_open_spool_loss_is_followed_by_a_chain_marker() {
        let probe = tempfile::tempdir().unwrap();
        let mut first = AuditRecord::from_event(&life(0), &product());
        first.push_chain_headers(0, &crate::chain::GENESIS_HEAD);
        let one = {
            let mut spool = Spool::open(probe.path(), ROOMY_CAP).unwrap();
            check!(spool.append(&first).unwrap());
            spool.size()
        };

        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FailableSink::default());
        sink.set_fail(true);
        let stats = Arc::new(AuditStats::new());
        let spool = Spool::open(dir.path(), one).unwrap();
        let (log, receiver) = AuditLog::new_with_mode_and_spool(8, AuditMode::FailOpen, &spool);
        let (params, timeline) = params_with_timeline(sink.clone(), spool, stats.clone());
        let writer = AuditWriter::new(receiver, params);
        let handle = tokio::spawn(writer.run());

        log.emit(life(0));
        log.emit(life(1));
        log.emit(life(2));
        await_until("one spooled and two lost", || stats.dropped() == 2).await;
        check!(
            std::fs::metadata(dir.path().join("audit.spool"))
                .unwrap()
                .len()
                <= one.bytes_u64()
        );

        sink.set_fail(false);
        timeline.advance(REPLAY_EVERY.to_std());
        await_until("record replayed", || sink.inner.records().len() == 1).await;
        timeline.advance(REPLAY_EVERY.to_std());
        await_until("coalesced loss marker replayed", || {
            sink.inner.records().len() == 2
        })
        .await;
        let records = sink.inner.records();
        let lost: u64 = records
            .iter()
            .filter(|record| record.class == AuditEventClass::RecordsLost)
            .map(|record| {
                serde_json::from_slice::<serde_json::Value>(&record.value).unwrap()["records_lost"]
                    .as_u64()
                    .unwrap()
            })
            .sum();
        check!(lost == 2);
        check!(
            std::fs::metadata(dir.path().join("audit.spool"))
                .unwrap()
                .len()
                <= one.bytes_u64()
        );

        drop(log);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn partial_replay_keeps_remainder_then_drains() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FailableSink::default());
        sink.set_fail(true);
        let stats = Arc::new(AuditStats::new());
        let (log, rx) = AuditLog::new(64);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (p, timeline) = params_with_timeline(sink.clone(), spool, stats.clone());
        let writer = AuditWriter::new(rx, p);
        let h = tokio::spawn(writer.run());
        log.emit(life(0));
        log.emit(life(1));
        log.emit(life(2));
        await_until("3 records spooled", || stats.depth() == 3).await;

        // allow exactly 2 replay writes, then fail → partial replay
        sink.allow_n(2);
        timeline.advance(REPLAY_EVERY.to_std());
        await_until("2 of 3 replayed", || {
            stats.replayed() == 2 && stats.depth() == 1
        })
        .await;
        check!(stats.depth() == 1); // remainder retained, still spooling

        // allow the rest; fire the replay ticker again to drain the remainder
        sink.allow_unlimited();
        timeline.advance(REPLAY_EVERY.to_std());
        await_until("remainder drained", || stats.depth() == 0).await;

        drop(log);
        h.await.unwrap();
        // all 3 chained records reached the sink exactly once, in seq order
        let seqs: Vec<String> = sink
            .inner
            .records()
            .iter()
            .filter(|r| r.class != AuditEventClass::Checkpoint)
            .map(|r| header(r, "seq").unwrap())
            .collect();
        check!(seqs == vec!["0".to_string(), "1".to_string(), "2".to_string()]);
    }
}
