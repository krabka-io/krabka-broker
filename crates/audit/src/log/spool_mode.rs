//! The AU-5 degraded path of the `AuditWriter`: spool on sink failure, and
//! replay in order once the sink recovers.
//!
//! Spool mode is sticky. Once a sink write fails, every record — chained event
//! and `Checkpoint` alike — goes to the durable `Spool` instead, so the order
//! of the hash chain is preserved. The replay ticker drains the spool back to
//! the sink and leaves spool mode only when the spool is empty.

use super::writer::AuditWriter;
use crate::sink::AuditRecord;

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
    pub(super) async fn write_or_spool(&mut self, record: AuditRecord) {
        if self.spooling {
            self.spool_record(&record);
            return;
        }
        if let Err(e) = self.sink.write(record.clone()).await {
            tracing::warn!(error = %e, "audit sink write failed; entering spool mode");
            self.spooling = true;
            self.spool_record(&record);
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(class = ?record.class))]
    fn spool_record(&mut self, record: &AuditRecord) {
        let Some(spool) = &mut self.spool else {
            self.stats.inc_dropped();
            return;
        };
        match spool.append(record) {
            Ok(true) => {
                self.stats.inc_spooled();
                self.stats.set_depth(spool.count(), spool.size());
            }
            Ok(false) => {
                self.stats.inc_dropped();
                tracing::warn!("audit spool full; record dropped");
            }
            Err(e) => {
                self.stats.inc_dropped();
                tracing::error!(error = %e, "audit spool write failed; record dropped");
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
    pub(super) async fn try_replay(&mut self) {
        let Some(spool) = &mut self.spool else {
            self.spooling = false;
            return;
        };
        if spool.is_empty() {
            self.spooling = false;
            return;
        }
        let records = match spool.read_all() {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "audit spool read failed during replay");
                return;
            }
        };
        let mut replayed = 0usize;
        for rec in &records {
            if self.sink.write(rec.clone()).await.is_err() {
                break; // topic still unhealthy
            }
            replayed += 1;
        }
        let span = tracing::Span::current();
        span.record("replayed", replayed);
        span.record("total", records.len());
        if replayed == records.len() {
            if let Err(e) = spool.truncate() {
                tracing::error!(error = %e, "audit spool truncate failed");
                return;
            }
            self.spooling = false;
            tracing::info!(replayed, "audit spool drained; resumed direct topic writes");
        } else if let Err(e) = spool.rewrite(&records[replayed..]) {
            tracing::error!(error = %e, "audit spool rewrite failed during replay");
            return;
        }
        self.stats
            .inc_replayed_by(u64::try_from(replayed).unwrap_or(u64::MAX));
        self.stats.set_depth(spool.count(), spool.size());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use krabka_units::prelude::{TimeExt as _, bytes};

    use super::*;
    use crate::{
        event::AuditEventClass,
        log::{
            AuditLog,
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
