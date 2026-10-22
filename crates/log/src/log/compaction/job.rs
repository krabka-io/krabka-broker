//! A compaction pass in three steps, so the slow one runs without the log.
//!
//! [`Log::prepare_compaction`] takes what the pass needs from the log: the
//! sealed segments it consumes, their aborted transactions, the active
//! producers and the configuration. [`CompactionJob::build`] reads those
//! segments and writes the `.cleaned` files, and touches nothing the log
//! holds, so the caller runs it without the log's lock while appends and
//! reads go on. [`Log::finish_compaction`] installs the result, unless the
//! log has meanwhile dropped or replaced a segment the pass read, or been
//! fenced. Kafka's cleaner works the same way: `Cleaner.cleanSegments` reads
//! and writes without the log's lock and takes it in `replaceSegments`.
//!
//! [`Log::prepare_compaction`]: super::Log::prepare_compaction
//! [`Log::finish_compaction`]: super::Log::finish_compaction

use {
    super::GroupRewrite,
    crate::{
        compact::{
            CleanedTransactionMetadata, CleaningRound, ProducerLastRecord, RewriteRetention,
            build_offset_map, rewrite_segments,
        },
        error::LogError,
        io::LogIo,
        log::rollover_flush::FlushTicket,
        recovery::swap::{cleaned_path, remove_if_present},
        segment::SealedInput,
        txn_index::AbortedTxn,
    },
    krabka_ids::{Offset, ProducerId},
    krabka_units::prelude::ByteSize,
    std::{
        collections::HashMap,
        path::PathBuf,
        sync::{
            Arc, Condvar, Mutex, MutexGuard, PoisonError,
            atomic::{AtomicBool, Ordering},
        },
    },
};

/// The `.cleaned` sidecars a group's rewrite can leave, by extension.
const CLEANED_EXTENSIONS: [&str; 4] = ["log", "index", "timeindex", "txnindex"];

/// What a log knows about its compaction job, shared with the job.
///
/// It allows one job at a time, and it lets [`Log::fence`] stop a job's build
/// and wait until the build writes nothing more, so the broker can delete the
/// partition's directory once the fence returns.
///
/// [`Log::fence`]: super::Log::fence
#[derive(Debug, Default)]
pub(in crate::log) struct CompactionGate {
    /// Set by the fence. Each input of a job reads it before every batch.
    fenced: Arc<AtomicBool>,
    state: Mutex<GateState>,
    /// Signalled when a build ends.
    build_ended: Condvar,
}

#[derive(Debug, Default)]
struct GateState {
    /// A job exists: prepared and not yet finished or dropped.
    job: bool,
    /// A job's build is running, and may be writing `.cleaned` files.
    building: bool,
}

impl CompactionGate {
    fn state(&self) -> MutexGuard<'_, GateState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The flag the fence sets, for a job's inputs to watch.
    pub(in crate::log) fn fenced_flag(&self) -> Arc<AtomicBool> { Arc::clone(&self.fenced) }

    /// Claim the log's one job, or `None` when a job already holds it.
    pub(in crate::log) fn claim(self: &Arc<Self>) -> Option<JobSlot> {
        let mut state = self.state();
        if state.job {
            return None;
        }
        state.job = true;
        Some(JobSlot(Arc::clone(self)))
    }

    /// Fence the gate: a build that has not started refuses to, and a running
    /// one stops at its next batch. Returns once no build is running.
    pub(in crate::log) fn fence(&self) {
        let mut state = self.state();
        self.fenced.store(true, Ordering::Release);
        while state.building {
            state = self
                .build_ended
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

/// A claim on the log's one compaction job. Dropping it releases the claim.
#[derive(Debug)]
pub(in crate::log) struct JobSlot(Arc<CompactionGate>);

impl JobSlot {
    /// Mark the build as running, or refuse with [`LogError::Fenced`] when the
    /// log is fenced.
    fn start_building(&self) -> Result<Building<'_>, LogError> {
        let mut state = self.0.state();
        if self.0.fenced.load(Ordering::Acquire) {
            return Err(LogError::Fenced);
        }
        state.building = true;
        Ok(Building(&self.0))
    }
}

impl Drop for JobSlot {
    fn drop(&mut self) { self.0.state().job = false; }
}

/// A running build. Dropping it, on return or on a panic, ends the build and
/// wakes a fence that waits for it.
struct Building<'a>(&'a CompactionGate);

impl Drop for Building<'_> {
    fn drop(&mut self) {
        self.0.state().building = false;
        self.0.build_ended.notify_all();
    }
}

/// One output group of the job: how many inputs it consumes, and the aborted
/// transactions of its range, which its rewrite adds to the walk's tracker.
#[derive(Debug)]
pub(in crate::log) struct GroupPlan {
    pub(in crate::log) len: usize,
    pub(in crate::log) aborted: Vec<AbortedTxn>,
}

/// A compaction pass taken from a log by [`Log::prepare_compaction`], ready
/// to [`build`](Self::build) without the log.
///
/// [`Log::prepare_compaction`]: super::Log::prepare_compaction
#[derive(Debug)]
pub struct CompactionJob {
    pub(in crate::log) dir: PathBuf,
    pub(in crate::log) io: Arc<dyn LogIo>,
    /// The sealed segments the pass consumes, oldest first.
    pub(in crate::log) inputs: Vec<SealedInput>,
    /// The output groups, in order. Their lengths sum to `inputs.len()`.
    pub(in crate::log) groups: Vec<GroupPlan>,
    /// The aborted transactions of the whole consumed range, for the map.
    pub(in crate::log) map_aborted: Vec<AbortedTxn>,
    pub(in crate::log) active_producers: HashMap<ProducerId, ProducerLastRecord>,
    /// The offset after the last consumed segment.
    pub(in crate::log) upper_bound: Offset,
    pub(in crate::log) record_limit: Option<ByteSize>,
    pub(in crate::log) retention: RewriteRetention,
    pub(in crate::log) index_interval: ByteSize,
    /// The rollover flushes submitted when the job was prepared, which cover
    /// every consumed segment. The install waits for them.
    pub(in crate::log) flushes: FlushTicket,
    pub(in crate::log) slot: JobSlot,
    /// Whether the build may have written `.cleaned` files that no install
    /// has taken. Dropping the job deletes them then, so a pass that is
    /// abandoned, finished as stale, or failed leaves nothing behind.
    pub(in crate::log) output_unclaimed: bool,
}

impl CompactionJob {
    /// Read the consumed segments and write the pass's `.cleaned` files.
    ///
    /// The build reads the segments through file handles it took from them,
    /// and touches nothing the log holds, so it needs no lock on the log. A
    /// failure, a [`LogError::Fenced`] from a fence among them, travels in
    /// the result to [`Log::finish_compaction`], which decides what it means.
    ///
    /// [`Log::finish_compaction`]: super::Log::finish_compaction
    #[must_use]
    pub fn build(mut self) -> BuiltCompaction {
        self.output_unclaimed = true;
        let rewrites = self.rewrite();
        BuiltCompaction {
            job: self,
            rewrites,
        }
    }

    fn rewrite(&mut self) -> Result<Vec<GroupRewrite>, LogError> {
        let _building = self.slot.start_building()?;
        let offset_map = build_offset_map(
            &self.inputs,
            std::mem::take(&mut self.map_aborted),
            self.record_limit,
        )?;
        // One tracker walks every group in order, as a transaction can span
        // two of them.
        let mut txn_meta = CleanedTransactionMetadata::default();
        let mut rewrites = Vec::with_capacity(self.groups.len());
        let mut start = 0;
        for group in &mut self.groups {
            let inputs = &self.inputs[start..start + group.len];
            // The rewrite creates its files before it reads a batch, so a
            // fenced job must stop here.
            if inputs.iter().any(SealedInput::is_fenced) {
                return Err(LogError::Fenced);
            }
            txn_meta.add_aborted_transactions(std::mem::take(&mut group.aborted));
            let rewrite = rewrite_segments(
                Arc::clone(&self.io),
                &self.dir,
                inputs,
                &offset_map,
                &mut txn_meta,
                self.retention,
                CleaningRound {
                    active_producers: &self.active_producers,
                    // The round rewrites the offset map's whole range, so it
                    // ends where the last consumed segment does.
                    upper_bound: self.upper_bound,
                    max_decompressed_record: self.record_limit,
                    index_interval: self.index_interval,
                },
            )?;
            rewrites.push(GroupRewrite {
                group_bases: inputs.iter().map(SealedInput::base_offset).collect(),
                rewrite,
            });
            start += group.len;
        }
        Ok(rewrites)
    }

    /// Delete the `.cleaned` files the build may have written, as far as the
    /// directory allows. Recovery deletes any it leaves.
    fn discard_output(&self) {
        let mut start = 0;
        for group in &self.groups {
            let base = self.inputs[start].base_offset();
            for extension in CLEANED_EXTENSIONS {
                let path = cleaned_path(&self.dir, base.0, extension);
                if let Err(error) = remove_if_present(&*self.io, &path) {
                    tracing::warn!(path = %path.display(), %error,
                        "deleting an abandoned compaction output failed");
                }
            }
            start += group.len;
        }
    }
}

impl Drop for CompactionJob {
    fn drop(&mut self) {
        if self.output_unclaimed {
            self.discard_output();
        }
    }
}

/// A built [`CompactionJob`], for [`Log::finish_compaction`] to install.
/// Dropping it instead deletes the files the build wrote.
///
/// [`Log::finish_compaction`]: super::Log::finish_compaction
#[derive(Debug)]
pub struct BuiltCompaction {
    pub(in crate::log) job: CompactionJob,
    pub(in crate::log) rewrites: Result<Vec<GroupRewrite>, LogError>,
}

/// What [`Log::finish_compaction`] did with a built pass.
///
/// [`Log::finish_compaction`]: super::Log::finish_compaction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum CompactionOutcome {
    /// The pass replaced the segments it read.
    Installed,
    /// The log dropped or replaced a segment the pass read, or was fenced or
    /// closed, while the pass was built. The pass changed nothing, and its
    /// files are gone. The next pass starts from the log as it is.
    Stale,
}
