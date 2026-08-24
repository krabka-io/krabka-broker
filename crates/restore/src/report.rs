//! The restore summary an operator reads, and its text and JSON renderings.
//!
//! This module owns the report model and nothing else. It aggregates what the
//! other stages establish: which partitions were restored, how many segments
//! and records each kept, where each partition's log now ends, which segments
//! failed verification and were skipped under `--continue-on-corrupt`, and
//! which bounds were applied. The text rendering is for a human during an
//! incident, and the JSON rendering is for a runbook that has to assert on the
//! outcome. Both render the same model, so they can never disagree. A dry run
//! produces the same report as a real run, with `dry_run` set.

use std::path::PathBuf;

use serde::Serialize;
use uuid::Uuid;

use crate::materialize::SegmentOutcome;

/// How [`RestoreReport::render`] writes the summary.
///
/// This is the `--report` value, so it is part of the command-line surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, clap::ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReportFormat {
    /// Aligned plain text, for a person.
    #[default]
    Text,
    /// One JSON object, for a script.
    Json,
}

/// What the restore did, as a whole.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RestoreReport {
    /// Whether the run wrote anything.
    pub dry_run: bool,
    /// The target log directory.
    pub log_dir: PathBuf,
    /// The cluster id the target was formatted with.
    pub cluster_id: Uuid,
    /// One entry per restored partition, ordered by topic then partition.
    pub partitions: Vec<PartitionReport>,
    /// Segments that failed verification and were skipped.
    pub skipped: Vec<SkippedSegment>,
}

/// What the restore did for one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PartitionReport {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Stable topic id, as the archive records it.
    pub topic_id: Uuid,
    /// One entry per segment the restore wrote, in offset order.
    pub segments: Vec<SegmentOutcome>,
}

/// One segment the restore could not verify and did not write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkippedSegment {
    /// Topic of the skipped segment.
    pub topic: String,
    /// Partition index of the skipped segment.
    pub partition: i32,
    /// The segment id the archive names the copy with.
    pub segment_id: Uuid,
    /// Why verification rejected it.
    pub reason: String,
}

impl RestoreReport {
    /// Render the report in `format`.
    ///
    /// The rendering is total. Every field of the model is a string, a number,
    /// or a sequence of the same, so the JSON encoder has no failure mode to
    /// report.
    #[must_use]
    pub fn render(&self, _format: ReportFormat) -> String {
        todo!("render the restore summary as text and as JSON")
    }
}
