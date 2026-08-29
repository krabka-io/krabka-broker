//! One fixture builder shared by the unit tests of the `state` module tree.
//!
//! It turns `(subtopology, partitions)` pairs into the `BTreeMap` shape that
//! every role assignment uses, so each test states only the tasks it cares
//! about.

use std::collections::BTreeMap;

/// Builds a task map from `(subtopology_id, partitions)` pairs, verbatim: it
/// neither sorts nor dedups, so a test can feed a deliberately unnormalized
/// map in.
pub(super) fn task_map(entries: &[(&str, &[i32])]) -> BTreeMap<String, Vec<i32>> {
    entries
        .iter()
        .map(|(sub, parts)| (sub.to_string(), parts.to_vec()))
        .collect()
}
