//! Snapshot-transfer chunk admission.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Pure decision for one snapshot response chunk.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum SnapshotChunkDecision {
    Restart,
    Continue { next_position: i64 },
    Complete,
}

/// Admit exactly the next bounded chunk for one fixed snapshot identity and size.
#[allow(
    clippy::comparison_chain,
    reason = "the explicit branches mirror the proved overshoot, exact, and incomplete cases"
)]
#[ensures(match result {
    SnapshotChunkDecision::Restart => !identity_matches
        || received@ < 0
        || chunk_len@ < 0
        || max_size@ < 0
        || declared_size@ < 0
        || declared_size@ > max_size@
        || position@ != received@
        || match fixed_size {
            Some(size) => size@ != declared_size@,
            None => false,
        }
        || received@ + chunk_len@ > declared_size@,
    SnapshotChunkDecision::Continue { next_position } => identity_matches
        && received@ >= 0
        && chunk_len@ >= 0
        && max_size@ >= 0
        && declared_size@ >= 0
        && declared_size@ <= max_size@
        && position@ == received@
        && match fixed_size {
            Some(size) => size@ == declared_size@,
            None => true,
        }
        && next_position@ == received@ + chunk_len@
        && next_position@ < declared_size@,
    SnapshotChunkDecision::Complete => identity_matches
        && received@ >= 0
        && chunk_len@ >= 0
        && max_size@ >= 0
        && declared_size@ >= 0
        && declared_size@ <= max_size@
        && position@ == received@
        && match fixed_size {
            Some(size) => size@ == declared_size@,
            None => true,
        }
        && received@ + chunk_len@ == declared_size@,
})]
#[must_use]
pub fn snapshot_chunk_admission(
    identity_matches: bool,
    fixed_size: Option<i64>,
    received: i64,
    declared_size: i64,
    position: i64,
    chunk_len: i64,
    max_size: i64,
) -> SnapshotChunkDecision {
    if !identity_matches
        || received < 0
        || chunk_len < 0
        || max_size < 0
        || declared_size < 0
        || declared_size > max_size
        || position != received
    {
        return SnapshotChunkDecision::Restart;
    }
    if let Some(size) = fixed_size
        && size != declared_size
    {
        return SnapshotChunkDecision::Restart;
    }
    let Some(next_position) = received.checked_add(chunk_len) else {
        return SnapshotChunkDecision::Restart;
    };
    if next_position > declared_size {
        SnapshotChunkDecision::Restart
    } else if next_position == declared_size {
        SnapshotChunkDecision::Complete
    } else {
        SnapshotChunkDecision::Continue { next_position }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn chunk_admission_covers_retries_order_bounds_and_completion() {
        use SnapshotChunkDecision::{Complete, Continue, Restart};

        check!(
            snapshot_chunk_admission(true, None, 0, 6, 0, 3, 10) == Continue { next_position: 3 }
        );
        check!(snapshot_chunk_admission(true, Some(6), 3, 6, 3, 3, 10) == Complete);
        check!(snapshot_chunk_admission(true, None, 0, 0, 0, 0, 10) == Complete);

        for decision in [
            snapshot_chunk_admission(false, Some(6), 3, 6, 3, 3, 10),
            snapshot_chunk_admission(true, Some(7), 3, 6, 3, 3, 10),
            snapshot_chunk_admission(true, Some(6), 3, 6, 0, 3, 10),
            snapshot_chunk_admission(true, Some(6), 3, 6, 2, 3, 10),
            snapshot_chunk_admission(true, Some(6), 3, 6, 4, 2, 10),
            snapshot_chunk_admission(true, Some(6), 3, 6, 3, 4, 10),
            snapshot_chunk_admission(true, None, 0, -1, 0, 0, 10),
            snapshot_chunk_admission(true, None, 0, 11, 0, 1, 10),
            snapshot_chunk_admission(true, None, i64::MAX, i64::MAX, i64::MAX, 1, i64::MAX),
        ] {
            check!(decision == Restart);
        }
    }
}
