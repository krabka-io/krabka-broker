//! The lifecycle state machine of one remote log segment.
//!
//! [`RemoteLogSegmentState`] holds the four states a segment moves through and
//! the single rule that decides which move is legal, so every metadata update
//! is checked against one place.

/// Lifecycle state of a remote log segment.
///
/// Valid transitions (see [`RemoteLogSegmentState::is_valid_transition`]):
///
/// ```text
/// CopySegmentStarted ──► CopySegmentFinished ──► DeleteSegmentStarted ──► DeleteSegmentFinished
///         └───────────────────────────────────►┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RemoteLogSegmentState {
    /// A copy to the remote tier has begun but not finished. The starting
    /// state of every segment.
    CopySegmentStarted,
    /// The copy finished; the segment is durable in the remote tier and
    /// readable.
    CopySegmentFinished,
    /// Deletion from the remote tier has begun.
    DeleteSegmentStarted,
    /// The segment has been fully removed from the remote tier.
    DeleteSegmentFinished,
}

impl RemoteLogSegmentState {
    /// `true` if a segment currently in `self` may transition to `target`.
    ///
    /// A same-state "transition" is not valid. Callers treat it as a no-op
    /// or a duplicate, not as an advance.
    #[must_use]
    pub fn is_valid_transition(self, target: Self) -> bool {
        use RemoteLogSegmentState::{
            CopySegmentFinished, CopySegmentStarted, DeleteSegmentFinished, DeleteSegmentStarted,
        };
        matches!(
            (self, target),
            (
                CopySegmentStarted,
                CopySegmentFinished | DeleteSegmentStarted
            ) | (CopySegmentFinished, DeleteSegmentStarted)
                | (DeleteSegmentStarted, DeleteSegmentFinished)
        )
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn segment_state_valid_transitions() {
        use RemoteLogSegmentState::{
            CopySegmentFinished, CopySegmentStarted, DeleteSegmentFinished, DeleteSegmentStarted,
        };
        for (from, to) in [
            (CopySegmentStarted, CopySegmentFinished),
            (CopySegmentStarted, DeleteSegmentStarted),
            (CopySegmentFinished, DeleteSegmentStarted),
            (DeleteSegmentStarted, DeleteSegmentFinished),
        ] {
            check!(from.is_valid_transition(to), "{from:?} -> {to:?}");
        }
    }

    #[test]
    fn segment_state_invalid_transitions() {
        use RemoteLogSegmentState::{
            CopySegmentFinished, CopySegmentStarted, DeleteSegmentFinished, DeleteSegmentStarted,
        };
        // No backward / skipping / same-state transitions.
        for (from, to) in [
            (CopySegmentStarted, CopySegmentStarted),
            (CopySegmentStarted, DeleteSegmentFinished),
            (CopySegmentFinished, CopySegmentStarted),
            (CopySegmentFinished, CopySegmentFinished),
            (DeleteSegmentStarted, CopySegmentFinished),
            (DeleteSegmentFinished, DeleteSegmentStarted),
        ] {
            check!(!from.is_valid_transition(to), "{from:?} -> {to:?}");
        }
    }
}
