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
        krabka_verified::remote_segment_transition(self.proof_tag(), target.proof_tag())
    }

    const fn proof_tag(self) -> u8 {
        match self {
            Self::CopySegmentStarted => 0,
            Self::CopySegmentFinished => 1,
            Self::DeleteSegmentStarted => 2,
            Self::DeleteSegmentFinished => 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn segment_state_transition_matrix_is_exhaustive() {
        use RemoteLogSegmentState::{
            CopySegmentFinished, CopySegmentStarted, DeleteSegmentFinished, DeleteSegmentStarted,
        };
        let states = [
            CopySegmentStarted,
            CopySegmentFinished,
            DeleteSegmentStarted,
            DeleteSegmentFinished,
        ];
        let expected = [
            [false, true, true, false],
            [false, false, true, false],
            [false, false, false, true],
            [false, false, false, false],
        ];
        for (from_index, from) in states.into_iter().enumerate() {
            for (to_index, to) in states.into_iter().enumerate() {
                check!(
                    from.is_valid_transition(to) == expected[from_index][to_index],
                    "{from:?} -> {to:?}"
                );
            }
        }
    }
}
