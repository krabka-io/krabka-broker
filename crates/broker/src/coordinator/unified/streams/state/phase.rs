//! The KIP-1071 group lifecycle phase and the Kafka group-state string it
//! serializes to.
//!
//! The phase is what `DescribeGroups`, `ListGroups`, and the admin tools read,
//! so its string mapping is a wire-visible contract and is kept apart from the
//! state machine that sets it.

/// The KIP-1071 group lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamsGroupStatePhase {
    /// No members.
    #[default]
    Empty,
    /// The group has members but cannot be assigned yet. Usually no topology
    /// is initialized, or required internal topics are missing.
    NotReady,
    /// A reconcile is in flight computing a new target assignment.
    Assigning,
    /// A target exists, and members converge on it by revoking and
    /// installing tasks.
    Reconciling,
    /// All members are at the assignment epoch with no pending revocations.
    Stable,
}

impl StreamsGroupStatePhase {
    /// The Kafka group-state string this phase serializes to.
    /// `DescribeGroups`, `ListGroups`, and the admin tools read it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::NotReady => "NotReady",
            Self::Assigning => "Assigning",
            Self::Reconciling => "Reconciling",
            Self::Stable => "Stable",
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn group_state_phase_as_str_strings() {
        for (phase, want) in [
            (StreamsGroupStatePhase::Empty, "Empty"),
            (StreamsGroupStatePhase::NotReady, "NotReady"),
            (StreamsGroupStatePhase::Assigning, "Assigning"),
            (StreamsGroupStatePhase::Reconciling, "Reconciling"),
            (StreamsGroupStatePhase::Stable, "Stable"),
        ] {
            assert!(phase.as_str() == want);
        }
        assert!(StreamsGroupStatePhase::default() == StreamsGroupStatePhase::Empty);
    }
}
