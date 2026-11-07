//! The KIP-951 leader hint: which node leads a partition, and at what epoch,
//! as the metadata image names it.
//!
//! `Fetch`, `Produce`, `ShareFetch` and `ShareAcknowledge` each generate their
//! own `LeaderIdAndEpoch`. They all carry the same two values, so the handlers
//! work out the hint once, as a [`LeaderHint`], and convert it into the
//! response's type at the end.

use krabka_protocol::owned::{
    fetch_response, produce_response, share_acknowledge_response, share_fetch_response,
};

/// The leader a response names for a partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaderHint {
    pub leader_id: i32,
    pub leader_epoch: i32,
}

impl LeaderHint {
    /// The hint for a partition the image does not know: `-1` for both.
    pub const UNKNOWN: Self = Self {
        leader_id: -1,
        leader_epoch: -1,
    };

    /// The hint `record` names. A leader id that does not fit the wire's
    /// `i32` reads as `-1`, the wire's "leader unknown".
    pub fn of(record: &krabka_metadata::PartitionRecord) -> Self {
        Self {
            leader_id: i32::try_from(record.leader.0).unwrap_or(Self::UNKNOWN.leader_id),
            leader_epoch: record.leader_epoch.0,
        }
    }
}

/// Converts a [`LeaderHint`] into each API's generated `LeaderIdAndEpoch`,
/// with no unknown tagged fields.
macro_rules! into_leader_id_and_epoch {
    ($($api:ident),+ $(,)?) => {$(
        impl From<LeaderHint> for $api::LeaderIdAndEpoch {
            fn from(hint: LeaderHint) -> Self {
                Self {
                    leader_id: hint.leader_id,
                    leader_epoch: hint.leader_epoch,
                    ..Default::default()
                }
            }
        }
    )+};
}

into_leader_id_and_epoch!(
    fetch_response,
    produce_response,
    share_fetch_response,
    share_acknowledge_response,
);

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_metadata::{LeaderEpoch, NodeId, PartitionRecord},
    };

    fn record(leader: u64, leader_epoch: i32) -> PartitionRecord {
        PartitionRecord {
            leader: NodeId(leader),
            leader_epoch: LeaderEpoch(leader_epoch),
            ..Default::default()
        }
    }

    #[test]
    fn of_reads_the_record_leader_and_epoch() {
        assert!(
            LeaderHint::of(&record(2, 5))
                == LeaderHint {
                    leader_id: 2,
                    leader_epoch: 5
                }
        );
    }

    #[test]
    fn of_reports_an_id_past_i32_as_unknown() {
        let hint = LeaderHint::of(&record(u64::from(u32::MAX), 7));
        assert!(
            hint == LeaderHint {
                leader_id: -1,
                leader_epoch: 7
            }
        );
    }

    #[test]
    fn unknown_matches_the_fetch_and_produce_wire_default() {
        assert!(
            fetch_response::LeaderIdAndEpoch::from(LeaderHint::UNKNOWN)
                == fetch_response::LeaderIdAndEpoch::default()
        );
        assert!(
            produce_response::LeaderIdAndEpoch::from(LeaderHint::UNKNOWN)
                == produce_response::LeaderIdAndEpoch::default()
        );
    }

    #[test]
    fn converts_into_every_api_unchanged() {
        let hint = LeaderHint {
            leader_id: 3,
            leader_epoch: 9,
        };
        let share_fetch = share_fetch_response::LeaderIdAndEpoch::from(hint);
        let share_ack = share_acknowledge_response::LeaderIdAndEpoch::from(hint);
        assert!((share_fetch.leader_id, share_fetch.leader_epoch) == (3, 9));
        assert!((share_ack.leader_id, share_ack.leader_epoch) == (3, 9));
    }
}
