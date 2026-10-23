//! The shared freeze payload shape and its wire-response adapter.

use krabka_protocol::krabka::freeze::DescribedTopicFreeze;

/// The signed fields of one topic-freeze record, shared by the broker and the
/// operator CLI. Each side still encodes these fields independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreezeSigningInput<'a> {
    pub cluster_id: &'a str,
    pub pattern_type: i8,
    pub scope: &'a str,
    pub frozen: bool,
    pub reason: &'a str,
    pub set_by: &'a str,
    pub set_at_ms: i64,
    pub proposal_id: [u8; 16],
}

/// Project a described freeze into its signed fields for a freeze or thaw.
#[bon::builder]
pub fn freeze_signing_input<'a>(
    cluster_id: &'a str,
    entry: &'a DescribedTopicFreeze,
    frozen: bool,
) -> FreezeSigningInput<'a> {
    FreezeSigningInput {
        cluster_id,
        pattern_type: entry.pattern_type,
        scope: &entry.scope,
        frozen,
        reason: &entry.reason,
        set_by: &entry.set_by,
        set_at_ms: entry.set_at_ms,
        proposal_id: entry.proposal_id.0,
    }
}


#[cfg(test)]
mod tests {
    use {
        super::{FreezeSigningInput, freeze_signing_input},
        assert2::check,
        krabka_protocol::{krabka::freeze::DescribedTopicFreeze, primitives::uuid::Uuid},
    };

    #[test]
    fn the_adapter_preserves_every_signed_field_for_freeze_and_thaw() {
        let entry = DescribedTopicFreeze {
            pattern_type: 4,
            scope: "tenant-a.".to_owned(),
            reason: "maintenance".to_owned(),
            set_by: "User:alice".to_owned(),
            set_at_ms: 0x0102_0304_0506_0708,
            proposal_id: Uuid([7; 16]),
            ..DescribedTopicFreeze::default()
        };
        for frozen in [true, false] {
            check!(
                freeze_signing_input()
                    .cluster_id("cluster-a")
                    .entry(&entry)
                    .frozen(frozen)
                    .call()
                    == FreezeSigningInput {
                        cluster_id: "cluster-a",
                        pattern_type: 4,
                        scope: "tenant-a.",
                        frozen,
                        reason: "maintenance",
                        set_by: "User:alice",
                        set_at_ms: 0x0102_0304_0506_0708,
                        proposal_id: [7; 16],
                    }
            );
        }
    }
}
