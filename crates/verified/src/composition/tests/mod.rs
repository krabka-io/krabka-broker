use proptest::prelude::*;

use super::*;

mod abort_union;
mod consume_trace;
mod list_offsets;
mod refill;
mod tiered_timestamp;
mod typed_timestamp;

fn token_acl(operation: AclOperationKind, allow: bool, flags: u16) -> TokenDescriptionAcl {
    let bit = |index| flags & (1_u16 << index) != 0_u16;
    TokenDescriptionAcl {
        resource: AclResourceFacts {
            resource_type: if bit(0) {
                AclResourceTypeMatch::Same
            } else {
                AclResourceTypeMatch::Different
            },
            exact_name: bit(1),
            wildcard_name: bit(2),
            name_has_prefix: bit(3),
        },
        pattern: if bit(4) {
            AclPatternKind::Literal
        } else {
            AclPatternKind::Prefixed
        },
        operation,
        allow,
        principal: (bit(5), bit(6)),
        host: (bit(7), bit(8), bit(9)),
    }
}

fn recovered_window_row(base: i64, delta: i32, last_sequence: i32) -> ProducerSnapshotEntryFacts {
    ProducerSnapshotEntryFacts {
        producer_id: 42,
        producer_epoch: 7,
        last_sequence,
        last_offset: base + i64::from(delta),
        offset_delta: delta,
        coordinator_epoch: -1,
        current_txn_first_offset: -1,
    }
}

mod charge_refund_agrees_with_a_signed_ledger_and_detects_storage_loss;

mod replayed_window_keeps_first_alias_and_pads_the_last_batch_at_slot_four;

mod snapshot_retry_covers_wraparound_exhaustion_and_invalid_rows;

mod sparse_timestamp_composition_matches_full_record_oracle;

mod checked_wal_copy_boundaries;

mod trim_composition_boundaries_expose_inherited_frontier_requirement;

mod delivery_replication_and_restore_composition_boundaries;

mod restored_state_composition_boundaries;

mod restore_retry;

mod append;

mod stable_fetch;

mod loss_replay;

mod scheduled_prefix;

mod wal_placement;

mod epoch_replay;

mod offset_seek;
