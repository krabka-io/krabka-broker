//! Application of one piggybacked acknowledgement batch to a share
//! partition's acquisition state.
//!
//! `ShareAcknowledge` applies the same batches without a fetch, so this step
//! is shared and not folded into the acquire pass.

use std::time::{Duration, Instant};

use krabka_log::Offset;
use krabka_metadata::MetadataImage;

use crate::{codes, share_partition::state::AckType};

/// The KIP-1222 acknowledge type `Renew`.
const ACK_RENEW: i8 = 4;

/// The group config that allows `Renew` acknowledgements.
const KEY_SHARE_RENEW_ACKNOWLEDGE_ENABLE: &str = "share.renew.acknowledge.enable";

/// How an acknowledgement request treats the acknowledge type `Renew`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Renewal {
    /// The request set `IsRenewAck`.
    pub(crate) requested: bool,
    /// The group allows renewals: `share.renew.acknowledge.enable`.
    pub(crate) enabled: bool,
    /// The new lock length of a renewed record.
    pub(crate) lock_duration: Duration,
}

/// Whether `group` allows `Renew` acknowledgements.
///
/// Kafka's `GroupConfig` defines `share.renew.acknowledge.enable` as a
/// boolean with the default `true`, and parses it without regard to case.
pub(crate) fn renew_acknowledge_enabled(image: &MetadataImage, group: &str) -> bool {
    image
        .group_config(group)
        .and_then(|configs| configs.get(KEY_SHARE_RENEW_ACKNOWLEDGE_ENABLE))
        .is_none_or(|value| !value.eq_ignore_ascii_case("false"))
}

/// The highest acknowledge type of a request version without `Renew`:
/// `Reject`.
const MAX_ACK_TYPE: i8 = 3;

/// Kafka's `KafkaApis.validateAcknowledgementBatches` for the batches of one
/// partition, as `(first_offset, last_offset, acknowledge_types)`.
///
/// A partition is refused with `INVALID_REQUEST` when a batch has its first
/// offset past its last, starts before the last offset of the batch in front
/// of it, has no acknowledge type, has more than one type but not one per
/// offset, has a type outside `[0, 3]` (`[0, 4]` when the version supports
/// `Renew`), or has the type `Renew` in a request without `IsRenewAck`.
pub(crate) fn acknowledgement_batches_are_valid<'a>(
    batches: impl IntoIterator<Item = (i64, i64, &'a [i8])>,
    supports_renew: bool,
    is_renew_ack: bool,
) -> bool {
    let max_type = if supports_renew {
        ACK_RENEW
    } else {
        MAX_ACK_TYPE
    };
    let mut previous_last = -1_i64;
    for (first, last, types) in batches {
        let per_offset = types.len() > 1;
        let covers_range = i64::try_from(types.len())
            .is_ok_and(|count| last.checked_sub(first) == Some(count - 1));
        let valid = first <= last
            && first >= previous_last
            && !types.is_empty()
            && (!per_offset || covers_range)
            && types.iter().all(|ack| (0..=max_type).contains(ack))
            && (is_renew_ack || !types.contains(&ACK_RENEW));
        if !valid {
            return false;
        }
        previous_last = last;
    }
    true
}

/// Applies one acknowledgement batch to the state machine.
///
/// A singleton `acknowledge_types` applies that type across the whole range;
/// otherwise each entry maps to one offset, starting at `first`. This function
/// merges a run of the same type into one call. An empty array applies
/// `Accept` across `[first, last]`. It returns the last error code that it
/// met.
///
/// The type `Renew` (4) renews the lock of its offsets and leaves their
/// state as it is. The other types take their normal transition in the same
/// batch, as in Kafka's `SharePartition.acknowledgePerOffsetBatchRecords` and
/// `acknowledgeCompleteBatch`. A `Renew` in a request without `IsRenewAck`
/// is `INVALID_REQUEST`, and a `Renew` for a group that does not allow it is
/// `INVALID_RECORD_STATE`.
pub(crate) fn apply_one_ack(
    st: &mut crate::share_partition::state::AcquisitionState,
    member: &str,
    first: i64,
    last: i64,
    types: &[i8],
    now: Instant,
    renewal: Renewal,
) -> Result<(), i16> {
    let apply = |st: &mut crate::share_partition::state::AcquisitionState,
                 run_first: i64,
                 run_last: i64,
                 ack_type: i8| {
        if ack_type == ACK_RENEW {
            if !renewal.requested {
                return Err(codes::INVALID_REQUEST);
            }
            if !renewal.enabled {
                return Err(codes::INVALID_RECORD_STATE);
            }
            return st.renew(
                member,
                Offset(run_first),
                Offset(run_last),
                now,
                renewal.lock_duration,
            );
        }
        let ack = AckType::from_i8(ack_type).ok_or(codes::INVALID_RECORD_STATE)?;
        st.acknowledge(member, Offset(run_first), Offset(run_last), ack, now)
    };
    if types.is_empty() {
        return apply(st, first, last, 1);
    }
    if types.len() == 1 {
        return apply(st, first, last, types[0]);
    }
    let range_len = last
        .checked_sub(first)
        .and_then(|len| len.checked_add(1))
        .and_then(|len| usize::try_from(len).ok());
    if range_len != Some(types.len()) {
        return Err(codes::INVALID_RECORD_STATE);
    }
    // Walk the per-offset type list, coalescing equal-typed runs.
    let mut result = Ok(());
    let mut run_start = first;
    let mut idx = 0_usize;
    while idx < types.len() {
        let t = types[idx];
        let mut run_end = run_start;
        let mut j = idx + 1;
        while j < types.len() && types[j] == t {
            run_end += 1;
            j += 1;
        }
        if let Err(code) = apply(st, run_start, run_end, t) {
            result = Err(code);
        }
        run_start = run_end + 1;
        idx = j;
    }
    result
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn singleton_ack_type_applies_to_the_whole_range() {
        let mut state = crate::share_partition::state::AcquisitionState::new(Offset(0));
        state.materialize(Offset(200), 200);
        assert!(
            state
                .acquire(
                    "member",
                    200,
                    krabka_log::Offset(i64::MAX),
                    Instant::now(),
                    Duration::from_secs(30),
                    5
                )
                .len()
                == 1
        );

        let renewal = Renewal {
            requested: false,
            enabled: true,
            lock_duration: Duration::from_secs(30),
        };
        apply_one_ack(&mut state, "member", 0, 199, &[1], Instant::now(), renewal)
            .expect("acknowledge");

        assert!(state.start_offset == Offset(200));
        assert!(
            state
                .acquire(
                    "other",
                    200,
                    krabka_log::Offset(i64::MAX),
                    Instant::now(),
                    Duration::from_secs(30),
                    5
                )
                .is_empty()
        );
    }
}
