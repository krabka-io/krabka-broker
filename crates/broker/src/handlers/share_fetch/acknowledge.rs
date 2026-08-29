//! Application of one piggybacked acknowledgement batch to a share
//! partition's acquisition state.
//!
//! `ShareAcknowledge` applies the same batches without a fetch, so this step
//! is shared and not folded into the acquire pass.

use std::time::Instant;

use krabka_log::Offset;

use crate::{codes, share_partition::state::AckType};

/// Applies one acknowledgement batch to the state machine.
///
/// A singleton `acknowledge_types` applies that type across the whole range;
/// otherwise each entry maps to one offset, starting at `first`. This function
/// merges a run of the same type into one `acknowledge` call. An empty array
/// applies `Accept` across `[first, last]`. It returns the first error code that
/// it met.
pub(crate) fn apply_one_ack(
    st: &mut crate::share_partition::state::AcquisitionState,
    member: &str,
    first: i64,
    last: i64,
    types: &[i8],
    now: Instant,
) -> Result<(), i16> {
    if types.is_empty() {
        let ack = AckType::Accept;
        return st.acknowledge(member, Offset(first), Offset(last), ack, now);
    }
    if types.len() == 1 {
        let ack = AckType::from_i8(types[0]).ok_or(codes::INVALID_RECORD_STATE)?;
        return st.acknowledge(member, Offset(first), Offset(last), ack, now);
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
        if let Some(ack) = AckType::from_i8(t) {
            if let Err(code) = st.acknowledge(member, Offset(run_start), Offset(run_end), ack, now)
            {
                result = Err(code);
            }
        } else {
            result = Err(codes::INVALID_RECORD_STATE);
        }
        run_start = run_end + 1;
        idx = j;
    }
    result
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
                    i32::MAX,
                    Instant::now(),
                    Duration::from_secs(30),
                    5
                )
                .len()
                == 1
        );

        apply_one_ack(&mut state, "member", 0, 199, &[1], Instant::now()).expect("acknowledge");

        assert!(state.start_offset == Offset(200));
        assert!(
            state
                .acquire(
                    "other",
                    200,
                    i32::MAX,
                    Instant::now(),
                    Duration::from_secs(30),
                    5
                )
                .is_empty()
        );
    }
}
