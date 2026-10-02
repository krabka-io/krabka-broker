use creusot_std::prelude::*;

use super::{
    ProducerDecision, ProducerSnapshotEntryFacts, rebuilt_data_window_bounds_retry,
    whole_batch_truncation_bounds_controls,
};

// Actual end, retained history count, rebuilt window start, decision, retry witness.
type TruncatedRetry = (
    i64,
    usize,
    usize,
    ProducerDecision,
    Option<(usize, i64, i64, bool)>,
);

/// Rebuild the last five data batches at the latest surviving producer epoch
/// from the physical prefix. Older aliases can reenter after tail deletion;
/// the selected original history index and acknowledgement must describe that
/// rebuilt window. Every duplicate frontier fits the retained end, and clamping
/// the HWM preserves readiness for that returned frontier. Complete data history,
/// faithful rows and complete physical batch ends remain host obligations.
#[requires(0 <= physical_start@ && physical_start@ <= cut@)]
#[requires(forall<i: Int> 0 <= i && i < ends@.len() ==> physical_start@ < ends@[i]@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < ends@.len() ==> ends@[i]@ < ends@[j]@)]
#[requires(0 <= previous_hwm@ && previous_hwm@ <= if ends@.len() == 0 { physical_start@ } else { ends@[ends@.len() - 1]@ })]
#[requires(rows@.len() > 0
    && (forall<i: Int> 0 <= i && i < rows@.len() ==>
        crate::producer_snapshot::snapshot_entry_valid_model(
            if ends@.len() == 0 { physical_start@ } else { ends@[ends@.len() - 1]@ }, rows@[i])
        && rows@[i].last_offset@ >= 0
        && rows@[i].producer_id == rows@[0].producer_id
        && exists<j: Int> 0 <= j && j < ends@.len() && ends@[j]@ == rows@[i].last_offset@ + 1)
    && (forall<i: Int, j: Int> 0 <= i && i < j && j < rows@.len() ==>
        rows@[i].last_offset@ < rows@[j].last_offset@
        && rows@[i].producer_epoch@ <= rows@[j].producer_epoch@))]
#[ensures(physical_start@ <= result.0@ && result.0@ <= cut@
    && (result.0 == physical_start || exists<i: Int> 0 <= i && i < ends@.len() && ends@[i] == result.0)
    && (forall<i: Int> 0 <= i && i < ends@.len() && ends@[i]@ <= cut@ ==> ends@[i]@ <= result.0@)
    && result.2@ <= result.1@ && result.1@ <= rows@.len() && result.1@ - result.2@ <= 5
    && (forall<i: Int> 0 <= i && i < rows@.len() ==>
        (i < result.1@) == (rows@[i].last_offset@ < cut@)
        && (i < result.1@) == (rows@[i].last_offset@ < result.0@))
    && (result.1@ == 0 ==> result.2@ == 0)
    && (result.1@ > 0 ==> result.2@ < result.1@
        && (forall<i: Int> result.2@ <= i && i < result.1@ ==>
            rows@[i].producer_epoch == rows@[result.1@ - 1].producer_epoch)
        && (result.2@ > 0 && result.1@ - result.2@ < 5 ==>
            rows@[result.2@ - 1].producer_epoch != rows@[result.1@ - 1].producer_epoch))
    && (match result.3 { ProducerDecision::Duplicate { .. } => true, _ => false }) ==
        (exists<i: Int> result.2@ <= i && i < result.1@
            && request.0 == rows@[i].producer_epoch
            && request.1@ == crate::producer::sequence_modulo_2_31(rows@[i].last_sequence@ - rows@[i].offset_delta@)
            && rows@[i].last_sequence@ == crate::producer::sequence_modulo_2_31(request.1@ + request.2@))
    && match result.4 { None => (match result.3 { ProducerDecision::Duplicate { .. } => false, _ => true }),
        Some((index, base, frontier, ready)) => result.2@ <= index@ && index@ < result.1@
            && (match result.3 { ProducerDecision::Duplicate { retained: slot } =>
                slot@ == if index@ + 1 == result.1@ { 4 } else { index@ - result.2@ }, _ => false })
            && base@ == rows@[index@].last_offset@ - rows@[index@].offset_delta@
            && frontier@ == rows@[index@].last_offset@ + 1
            && 0 <= base@ && base@ < frontier@ && frontier@ <= result.0@
            && ready == (previous_hwm@ >= frontier@)
            && request.0 == rows@[index@].producer_epoch
            && request.1@ == crate::producer::sequence_modulo_2_31(rows@[index@].last_sequence@ - rows@[index@].offset_delta@)
            && rows@[index@].last_sequence@ == crate::producer::sequence_modulo_2_31(request.1@ + request.2@)
            && (forall<i: Int> result.2@ <= i && i < index@ ==>
                !(request.1@ == crate::producer::sequence_modulo_2_31(rows@[i].last_sequence@ - rows@[i].offset_delta@)
                && rows@[i].last_sequence@ == crate::producer::sequence_modulo_2_31(request.1@ + request.2@))),
    }
    && (result.1@ == 0 ==> result.3 == if request.3 && result.0@ == 0 && request.1@ != 0 {
        ProducerDecision::OutOfOrder } else { ProducerDecision::Append })
    && (result.1@ > 0 && result.4 == None ==> result.3 ==
        if request.0@ < rows@[result.1@ - 1].producer_epoch@ { ProducerDecision::Fenced }
        else if request.0@ > rows@[result.1@ - 1].producer_epoch@ {
            if request.1@ == 0 { ProducerDecision::Append } else { ProducerDecision::OutOfOrder }
        } else if request.1@ == crate::producer::sequence_modulo_2_31(
            rows@[result.1@ - 1].last_sequence@ + 1) { ProducerDecision::Append }
        else { ProducerDecision::OutOfOrder }))]
pub(super) fn truncated_replay_bounds_first_retry(
    ends: &[i64],
    physical_start: i64,
    cut: i64,
    previous_hwm: i64,
    rows: &[ProducerSnapshotEntryFacts],
    request: (i16, i32, i32, bool),
) -> TruncatedRetry {
    let mut offsets: Vec<i64> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= rows@.len() && offsets@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> offsets@[j] == rows@[j].last_offset)]
    #[variant(rows@.len() - i@)]
    while i < rows.len() {
        offsets.push(rows[i].last_offset);
        i += 1;
    }
    let (_, end, hwm, count, _, _, _) = whole_batch_truncation_bounds_controls(
        ends,
        physical_start,
        cut,
        previous_hwm,
        &offsets,
        0,
    );
    proof_assert!(forall<j: Int> 0 <= j && j < rows@.len() ==>
        (j < count@) == (rows@[j].last_offset@ < cut@));
    let mut prefix: Vec<ProducerSnapshotEntryFacts> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= count@ && prefix@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        prefix@[j] == rows@[j]
        && crate::producer_snapshot::snapshot_entry_valid_model(end@, prefix@[j]))]
    #[variant(count@ - i@)]
    while i < count {
        proof_assert!(crate::producer_snapshot::snapshot_entry_valid_model(end@, rows@[i@]));
        prefix.push(rows[i]);
        i += 1;
    }
    let (first, decision, witness) = rebuilt_data_window_bounds_retry(end, hwm, &prefix, request);
    (end, count, first, decision, witness)
}
