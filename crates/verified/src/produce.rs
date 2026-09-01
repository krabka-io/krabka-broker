//! Produce batch-header admission.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ProduceBatchAdmission {
    Admit,
    InvalidRecord,
    InvalidTimestamp,
}

#[allow(
    clippy::fn_params_excessive_bools,
    reason = "the proof classifies independent CRC-covered header facts"
)]
#[ensures((result == ProduceBatchAdmission::InvalidRecord) ==
    (base_offset@ != 0
        || last_offset_delta@ < 0
        || last_offset_delta@ >= i32::MAX@
        || records_count@ != last_offset_delta@ + 1
        || records_count@ <= 0
        || control_batch
        || (producer_id@ >= 0 && base_sequence@ < 0)))]
#[ensures((result == ProduceBatchAdmission::InvalidTimestamp) ==
    (base_offset@ == 0
        && 0 <= last_offset_delta@
        && last_offset_delta@ < i32::MAX@
        && records_count@ == last_offset_delta@ + 1
        && records_count@ > 0
        && !control_batch
        && (producer_id@ < 0 || base_sequence@ >= 0)
        && !create_time))]
#[ensures((result == ProduceBatchAdmission::Admit) ==
    (base_offset@ == 0
        && 0 <= last_offset_delta@
        && last_offset_delta@ < i32::MAX@
        && records_count@ == last_offset_delta@ + 1
        && records_count@ > 0
        && !control_batch
        && (producer_id@ < 0 || base_sequence@ >= 0)
        && create_time))]
#[must_use]
pub fn produce_batch_admission(
    base_offset: i64,
    last_offset_delta: i32,
    records_count: i32,
    control_batch: bool,
    producer_id: i64,
    base_sequence: i32,
    create_time: bool,
) -> ProduceBatchAdmission {
    if base_offset != 0
        || last_offset_delta < 0
        || last_offset_delta == i32::MAX
        || last_offset_delta + 1 != records_count
        || records_count <= 0
        || control_batch
        || (producer_id >= 0 && base_sequence < 0)
    {
        ProduceBatchAdmission::InvalidRecord
    } else if !create_time {
        ProduceBatchAdmission::InvalidTimestamp
    } else {
        ProduceBatchAdmission::Admit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produce_header_admission_is_ordered_and_exhaustive() {
        use ProduceBatchAdmission::{Admit, InvalidRecord, InvalidTimestamp};

        assert2::assert!(produce_batch_admission(0, 0, 1, false, -1, -1, true) == Admit);
        assert2::assert!(produce_batch_admission(1, 0, 1, false, -1, -1, true) == InvalidRecord);
        assert2::assert!(produce_batch_admission(0, -1, 0, false, -1, -1, true) == InvalidRecord);
        assert2::assert!(
            produce_batch_admission(0, i32::MAX, i32::MAX, false, -1, -1, true) == InvalidRecord
        );
        assert2::assert!(produce_batch_admission(0, 0, 0, false, -1, -1, true) == InvalidRecord);
        assert2::assert!(produce_batch_admission(0, 0, 1, true, -1, -1, true) == InvalidRecord);
        assert2::assert!(produce_batch_admission(0, 0, 1, false, 7, -1, true) == InvalidRecord);
        assert2::assert!(
            produce_batch_admission(0, 0, 1, false, -1, -1, false) == InvalidTimestamp
        );
    }
}
