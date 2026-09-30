use creusot_std::prelude::*;

use super::{
    offset_index_lookup, offset_index_position_at_or_after, restore_offset_index_entry_valid,
};

/// The actual per-row archive validator establishes both binary-search
/// ordering and byte bounds. The floor cannot lie after a present ceiling.
#[ensures(result)]
pub(super) fn validated_index_bounds_lookup(
    entries: &[(u32, u32)],
    target: u32,
    max_relative: i64,
    log_bytes: u64,
) -> bool {
    let mut i = 0usize;
    let mut previous = None;
    #[invariant(i@ <= entries@.len())]
    #[invariant(previous == if i@ == 0 { None } else { Some(entries@[i@ - 1]) })]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].1@ < log_bytes@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@
        ==> entries@[j].0@ < entries@[k].0@ && entries@[j].1@ < entries@[k].1@)]
    #[variant(entries@.len() - i@)]
    while i < entries.len() {
        let (relative, position) = entries[i];
        if !restore_offset_index_entry_valid(previous, relative, position, max_relative, log_bytes)
        {
            // An invalid archive never reaches lookup.
            return true;
        }
        previous = Some((relative, position));
        i += 1;
    }
    let floor = offset_index_lookup(entries, target);
    if !matches!(entries.len(), 0) && u64::from(floor) >= log_bytes {
        return false;
    }
    match offset_index_position_at_or_after(entries, target) {
        Some(ceiling) => floor <= ceiling && u64::from(ceiling) < log_bytes,
        None => true,
    }
}
