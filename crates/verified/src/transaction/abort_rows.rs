use creusot_std::prelude::*;

/// Preserve every distinct wire `(producer_id, first_offset)` row exactly
/// once. The caller has already selected the overlapping transactions.
#[ensures(result@.len() <= rows@.len())]
#[ensures(forall<i: Int> 0 <= i && i < result@.len()
    ==> exists<j: Int> 0 <= j && j < rows@.len() && result@[i] == rows@[j])]
#[ensures(forall<i: Int> 0 <= i && i < rows@.len()
    ==> exists<j: Int> 0 <= j && j < result@.len() && rows@[i] == result@[j])]
#[ensures(forall<i: Int, j: Int> 0 <= i && i < j && j < result@.len()
    ==> result@[i] != result@[j])]
#[must_use]
pub fn unique_aborted_transaction_rows(rows: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut selected: Vec<(i64, i64)> = Vec::new();
    let mut index = 0usize;
    #[invariant(index@ <= rows@.len() && selected@.len() <= index@)]
    #[invariant(forall<i: Int> 0 <= i && i < selected@.len()
        ==> exists<j: Int> 0 <= j && j < index@ && selected@[i] == rows@[j])]
    #[invariant(forall<i: Int> 0 <= i && i < index@
        ==> exists<j: Int> 0 <= j && j < selected@.len() && rows@[i] == selected@[j])]
    #[invariant(forall<i: Int, j: Int> 0 <= i && i < j && j < selected@.len()
        ==> selected@[i] != selected@[j])]
    #[variant(rows@.len() - index@)]
    while index < rows.len() {
        let row = rows[index];
        let mut scan = 0usize;
        // ponytail: linear duplicate search; use an indexed set if abort
        // lists become large enough to affect Fetch latency.
        #[invariant(scan@ <= selected@.len())]
        #[invariant(forall<j: Int> 0 <= j && j < scan@ ==> selected@[j] != row)]
        #[variant(selected@.len() - scan@)]
        while scan < selected.len() && !(selected[scan].0 == row.0 && selected[scan].1 == row.1) {
            scan += 1;
        }
        if scan == selected.len() {
            selected.push(row);
        }
        index += 1;
    }
    selected
}
