use creusot_std::prelude::*;

use super::completion_offset;

mod order;

/// A sorted insertion followed by a one-row prefix cut keeps the greatest
/// five distinct offsets. This isolates the inverse-index proof from the
/// mutable Vec and producer-epoch branches of the executable selector.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(ends.len() <= 5 && 0 <= position && position <= ends.len())]
#[requires(first == if ends.len() == 5 { 1 } else { 0 })]
#[requires(selected.len() == ends.len() + 1 - first)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < ends.len() ==> ends[i]@ < ends[j]@)]
#[requires(forall<i: Int> 0 <= i && i < position ==> ends[i]@ < incoming@)]
#[requires(forall<i: Int> position <= i && i < ends.len() ==> incoming@ < ends[i]@)]
#[requires(forall<j: Int> 0 <= j && j < selected.len() ==> selected[j]@ ==
    if first + j == position { ends.len() }
    else if first + j > position { first + j - 1 } else { first + j })]
#[ensures(result)]
#[ensures(result ==> selected.len() > 0 && selected.len() <= 5)]
#[ensures(result ==> (forall<j: Int> 0 <= j && j < selected.len() ==> selected[j]@ <= ends.len()))]
#[ensures(result ==> (forall<i: Int, j: Int> 0 <= i && i < j && j < selected.len() ==>
    completion_offset(ends, incoming, selected[i]@) < completion_offset(ends, incoming, selected[j]@)))]
#[ensures(result ==> (forall<i: Int> 0 <= i && i < ends.len()
    && !(exists<j: Int> 0 <= j && j < selected.len() && selected[j]@ == i) ==>
        selected.len() == 5 && (forall<j: Int> 0 <= j && j < selected.len() ==>
            ends[i]@ < completion_offset(ends, incoming, selected[j]@))))]
#[ensures(result ==> (!(exists<j: Int> 0 <= j && j < selected.len()
    && completion_offset(ends, incoming, selected[j]@) == incoming@) ==>
        selected.len() == 5 && (forall<j: Int> 0 <= j && j < selected.len() ==>
            incoming@ < completion_offset(ends, incoming, selected[j]@))))]
pub(super) fn lemma_inserted_window(
    ends: Seq<i64>,
    incoming: i64,
    position: Int,
    first: Int,
    selected: Seq<usize>,
) -> bool {
    proof_assert!(forall<i: Int> 0 <= i && i < ends.len()
        && (if i >= position { i + 1 >= first } else { i >= first }) ==>
        selected[(if i >= position { i + 1 } else { i }) - first]@ == i);
    proof_assert!(position >= first ==> selected[position - first]@ == ends.len());
    proof_assert!(forall<i: Int> 0 <= i && i < ends.len()
        && !(exists<j: Int> 0 <= j && j < selected.len() && selected[j]@ == i) ==>
        i == 0 && ends[i] == ends[0] && first == 1 && position > 0 && selected.len() == 5);
    proof_assert!(forall<j: Int> 0 <= j && j < selected.len() ==>
        selected[j]@ == order::insertion_source(ends.len(), position, first + j));
    proof_assert!(forall<i: Int, j: Int> 0 <= i && i < j && j < selected.len() ==> {
        let checked = order::lemma_inserted_pair(ends, incoming, position, first + i, first + j);
        checked && completion_offset(ends, incoming, selected[i]@)
            < completion_offset(ends, incoming, selected[j]@)
    });
    proof_assert!(forall<j: Int> first == 1 && position > 0 && 0 <= j && j < selected.len() ==> {
        let checked = order::lemma_inserted_pair(ends, incoming, position, 0, first + j);
        checked && ends[0]@ < completion_offset(ends, incoming, selected[j]@)
    });
    true
}
