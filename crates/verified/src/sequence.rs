//! Shared quantified laws for sequence order and uniqueness.
use creusot_std::prelude::*;

open_logic! {
/// No pair of distinct indexes contains equal values.
pub fn distinct<T>(values: Seq<T>) -> bool {
    pearlite! { pairwise(values, |(left, right): (T, T)| left != right) }
}
}

open_logic! {
/// Integer views increase strictly with their indexes.
pub fn strictly_increasing<T: View<ViewTy = Int>>(values: Seq<T>) -> bool {
    pearlite! { pairwise(values, |(left, right): (T, T)| left@ < right@) }
}
}

open_logic! {
/// Apply a relation to every pair of values in source-index order.
pub fn pairwise<T>(values: Seq<T>, relation: creusot_std::logic::Mapping<(T, T), bool>) -> bool {
    pearlite! { forall<i: Int, j: Int> 0 <= i && i < j && j < values.len()
    ==> relation.get((values[i], values[j])) }
}
}

open_logic! {
/// The selected source-index sequence includes this original position.
pub(crate) fn contains_source_index(selected: Seq<usize>, index: Int) -> bool {
    pearlite! { exists<j: Int> 0 <= j && j < selected.len() && selected[j]@ == index }
}
}
