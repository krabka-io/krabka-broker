use creusot_std::prelude::*;

use super::completion_offset;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn insertion_source(count: Int, position: Int, ordinal: Int) -> Int {
    pearlite! { if ordinal == position { count }
    else if ordinal > position { ordinal - 1 } else { ordinal } }
}

/// Any two positions in a sorted insertion retain their strict offset order.
/// This pointwise fact avoids mixing index arithmetic with quantified Vec
/// coverage and the producer selector's epoch branches.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= position && position <= ends.len())]
#[requires(0 <= lower && lower < upper && upper <= ends.len())]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < ends.len() ==> ends[i]@ < ends[j]@)]
#[requires(forall<i: Int> 0 <= i && i < position ==> ends[i]@ < incoming@)]
#[requires(forall<i: Int> position <= i && i < ends.len() ==> incoming@ < ends[i]@)]
#[ensures(result)]
#[ensures(result ==> completion_offset(ends, incoming, insertion_source(ends.len(), position, lower))
    < completion_offset(ends, incoming, insertion_source(ends.len(), position, upper)))]
pub(super) fn lemma_inserted_pair(
    ends: Seq<i64>,
    incoming: i64,
    position: Int,
    lower: Int,
    upper: Int,
) -> bool {
    true
}
