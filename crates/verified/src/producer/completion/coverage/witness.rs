use creusot_std::prelude::*;

/// Construct the retained slot for an old source outside the removed prefix.
/// A concrete witness lets the caller contradict an omitted source without
/// asking the solver to invert a quantified conditional index expression.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= position && position <= count && 0 <= first)]
#[requires(0 <= source && source < count)]
#[requires(if source < position { first <= source } else { first <= source + 1 })]
#[requires(selected.len() == count + 1 - first)]
#[requires(forall<j: Int> 0 <= j && j < selected.len() ==> selected[j]@ ==
    if first + j == position { count }
    else if first + j > position { first + j - 1 } else { first + j })]
#[ensures(0 <= result && result < selected.len())]
#[ensures(selected[result]@ == source)]
pub(super) fn old_source_slot(
    selected: Seq<usize>,
    count: Int,
    position: Int,
    first: Int,
    source: Int,
) -> Int {
    if source < position {
        source - first
    } else {
        source + 1 - first
    }
}
