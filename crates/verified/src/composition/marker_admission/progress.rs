use creusot_std::prelude::*;

// Isolate the integer successor witness from admission and marker decoding.
// cargo-mutants: #[cfg(creusot)] lemma; checked by Creusot, absent from native builds.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(start < caps.0 && start < caps.1 && start < caps.2)]
#[requires(forall<i: Int> 0 <= i && i < others.len() ==> start < others[i]@)]
#[requires(forall<v: Int> v <= caps.0 && v <= caps.1 && v <= caps.2
    && (forall<i: Int> 0 <= i && i < others.len() ==> v <= others[i]@)
    ==> v <= limit)]
#[ensures(result)]
#[ensures(result ==> start < limit)]
pub(super) fn lemma_maximal_prefix_advances(
    start: Int,
    caps: (Int, Int, Int),
    others: Seq<i64>,
    limit: Int,
) -> bool {
    let next = start + 1;
    proof_assert!(next <= caps.0 && next <= caps.1 && next <= caps.2
        && (forall<i: Int> 0 <= i && i < others.len() ==> next <= others[i]@));
    proof_assert!(next <= limit);
    true
}
