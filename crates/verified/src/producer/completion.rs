use creusot_std::prelude::*;

#[cfg(creusot)]
mod coverage;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn completion_offset(ends: Seq<i64>, incoming: i64, index: Int) -> Int {
    pearlite! { if index == ends.len() { incoming@ } else { ends[index]@ } }
}

/// Name source membership so coverage laws have a term for each old origin.
// cargo-mutants: #[cfg(creusot)] logical predicate; not compiled outside Creusot.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[ensures(result == (exists<j: Int> 0 <= j && j < selected.len() && selected[j]@ == source))]
pub fn completion_source_selected(selected: Seq<usize>, source: Int) -> bool {
    pearlite! { exists<j: Int> 0 <= j && j < selected.len() && selected[j]@ == source }
}

/// Merge a deferred data completion into the current epoch's retry window.
/// The returned indices name old batches, or `ends.len()` for the incoming
/// batch. Equal physical offsets keep the existing metadata. Earlier
/// completions cannot displace a newer batch; only the five greatest distinct
/// offsets survive. A lower epoch preserves the old window and is rejected.
/// The host supplies sorted retained batches and a truthful incoming batch.
#[requires(ends@.len() <= 5)]
#[requires(current == None ==> ends@.len() == 0)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < ends@.len() ==> ends@[i]@ < ends@[j]@)]
#[ensures(result.0 == (match current { None => true, Some(value) => value@ <= epoch@ }))]
#[ensures(result.1@.len() <= 5 && (result.0 ==> result.1@.len() > 0))]
#[ensures(forall<j: Int> 0 <= j && j < result.1@.len() ==>
    result.1@[j]@ <= ends@.len()
    && (result.1@[j]@ < ends@.len() ==> current != None
        && match current { Some(value) => epoch@ <= value@, None => false })
    && (result.1@[j]@ == ends@.len() ==> result.0
        && !(match current { Some(value) => value == epoch
            && exists<i: Int> 0 <= i && i < ends@.len() && ends@[i] == incoming, None => false })))]
#[ensures(forall<i: Int, j: Int> 0 <= i && i < j && j < result.1@.len() ==>
    completion_offset(ends@, incoming, result.1@[i]@)
        < completion_offset(ends@, incoming, result.1@[j]@))]
#[ensures(forall<i: Int> 0 <= i && i < ends@.len()
    && (match current { Some(value) => epoch@ <= value@, None => false })
    && !completion_source_selected(result.1@, i) ==>
        result.1@.len() == 5 && (forall<j: Int> 0 <= j && j < result.1@.len() ==>
            ends@[i]@ < completion_offset(ends@, incoming, result.1@[j]@)))]
#[ensures(result.0 && !(exists<j: Int> 0 <= j && j < result.1@.len()
    && completion_offset(ends@, incoming, result.1@[j]@) == incoming@) ==>
        result.1@.len() == 5 && (forall<j: Int> 0 <= j && j < result.1@.len() ==>
            incoming@ < completion_offset(ends@, incoming, result.1@[j]@)))]
#[ensures(!result.0 ==> result.1@.len() == ends@.len()
    && (forall<j: Int> 0 <= j && j < ends@.len() ==> result.1@[j]@ == j))]
#[must_use]
pub fn producer_completion_window(
    current: Option<i16>,
    epoch: i16,
    ends: &[i64],
    incoming: i64,
) -> (bool, Vec<usize>) {
    let count = ends.len();
    let keep_old = match current {
        Some(value) => epoch <= value,
        None => false,
    };
    let accepted = match current {
        Some(value) => value <= epoch,
        None => true,
    };
    let mut position = 0usize;
    #[invariant(position@ <= count@)]
    #[invariant(forall<i: Int> 0 <= i && i < position@ ==> ends@[i]@ < incoming@)]
    #[variant(count@ - position@)]
    while position < count && ends[position] < incoming {
        position += 1;
    }
    proof_assert!(forall<i: Int> position@ <= i && i < count@ ==> incoming@ <= ends@[i]@);
    let insert = accepted && (!keep_old || position == count || ends[position] != incoming);
    let length = if keep_old { count } else { 0 } + usize::from(insert);
    let first = length.saturating_sub(5);
    let mut selected: Vec<usize> = Vec::with_capacity(length - first);
    let mut ordinal = first;
    #[invariant(first@ <= ordinal@ && ordinal@ <= length@ && selected@.len() == ordinal@ - first@)]
    #[invariant(forall<j: Int> 0 <= j && j < selected@.len() ==> selected@[j]@ ==
        if !keep_old || (insert && first@ + j == position@) { count@ }
        else if insert && first@ + j > position@ { first@ + j - 1 }
        else { first@ + j })]
    #[variant(length@ - ordinal@)]
    while ordinal < length {
        let source = if !keep_old || (insert && ordinal == position) {
            count
        } else if insert && ordinal > position {
            ordinal - 1
        } else {
            ordinal
        };
        selected.push(source);
        ordinal += 1;
    }
    if !keep_old {
        proof_assert!(selected@.len() == 1 && selected@[0]@ == count@);
        return (accepted, selected);
    }
    if !insert {
        proof_assert!(selected@.len() == ends@.len()
            && (forall<j: Int> 0 <= j && j < selected@.len() ==> selected@[j]@ == j));
        #[cfg(creusot)]
        proof_assert!(coverage::lemma_identity_window(selected@));
        // Expose source membership directly to the eviction postcondition.
        proof_assert!(forall<i: Int> 0 <= i && i < selected@.len()
            ==> completion_source_selected(selected@, i));
        proof_assert!(accepted ==>
            position@ < selected@.len() && selected@[position@]@ == position@ && ends@[position@] == incoming);
        return (accepted, selected);
    }
    proof_assert!(forall<i: Int> position@ <= i && i < count@ ==> incoming@ < ends@[i]@);
    proof_assert!(first@ == (if count@ == 5 { 1 } else { 0 })
        && selected@.len() == ends@.len() + 1 - first@);
    proof_assert!(forall<j: Int> 0 <= j && j < selected@.len() ==>
        selected@[j]@ == if first@ + j == position@ { ends@.len() }
        else if first@ + j > position@ { first@ + j - 1 } else { first@ + j });
    #[cfg(creusot)]
    proof_assert!(coverage::lemma_inserted_window(ends@, incoming, position@, first@, selected@));
    (accepted, selected)
}
