use creusot_std::prelude::*;

/// Select the earliest matching record across two readable tiers.
/// Preserve its timestamp, prefer remote on equal offsets, and return no
/// match exactly when neither tier found one. Tier lookup errors are handled
/// by the caller before this choice.
#[ensures((result == None) == (remote == None && local == None))]
#[ensures(match result {
    Some((offset, _)) => (result == remote || result == local)
        && (match remote { Some((r, _)) => offset@ <= r@, None => true })
        && (match local { Some((l, _)) => offset@ <= l@, None => true }),
    None => true,
})]
#[ensures(match (remote, local) {
    (Some((r, _)), Some((l, _))) => r@ <= l@ ==> result == remote,
    _ => true,
})]
#[must_use]
pub fn earliest_timestamp_candidate(
    remote: Option<(i64, i64)>,
    local: Option<(i64, i64)>,
) -> Option<(i64, i64)> {
    match (remote, local) {
        (Some(r), Some(l)) if l.0 < r.0 => Some(l),
        (Some(r), _) => Some(r),
        (None, l) => l,
    }
}
