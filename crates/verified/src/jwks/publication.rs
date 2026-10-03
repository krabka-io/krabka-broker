//! Publication generations for the host's exclusive JWKS writer.

use creusot_std::prelude::ensures;

/// Enter an odd write phase and commit the next even generation, or preserve
/// the prior cache. Never wrap: an old reader generation cannot recur.
#[ensures((result == None) == (generation@ % 2 != 0 || generation@ > u64::MAX@ - 2))]
#[ensures(match result {
    None => true,
    Some((writing, committed)) => writing@ == generation@ + 1
        && committed@ == generation@ + 2
        && generation@ % 2 == 0 && writing@ % 2 == 1 && committed@ % 2 == 0,
})]
#[must_use]
pub fn jwks_publication_generations(generation: u64) -> Option<(u64, u64)> {
    if !generation.is_multiple_of(2) || generation > u64::MAX - 2 {
        None
    } else {
        Some((generation + 1, generation + 2))
    }
}
