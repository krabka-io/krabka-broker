//! Kafka Streams topology-description cleanup and store-epoch decisions.
//!
//! The host must serialize membership and offset observations and persist the
//! UNCERTAIN barrier before invoking storage. These kernels do not prove IO.

#[cfg(creusot)]
use creusot_std::prelude::ensures;

/// Cleanup cannot run under live members or retained/pending offsets.
#[must_use]
#[cfg_attr(creusot, ensures(result == (!has_members && all_offsets_expired && initialized)))]
pub const fn may_reclaim(has_members: bool, all_offsets_expired: bool, initialized: bool) -> bool {
    !has_members && all_offsets_expired && initialized
}

/// Resolve a store only after the durable UNCERTAIN write.
///
/// Success makes this
/// epoch reliable; all failures retain uncertainty, and only permanent failure
/// suppresses future solicitation of this epoch.
#[must_use]
#[cfg_attr(creusot, ensures(result.0@ == if succeeded { epoch@ } else { -2 }))]
#[cfg_attr(creusot, ensures(result.1@ == if !succeeded && permanent { epoch@ } else { previous_failed@ }))]
pub const fn finish_store(
    epoch: i32,
    previous_failed: i32,
    succeeded: bool,
    permanent: bool,
) -> (i32, i32) {
    (
        if succeeded { epoch } else { -2 },
        if !succeeded && permanent {
            epoch
        } else {
            previous_failed
        },
    )
}

#[cfg(test)]
mod tests {
    use super::{finish_store, may_reclaim};
    #[test]
    fn cleanup_requires_every_eligibility_condition() {
        for members in [false, true] {
            for expired in [false, true] {
                for initialized in [false, true] {
                    assert2::assert!(
                        may_reclaim(members, expired, initialized)
                            == (!members && expired && initialized)
                    );
                }
            }
        }
    }
    #[test]
    fn store_outcomes_preserve_uncertainty_and_failed_epoch() {
        assert2::assert!(finish_store(7, 3, true, false) == (7, 3));
        assert2::assert!(finish_store(7, 3, false, false) == (-2, 3));
        assert2::assert!(finish_store(7, 3, false, true) == (-2, 7));
    }
}
