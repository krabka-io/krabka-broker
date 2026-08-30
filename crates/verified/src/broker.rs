//! Pure, safety-critical decision kernels used by `krabka-broker`.
//!
//! Keeping these small arithmetic decisions here lets Creusot prove the exact
//! executable bodies used by the asynchronous broker.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Visibility bounds and response watermarks for one Fetch partition.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FetchVisibility {
    pub out_of_range: bool,
    pub empty: bool,
    pub limit_offset: i64,
    pub effective_lso: i64,
    pub read_committed_aborts: bool,
    pub response_hw: i64,
    pub response_lso: i64,
}

/// The partition offsets one Fetch visibility decision reads.
///
/// They are one struct because they are five `i64` values with five different
/// meanings, and a transposed call site would compile.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FetchWatermarks {
    /// First offset the log still holds. Below it a fetch is out of range.
    pub log_start: i64,
    /// High watermark: the exclusive bound of what the ISR has replicated.
    pub hw: i64,
    /// Last stable offset: the first offset an open transaction may cover.
    pub lso: i64,
    /// Log end offset: the exclusive bound of what the leader holds.
    pub log_end: i64,
    /// KFC-1 delivery watermark: the first offset that is not due yet.
    pub deliverable: i64,
}

/// Compute Kafka's consumer/follower Fetch visibility window.
///
/// [`FetchWatermarks::deliverable`] is KFC-1's delivery watermark: the first
/// offset a consumer may not see yet, because the batch that starts there has
/// not reached its activation time. It caps a consumer, and it caps nothing
/// else:
///
/// - A follower is never gated. Replication carries a scheduled record to the
///   ISR, and it counts toward the high watermark, long before any consumer can
///   read it.
/// - `response_hw` and `response_lso` do not move. The broker reports the true
///   high watermark and last stable offset, so consumer lag stays honest and
///   KIP-227 watermark monotonicity is untouched.
///
/// The precondition bounds the delivery watermark inside `[log_start, hw]`,
/// which is what the caller clamps it to. The body still takes the minimum
/// against the bound it caps, so a caller that breaks the precondition gets a
/// narrower window and never a dirty read.
#[requires(0 <= w.log_start@ && w.log_start@ <= w.hw@ && w.hw@ <= w.log_end@)]
#[requires(w.log_start@ <= w.deliverable@ && w.deliverable@ <= w.hw@)]
#[ensures(result.out_of_range == (fetch_offset@ < w.log_start@))]
#[ensures(result.empty == (!(fetch_offset@ < w.log_start@)
    && fetch_offset@ >= if is_follower { w.log_end@ } else { w.deliverable@ }))]
#[ensures(result.effective_lso@ == if read_committed && !is_follower {
    if w.lso@ < w.hw@ { w.lso@ } else { w.hw@ }
} else { w.lso@ })]
#[ensures(result.read_committed_aborts == (read_committed && !is_follower))]
#[ensures(result.response_hw@ == if is_follower { w.log_end@ } else { w.hw@ })]
#[ensures(result.response_lso@ == if read_committed && !is_follower {
    if w.lso@ < w.hw@ { w.lso@ } else { w.hw@ }
} else if is_follower { w.log_end@ } else { w.hw@ })]
#[ensures(result.limit_offset@ == if is_follower { w.log_end@ } else if read_committed {
    if w.lso@ < w.deliverable@ { w.lso@ } else { w.deliverable@ }
} else { w.deliverable@ })]
#[ensures(is_follower ==> result.limit_offset@ == w.log_end@)]
#[ensures(!is_follower ==> result.limit_offset@ <= w.deliverable@)]
#[must_use]
pub fn fetch_visibility(
    is_follower: bool,
    read_committed: bool,
    w: FetchWatermarks,
    fetch_offset: i64,
) -> FetchVisibility {
    // The delivery watermark caps a consumer and never a follower.
    let visible = if w.deliverable < w.hw {
        w.deliverable
    } else {
        w.hw
    };
    let upper_bound = if is_follower { w.log_end } else { visible };
    let effective_lso = if read_committed && !is_follower {
        if w.lso < w.hw { w.lso } else { w.hw }
    } else {
        w.lso
    };
    let response_hw = if is_follower { w.log_end } else { w.hw };
    let response_lso = if read_committed && !is_follower {
        effective_lso
    } else if is_follower {
        w.log_end
    } else {
        w.hw
    };
    let limit_offset = if is_follower {
        w.log_end
    } else if read_committed {
        if effective_lso < visible {
            effective_lso
        } else {
            visible
        }
    } else {
        visible
    };
    let out_of_range = fetch_offset < w.log_start;
    FetchVisibility {
        out_of_range,
        empty: !out_of_range && fetch_offset >= upper_bound,
        limit_offset,
        effective_lso,
        read_committed_aborts: read_committed && !is_follower,
        response_hw,
        response_lso,
    }
}

/// Resolve `DeleteRecords`' `-1` sentinel to the current high watermark.
#[ensures(result@ == if requested_offset@ == -1 { high_watermark@ } else { requested_offset@ })]
#[must_use]
pub const fn delete_records_target(requested_offset: i64, high_watermark: i64) -> i64 {
    if requested_offset == -1 {
        high_watermark
    } else {
        requested_offset
    }
}

/// Whether a resolved `DeleteRecords` target is outside the local log.
#[ensures(result == (target@ < 0 || target@ > log_end_offset@))]
#[must_use]
pub const fn delete_records_offset_out_of_range(target: i64, log_end_offset: i64) -> bool {
    target < 0 || target > log_end_offset
}

/// Non-negative KIP-932 backlog above the effective share start offset.
#[cfg(creusot)]
#[logic]
#[cfg_attr(test, mutants::skip)]
pub fn effective_share_backlog_model(hwm: i64, spso: i64, log_start: i64) -> Int {
    pearlite! {
        let base = if spso@ >= 0 && spso@ > log_start@ { spso@ } else { log_start@ };
        let difference = hwm@ - base;
        if difference <= 0 {
            0
        } else if difference > 9223372036854775807 {
            9223372036854775807
        } else {
            difference
        }
    }
}

#[ensures(result@ == effective_share_backlog_model(hwm, spso, log_start))]
#[must_use]
pub fn effective_share_backlog(hwm: i64, spso: i64, log_start: i64) -> i64 {
    let base = if spso >= 0 && spso > log_start {
        spso
    } else {
        log_start
    };
    let difference = hwm.saturating_sub(base);
    if difference > 0 { difference } else { 0 }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn fetch_visibility_covers_consumer_and_follower_bounds() {
        // Nothing is held back: the delivery watermark sits at the high
        // watermark, so every bound is the one Kafka computes today.
        assert2::assert!(
            fetch_visibility(
                false,
                true,
                FetchWatermarks {
                    log_start: 2,
                    hw: 8,
                    lso: 6,
                    log_end: 10,
                    deliverable: 8,
                },
                3
            ) == FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 6,
                effective_lso: 6,
                read_committed_aborts: true,
                response_hw: 8,
                response_lso: 6,
            }
        );

        assert2::assert!(
            fetch_visibility(
                false,
                true,
                FetchWatermarks {
                    log_start: 2,
                    hw: 8,
                    lso: 9,
                    log_end: 10,
                    deliverable: 8,
                },
                3
            ) == FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 8,
                effective_lso: 8,
                read_committed_aborts: true,
                response_hw: 8,
                response_lso: 8,
            }
        );

        // A follower reads to the log end even where the whole log is waiting
        // to be delivered.
        assert2::assert!(
            fetch_visibility(
                true,
                false,
                FetchWatermarks {
                    log_start: 2,
                    hw: 8,
                    lso: 6,
                    log_end: 10,
                    deliverable: 2,
                },
                10
            ) == FetchVisibility {
                out_of_range: false,
                empty: true,
                limit_offset: 10,
                effective_lso: 6,
                read_committed_aborts: false,
                response_hw: 10,
                response_lso: 10,
            }
        );
    }

    #[test]
    fn fetch_visibility_caps_a_consumer_at_the_delivery_watermark() {
        // read_uncommitted: the cap is the delivery watermark, not the high
        // watermark, and the reported watermarks do not move with it.
        assert2::assert!(
            fetch_visibility(
                false,
                false,
                FetchWatermarks {
                    log_start: 0,
                    hw: 8,
                    lso: 8,
                    log_end: 10,
                    deliverable: 5,
                },
                3
            ) == FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 5,
                effective_lso: 8,
                read_committed_aborts: false,
                response_hw: 8,
                response_lso: 8,
            }
        );

        // A consumer parked exactly at the watermark reads nothing, which is
        // what parks it in a long poll until the batch there comes due.
        assert2::assert!(
            fetch_visibility(
                false,
                false,
                FetchWatermarks {
                    log_start: 0,
                    hw: 8,
                    lso: 8,
                    log_end: 10,
                    deliverable: 5,
                },
                5
            ) == FetchVisibility {
                out_of_range: false,
                empty: true,
                limit_offset: 5,
                effective_lso: 8,
                read_committed_aborts: false,
                response_hw: 8,
                response_lso: 8,
            }
        );

        // read_committed takes the lowest of the three. The abort-scan ceiling
        // stays `lso.min(hw)`, because a wider scan only lists aborts the
        // consumer already knows how to drop.
        assert2::assert!(
            fetch_visibility(
                false,
                true,
                FetchWatermarks {
                    log_start: 0,
                    hw: 8,
                    lso: 6,
                    log_end: 10,
                    deliverable: 4,
                },
                0
            ) == FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 4,
                effective_lso: 6,
                read_committed_aborts: true,
                response_hw: 8,
                response_lso: 6,
            }
        );
        // The last stable offset still wins where it is the lowest of the three.
        assert2::assert!(
            fetch_visibility(
                false,
                true,
                FetchWatermarks {
                    log_start: 0,
                    hw: 8,
                    lso: 3,
                    log_end: 10,
                    deliverable: 4,
                },
                0
            )
            .limit_offset
                == 3
        );
    }

    #[test]
    fn broker_arithmetic_edges_are_explicit() {
        assert!(delete_records_target(-1, 7) == 7);
        assert!(delete_records_offset_out_of_range(-1, 7));
        assert!(effective_share_backlog(12, -1, 4) == 8);
        assert!(effective_share_backlog(5, 9, 4) == 0);
        assert!(effective_share_backlog(i64::MAX, i64::MIN, i64::MIN) == i64::MAX);
    }

    #[test]
    fn fetch_visibility_matches_the_complete_decision_table() {
        for is_follower in [false, true] {
            for read_committed in [false, true] {
                for log_start in [0, 2] {
                    for hw in [2, 5] {
                        for lso in [1, 4, 7] {
                            for log_end in [5, 9] {
                                // The table walks `deliverable` past both ends
                                // of the proved domain `[log_start, hw]` as
                                // well as through it, because the precondition
                                // is a proof obligation on the caller and does
                                // not run. The oracle takes the same minimum
                                // the body does.
                                for deliverable in [0, 2, 3, 5, 9] {
                                    for fetch_offset in [0, 2, 4, 5, 10] {
                                        let got = fetch_visibility(
                                            is_follower,
                                            read_committed,
                                            FetchWatermarks {
                                                log_start,
                                                hw,
                                                lso,
                                                log_end,
                                                deliverable,
                                            },
                                            fetch_offset,
                                        );
                                        let visible = hw.min(deliverable);
                                        let upper = if is_follower { log_end } else { visible };
                                        let effective_lso = if read_committed && !is_follower {
                                            lso.min(hw)
                                        } else {
                                            lso
                                        };
                                        let response_lso = if is_follower {
                                            log_end
                                        } else if read_committed {
                                            lso.min(hw)
                                        } else {
                                            hw
                                        };
                                        let limit = if is_follower {
                                            log_end
                                        } else if read_committed {
                                            effective_lso.min(visible)
                                        } else {
                                            visible
                                        };
                                        let out_of_range = fetch_offset < log_start;

                                        assert2::assert!(
                                            got == FetchVisibility {
                                                out_of_range,
                                                empty: !out_of_range && fetch_offset >= upper,
                                                limit_offset: limit,
                                                effective_lso,
                                                read_committed_aborts: read_committed
                                                    && !is_follower,
                                                response_hw: if is_follower { log_end } else { hw },
                                                response_lso,
                                            }
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn broker_arithmetic_matches_wide_integer_oracles() {
        let values = [i64::MIN, -2, -1, 0, 1, 2, i64::MAX];
        for requested in values {
            for high_watermark in values {
                assert!(
                    delete_records_target(requested, high_watermark)
                        == if requested == -1 {
                            high_watermark
                        } else {
                            requested
                        }
                );
                assert!(
                    delete_records_offset_out_of_range(requested, high_watermark)
                        == (requested < 0 || requested > high_watermark)
                );
            }
        }

        for hwm in values {
            for spso in values {
                for log_start in values {
                    let base = if spso >= 0 {
                        spso.max(log_start)
                    } else {
                        log_start
                    };
                    let expected = i64::try_from(
                        (i128::from(hwm) - i128::from(base)).clamp(0, i128::from(i64::MAX)),
                    )
                    .expect("oracle is clamped to the i64 range");
                    assert!(effective_share_backlog(hwm, spso, log_start) == expected);
                }
            }
        }
    }
}
