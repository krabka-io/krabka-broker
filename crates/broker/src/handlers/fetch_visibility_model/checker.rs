use super::*;

impl Model for VisModel {
    type State = VisState;
    type Action = VisAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![VisState {
            log_start: 0,
            hw: 0,
            lso: 0,
            deliverable: 0,
            log_end: 0,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        // Advance watermarks, preserving 0 <= log_start <= lso <= hw <= log_end
        // <= max_offset, and log_start <= deliverable <= hw.
        if s.log_end < self.max_offset {
            actions.push(VisAction::AdvanceLogEnd);
        }
        if s.hw < s.log_end {
            actions.push(VisAction::AdvanceHw);
        }
        if s.lso < s.hw {
            actions.push(VisAction::AdvanceLso);
        }
        if s.log_start < s.lso {
            actions.push(VisAction::AdvanceLogStart);
        }
        // The delivery watermark trails the HW independently of the LSO: a
        // batch comes due because time passed, not because a txn committed.
        if s.deliverable < s.hw {
            actions.push(VisAction::AdvanceDeliverable);
        }
        // Probe every fetch shape over a bounded fetch-offset window (incl. one
        // past log_end so the empty/out-of-range edges are exercised).
        for fo in 0..=(self.max_offset + 1) {
            actions.push(VisAction::Fetch(false, false, fo)); // consumer, read_uncommitted
            actions.push(VisAction::Fetch(false, true, fo)); // consumer, read_committed
            actions.push(VisAction::Fetch(true, false, fo)); // follower
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        match action {
            VisAction::AdvanceLogEnd => {
                let mut s = last.clone();
                s.log_end += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::AdvanceHw => {
                let mut s = last.clone();
                s.hw += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::AdvanceLso => {
                let mut s = last.clone();
                s.lso += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::AdvanceLogStart => {
                // log_start advancing never lowers response_hw/lso.
                let mut s = last.clone();
                s.log_start += 1;
                // `plan_read` clamps the delivery watermark into the range the
                // log still holds, so retention carries it along rather than
                // leaving it below the first offset that exists.
                s.deliverable = s.deliverable.max(s.log_start);
                Some(s)
            }
            VisAction::AdvanceDeliverable => {
                // A batch coming due widens what a consumer may read and moves
                // no reported watermark, so KIP-227 holds here too.
                let mut s = last.clone();
                s.deliverable += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::Fetch(is_follower, read_committed, fetch_offset) => {
                let w = compute_visibility_window(
                    is_follower,
                    read_committed,
                    FetchWatermarks {
                        log_start: Offset(last.log_start),
                        hw: Offset(last.hw),
                        lso: Offset(last.lso),
                        log_end: Offset(last.log_end),
                        deliverable: Offset(last.deliverable),
                    },
                    Offset(fetch_offset),
                );
                assert_fetch_contract(last, is_follower, read_committed, fetch_offset, &w);
                None // probes never change state
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("watermarks_ordered", |_, s: &VisState| {
                0 <= s.log_start
                    && s.log_start <= s.lso
                    && s.lso <= s.hw
                    && s.hw <= s.log_end
                    && s.log_start <= s.deliverable
                    && s.deliverable <= s.hw
            }),
            // A read_committed clamp strictly below HW is reachable (lso < hw).
            Property::sometimes("can_clamp_lso", |_, s: &VisState| s.lso < s.hw),
            // A delivery clamp strictly below HW is reachable, and it is the
            // one that binds: a scheduled record is committed but not due.
            Property::sometimes("can_clamp_deliverable", |_, s: &VisState| {
                s.deliverable < s.hw && s.deliverable < s.lso
            }),
            // The delivery watermark can also be the loosest of the three, so
            // the LSO clamp is still reachable under it.
            Property::sometimes("deliverable_above_lso", |_, s: &VisState| {
                s.lso < s.deliverable
            }),
            // A follower can be served strictly beyond HW (hw < log_end).
            Property::sometimes("follower_beyond_hw", |_, s: &VisState| s.hw < s.log_end),
            // OFFSET_OUT_OF_RANGE is reachable (log_start > 0 ⟹ a sub-log_start
            // fetch_offset exists).
            Property::sometimes("can_out_of_range", |_, s: &VisState| s.log_start > 0),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.log_end <= self.max_offset
    }
}
