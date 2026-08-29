//! The per-decision safety invariants: what must hold of one `failover_one`
//! result given the state the controller saw before it.
//!
//! These checks run inside the transition rather than as stateright
//! properties, because they relate a decision to its own pre-state. A property
//! only sees the states, and the pre-state is gone by the time the successor
//! is checked.

use std::collections::HashSet;

use assert2::assert;
use krabka_raft::NodeId;

use super::failover_state::FailoverState;
use crate::{config_keys::RecoveryStrategy, leader_election::FailoverDecision};

/// Verify a `failover_one` decision against the pre-failover state. These are
/// the safety-critical invariants. They hold per-decision under any ordering.
pub(super) fn assert_decision(
    pre: &FailoverState,
    dead: NodeId,
    d: &FailoverDecision,
    unclean_enabled: bool,
    witnesses: &HashSet<NodeId>,
) {
    match d {
        FailoverDecision::Elect {
            leader,
            isr,
            unclean,
        } => {
            assert!(*leader != dead, "elected the dead broker {dead}");
            assert!(
                pre.alive.contains(leader),
                "elected leader {leader} not alive"
            );
            assert!(
                isr.contains(leader),
                "elected leader {leader} not in new ISR {isr:?}"
            );
            assert!(
                !witnesses.contains(leader),
                "elected witness {leader} as leader"
            );
            if *unclean {
                assert!(unclean_enabled, "unclean election without unclean_enabled");
            } else {
                // Clean election: the new leader was in the pre-failover ISR, so
                // it holds every committed record. No data loss.
                assert!(
                    pre.isr.contains(leader),
                    "clean election picked {leader} not in pre-failover ISR {:?} (data loss!)",
                    pre.isr
                );
                // A live witness is what keeps min-ISR satisfied after a site
                // loss, so a clean election must keep it in the ISR.
                assert!(
                    pre.isr
                        .iter()
                        .filter(|n| **n != dead && pre.alive.contains(n) && witnesses.contains(n))
                        .all(|n| isr.contains(n)),
                    "clean election dropped a live witness from ISR {isr:?}"
                );
            }
        }
        FailoverDecision::ShrinkIsr { isr } => {
            assert!(
                isr.iter().all(|n| pre.isr.contains(n)),
                "shrink introduced a non-member: {isr:?} vs {:?}",
                pre.isr
            );
            assert!(isr.len() < pre.isr.len(), "ShrinkIsr did not shrink");
        }
        FailoverDecision::Recover(s) => {
            assert!(*s != RecoveryStrategy::None, "Recover with strategy None");
            assert!(
                pre.leader == dead,
                "Recover when the dead broker was not leader"
            );
        }
        FailoverDecision::Unavailable | FailoverDecision::NoChange => {}
    }
}
