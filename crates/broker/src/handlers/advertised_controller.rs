//! The `controller_id` that a broker listener advertises in `Metadata` and
//! `DescribeCluster`.
//!
//! A client reads `controller_id` out of one of those responses and then
//! resolves it against the broker list in that *same* response to get a
//! `host:port`. The JVM `AdminClient` does exactly that before it sends a
//! controller-routed call. So the id has to name a row the response already
//! carries.
//!
//! A `KRaft` metadata quorum breaks that if the raft leader is named directly.
//! In a role-separated cluster the controllers are not brokers, never register
//! a broker endpoint, and so never appear in a `Metadata` broker list. Handing
//! the client the leader's node id gives it an id it cannot resolve, and every
//! controller-routed admin call fails before it leaves the client.
//!
//! Apache Kafka answers this by not naming the controller at all. `KafkaApis`
//! answers both APIs from `MetadataCache.getRandomAliveBrokerId`, so a `KRaft`
//! broker advertises one of the live brokers instead, and `-1` when it knows
//! of none. Brokers forward controller-only requests to the quorum, so any of
//! them is a correct destination.
//!
//! Measured against the pinned image. A role-separated `apache/kafka:4.3.1`
//! cluster -- controller-only node 1, brokers 2 and 3, raft leader 1 -- was
//! driven with raw `Metadata` v1 and `DescribeCluster` v0 requests against
//! broker 2. Both broker lists held only nodes 2 and 3, and twelve calls to
//! each API returned `controller_id` 2 or 3, in no fixed order, and never 1.
//! `4.3.1` is the tag `MODULE.bazel` pins for the JVM suites.
//!
//! [`ControllerIdRotation`] makes the same choice and rotates over the
//! eligible brokers rather than drawing at random. The client-visible contract
//! is the one Kafka offers -- an id that resolves to a broker row in the same
//! response -- and rotating spreads forwarded admin traffic over the fleet the
//! way the random draw intends, with no random source and no per-response
//! variance for a test to work around.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Kafka's sentinel for "this response names no controller". A client that
/// reads it retries later instead of resolving an endpoint.
pub(crate) const NO_CONTROLLER_ID: i32 = -1;

/// Rotates the advertised `controller_id` over the brokers a response lists.
///
/// One instance lives on [`crate::broker::Broker`], shared by `Metadata` and
/// `DescribeCluster`, so successive requests to the same node spread over the
/// fleet rather than pinning every client to one broker.
#[derive(Debug, Default)]
pub(crate) struct ControllerIdRotation {
    /// Free-running request counter. Only its value modulo the candidate
    /// count matters, so wrapping is not a defect.
    next: AtomicUsize,
}

impl ControllerIdRotation {
    /// Returns the id to advertise as `controller_id`.
    ///
    /// `eligible` is the node ids of the broker rows this response carries,
    /// minus any the node knows to be fenced or dead. Every returned id is
    /// therefore one of the response's own rows, and resolves to that row's
    /// host and port.
    ///
    /// Returns [`NO_CONTROLLER_ID`] for an empty `eligible`, which is what
    /// Kafka returns when its metadata cache holds no live broker.
    pub(crate) fn pick(&self, eligible: &[i32]) -> i32 {
        if eligible.is_empty() {
            return NO_CONTROLLER_ID;
        }
        let turn = self.next.fetch_add(1, Ordering::Relaxed);
        eligible[turn % eligible.len()]
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// No broker row to name means no controller to advertise.
    #[test]
    fn empty_candidate_set_advertises_the_no_controller_sentinel() {
        let rotation = ControllerIdRotation::default();
        assert!(rotation.pick(&[]) == NO_CONTROLLER_ID);
        assert!(rotation.pick(&[]) == NO_CONTROLLER_ID);
    }

    /// A single-broker cluster names that broker on every request.
    #[test]
    fn single_candidate_is_advertised_every_time() {
        let rotation = ControllerIdRotation::default();
        let picks: Vec<i32> = (0..4).map(|_| rotation.pick(&[7])).collect();
        assert!(picks == vec![7, 7, 7, 7]);
    }

    /// Successive requests walk the candidate list and wrap, so forwarded
    /// admin traffic spreads over every broker instead of pinning one.
    #[test]
    fn successive_picks_rotate_over_every_candidate() {
        let rotation = ControllerIdRotation::default();
        let picks: Vec<i32> = (0..7).map(|_| rotation.pick(&[2, 3, 5])).collect();
        assert!(picks == vec![2, 3, 5, 2, 3, 5, 2]);
    }

    /// The rotation is per-broker state: two nodes do not share a turn.
    #[test]
    fn rotations_are_independent() {
        let first = ControllerIdRotation::default();
        let second = ControllerIdRotation::default();
        let _ = first.pick(&[1, 2]);
        assert!((first.pick(&[1, 2]), second.pick(&[1, 2])) == (2, 1));
    }

    /// A candidate list that shrinks between requests still yields a member
    /// of the list it was given, never an index out of that list.
    #[test]
    fn picks_stay_inside_a_shrinking_candidate_set() {
        let rotation = ControllerIdRotation::default();
        for _ in 0..5 {
            let _ = rotation.pick(&[4, 6, 8, 9]);
        }
        let picks: Vec<i32> = (0..3).map(|_| rotation.pick(&[4])).collect();
        assert!(picks == vec![4, 4, 4]);
    }
}
