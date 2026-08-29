//! The classic `JoinGroup` transition.
//!
//! `handle_join` runs the KIP-394 member-id bootstrap, the KIP-345
//! static-instance fences, and the rebalance-window bookkeeping, then returns a
//! [`JoinAction`] that tells the actor whether to reply at once, park the reply,
//! or complete the round immediately. `build_join_result` renders the reply from
//! post-rebalance state, and `try_complete` runs the protocol-selection vote
//! that closes a round.

use std::time::{Duration, Instant};

use bytes::Bytes;
use krabka_protocol::owned::join_group_request::JoinGroupRequest;
use uuid::Uuid;

use crate::{
    codes,
    coordinator::unified::{
        actor::{JoinResult, JoinResultMember},
        classic_state::{
            AddMemberOutcome, ClassicGroup as ClassicState, GroupState, Member, select_protocol,
        },
    },
};

const DEFAULT_SESSION_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_REBALANCE_TIMEOUT_MS: u64 = 60_000;

/// What the actor should do with a `ClassicJoin`.
pub(crate) enum JoinAction {
    /// Reply right away. The fast paths are `MEMBER_ID_REQUIRED`, validation
    /// errors, and a static rejoin into a `Stable` group.
    Immediate(JoinResult),
    /// Park the reply. `state.rebalance_deadline` is set, and the actor
    /// completes the rebalance when that deadline fires.
    Park,
    /// Every still-live member has joined this round, and a membership change
    /// in a group that still had members triggered the round. This is not a
    /// start-from-`Empty` herd. Complete the rebalance now and drain
    /// all parked joiners. This mirrors the old `wake_other_joiners` path.
    CompleteNow,
}

/// Port of `handlers/join_group.rs` steps 1–6. It operates on `ClassicState`.
pub(crate) fn handle_join(
    state: &mut ClassicState,
    req: &mut JoinGroupRequest,
    client_id: &str,
    client_host: &str,
    require_known_member_id: bool,
    initial_rebalance_delay: Duration,
) -> JoinAction {
    // 1. Empty member_id on first join → broker generates one (KIP-394).
    //    KIP-345: derive the bootstrap id from the instance id, and return the
    //    existing slot's member_id if the instance is already pinned.
    if req.member_id.is_empty() {
        let member_id = if let Some(instance_id) = req.group_instance_id.as_deref() {
            match state.current_member_id_for_instance(instance_id) {
                Some(mid) => mid.to_string(),
                None => format!("{instance_id}-{}", Uuid::new_v4()),
            }
        } else {
            format!("krabka-{}", Uuid::new_v4())
        };
        if require_known_member_id {
            return JoinAction::Immediate(JoinResult {
                error_code: codes::MEMBER_ID_REQUIRED,
                member_id,
                ..JoinResult::default()
            });
        }
        req.member_id = member_id;
    }
    let req = &*req;

    // 2. protocol_type mismatch on an existing group → INCONSISTENT (KIP-559 echo).
    if let Some(existing_type) = state.protocol_type.as_deref()
        && existing_type != req.protocol_type
    {
        return JoinAction::Immediate(JoinResult {
            error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
            member_id: req.member_id.clone(),
            protocol_type: state.protocol_type.clone(),
            protocol_name: state.protocol_name.clone(),
            ..JoinResult::default()
        });
    }

    // 2b. KIP-345 fence: a known member_id must keep a consistent
    // group.instance.id. A join that reuses an existing member's id with a
    // different instance nature — e.g. a dynamic rejoin of a static member, or a
    // switch between instances — would otherwise overwrite the member in
    // `add_member` and orphan its `static_members` index entry, permanently
    // pinning the old instance id (a non-conforming client could lock a slot
    // forever). Apache Kafka validates the registered instance against the
    // request and fences the mismatch.
    if let Some(existing) = state.members.get(&req.member_id)
        && existing.group_instance_id.as_deref() != req.group_instance_id.as_deref()
    {
        return JoinAction::Immediate(JoinResult {
            error_code: codes::FENCED_INSTANCE_ID,
            member_id: req.member_id.clone(),
            protocol_type: state.protocol_type.clone(),
            protocol_name: state.protocol_name.clone(),
            ..JoinResult::default()
        });
    }

    // 3. KIP-345 fence: instance id pinned to a different live member id.
    if let Some(instance_id) = req.group_instance_id.as_deref()
        && let Some(pinned) = state.current_member_id_for_instance(instance_id)
        && pinned != req.member_id
    {
        return JoinAction::Immediate(JoinResult {
            error_code: codes::FENCED_INSTANCE_ID,
            member_id: req.member_id.clone(),
            protocol_type: state.protocol_type.clone(),
            protocol_name: state.protocol_name.clone(),
            ..JoinResult::default()
        });
    }

    // 4. Add member.
    let protocols: Vec<(String, Bytes)> = req
        .protocols
        .iter()
        .map(|p| (p.name.clone(), p.metadata.clone()))
        .collect();
    let session_timeout = Duration::from_millis(
        u64::try_from(req.session_timeout_ms).unwrap_or(DEFAULT_SESSION_TIMEOUT_MS),
    );
    let rebalance_timeout = Duration::from_millis(
        u64::try_from(req.rebalance_timeout_ms).unwrap_or(DEFAULT_REBALANCE_TIMEOUT_MS),
    );
    state.protocol_type = Some(req.protocol_type.clone());
    let pre_state = state.state;
    let outcome = state.add_member(
        Member::new(
            req.member_id.clone(),
            client_id.to_string(),
            client_host.to_string(),
            session_timeout,
            rebalance_timeout,
            protocols,
        )
        .with_instance_id(req.group_instance_id.clone()),
    );
    let static_rejoin_to_stable = matches!(outcome, AddMemberOutcome::StaticRejoin { .. })
        && matches!(pre_state, GroupState::Stable);
    // Open the rebalance window, anchored at the first join. A new group uses
    // the configured batching delay. An existing group gets the member's full
    // rebalance timeout so its current members have time to observe
    // REBALANCE_IN_PROGRESS and rejoin; they still complete the round early as
    // soon as all are present.
    if !static_rejoin_to_stable && state.rebalance_deadline.is_none() {
        let delay = if matches!(pre_state, GroupState::Empty) {
            rebalance_timeout.min(initial_rebalance_delay)
        } else {
            rebalance_timeout
        };
        state.rebalance_deadline = Some(Instant::now() + delay);
    }

    // 5. Static rejoin into a `Stable` group: skip the rebalance entirely.
    if static_rejoin_to_stable {
        return JoinAction::Immediate(build_join_result(state, &req.member_id));
    }

    // 6. Early-complete once every still-live member has rejoined this round —
    //    but only for a rebalance triggered by a membership change in a group
    //    that still had members. A round that opened from `Empty` (a fresh
    //    group, or one whose members all left — e.g. after a warm-up consumer
    //    joins and leaves) burns the full initial delay so a herd of
    //    consumers starting together batches into one generation, mirroring
    //    Kafka's `InitialDelayedJoin`. Eager-completing a from-`Empty` round
    //    strands the first joiner in a solo generation and forces an immediate
    //    re-rebalance when the next member arrives, which thrashes
    //    produce+fetch under concurrent load.
    let complete_now = !state.rebalance_from_empty
        && matches!(state.state, GroupState::PreparingRebalance)
        && state.all_members_joined_this_round();
    if complete_now {
        JoinAction::CompleteNow
    } else {
        JoinAction::Park
    }
}

/// Build a successful `JoinResult` from post-rebalance state. The leader gets
/// the member list, and followers get an empty list.
pub(crate) fn build_join_result(state: &ClassicState, member_id: &str) -> JoinResult {
    let is_leader = state.leader_id.as_deref() == Some(member_id);
    let members = if is_leader {
        state
            .members
            .values()
            .map(|m| JoinResultMember {
                member_id: m.id.clone(),
                group_instance_id: m.group_instance_id.clone(),
                metadata: m.protocol_metadata.clone(),
            })
            .collect()
    } else {
        Vec::new()
    };
    JoinResult {
        error_code: codes::NONE,
        generation_id: state.generation_id,
        protocol_type: state.protocol_type.clone(),
        protocol_name: state.protocol_name.clone(),
        leader: state.leader_id.clone().unwrap_or_default(),
        member_id: member_id.to_string(),
        members,
    }
}

/// Run the rebalance-completion vote. It returns `Ok(())` if the round
/// completed, or if there was nothing to complete. It returns `Err(())` if the
/// protocol intersection was empty, which is `INCONSISTENT_GROUP_PROTOCOL`.
/// Mirrors `join_group.rs` block 5.
pub(crate) fn try_complete(state: &mut ClassicState) -> Result<(), ()> {
    if matches!(state.state, GroupState::PreparingRebalance) && !state.members.is_empty() {
        if let Some(chosen) = select_protocol(&state.members) {
            state.resolve_selected_protocol_metadata(&chosen);
            state.complete_rebalance(chosen);
            Ok(())
        } else {
            Err(())
        }
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol;

    use super::*;
    use crate::coordinator::unified::classic_ops::test_support::{handle_join, join_req};

    #[test]
    fn join_empty_member_id_dynamic_returns_member_id_required() {
        let mut g = ClassicState::new("g");
        let action = handle_join(&mut g, &join_req("", None), "h");
        match action {
            JoinAction::Immediate(r) => {
                assert!(r.error_code == codes::MEMBER_ID_REQUIRED);
                assert!(r.member_id.starts_with("krabka-"));
            }
            _ => panic!("expected Immediate MEMBER_ID_REQUIRED"),
        }
    }

    #[test]
    fn join_empty_member_id_static_derives_from_instance() {
        let mut g = ClassicState::new("g");
        let action = handle_join(&mut g, &join_req("", Some("inst-a")), "h");
        match action {
            JoinAction::Immediate(r) => {
                assert!(r.error_code == codes::MEMBER_ID_REQUIRED);
                assert!(r.member_id.starts_with("inst-a-"));
            }
            _ => panic!("expected Immediate MEMBER_ID_REQUIRED"),
        }
    }

    #[test]
    fn legacy_join_with_empty_member_id_adds_generated_member() {
        let mut g = ClassicState::new("g");
        let mut request = join_req("", None);
        let action = super::handle_join(
            &mut g,
            &mut request,
            "client-a",
            "h",
            false,
            Duration::from_secs(3),
        );
        assert!(matches!(action, JoinAction::Park));
        assert!(request.member_id.starts_with("krabka-"));
        assert!(
            g.members
                .keys()
                .any(|member_id| member_id.starts_with("krabka-"))
        );
    }

    #[test]
    fn join_protocol_type_mismatch_is_inconsistent() {
        let mut g = ClassicState::new("g");
        g.protocol_type = Some("connect".into());
        let action = handle_join(&mut g, &join_req("m1", None), "h");
        match action {
            JoinAction::Immediate(r) => assert!(r.error_code == codes::INCONSISTENT_GROUP_PROTOCOL),
            _ => panic!("expected Immediate INCONSISTENT_GROUP_PROTOCOL"),
        }
    }

    #[test]
    fn join_fenced_instance_id() {
        let mut g = ClassicState::new("g");
        // Pin inst-a to m1 via a first join.
        let _ = handle_join(&mut g, &join_req("m1", Some("inst-a")), "h");
        // A different member id claiming the same instance is fenced.
        let action = handle_join(&mut g, &join_req("m2", Some("inst-a")), "h");
        match action {
            JoinAction::Immediate(r) => assert!(r.error_code == codes::FENCED_INSTANCE_ID),
            _ => panic!("expected Immediate FENCED_INSTANCE_ID"),
        }
    }

    #[test]
    fn join_new_member_parks_and_opens_deadline() {
        let mut g = ClassicState::new("g");
        let action = handle_join(&mut g, &join_req("m1", None), "h");
        assert!(matches!(action, JoinAction::Park));
        check!(g.members["m1"].client_id == "client-a");
        check!(g.members["m1"].host == "h");
        check!(g.rebalance_deadline.is_some());
        check!(g.state == GroupState::PreparingRebalance);
    }

    #[test]
    fn join_uses_configured_initial_rebalance_delay() {
        let mut g = ClassicState::new("g");
        let before = Instant::now();
        let mut request = join_req("m1", None);
        let action = super::handle_join(
            &mut g,
            &mut request,
            "client-a",
            "h",
            true,
            Duration::from_millis(17),
        );
        let after = Instant::now();

        assert!(matches!(action, JoinAction::Park));
        let deadline = g.rebalance_deadline.expect("rebalance deadline");
        assert!(deadline >= before + Duration::from_millis(17));
        assert!(deadline <= after + Duration::from_millis(17));
    }

    #[test]
    fn late_join_uses_rebalance_timeout_for_existing_members_to_rejoin() {
        let mut g = ClassicState::new("g");
        let _ = handle_join(&mut g, &join_req("m1", None), "h");
        try_complete(&mut g).unwrap();
        g.state = GroupState::Stable;

        let before = Instant::now();
        let mut request = join_req("m2", None);
        request.rebalance_timeout_ms = 60_000;
        let action = super::handle_join(
            &mut g,
            &mut request,
            "client-a",
            "h",
            true,
            Duration::from_secs(3),
        );

        assert!(matches!(action, JoinAction::Park));
        assert!(g.rebalance_deadline.unwrap() >= before + Duration::from_mins(1));
    }

    #[test]
    fn join_static_rejoin_to_stable_is_immediate_success() {
        let mut g = ClassicState::new("g");
        let _ = handle_join(&mut g, &join_req("m1", Some("inst-a")), "h");
        g.complete_rebalance("range");
        let mut a = std::collections::HashMap::new();
        a.insert("m1".to_string(), Bytes::from_static(b"asn"));
        g.install_assignments(a);
        assert!(g.state == GroupState::Stable);
        // Rejoin the same instance with the pinned member_id (KIP-345: a
        // restarted static client re-supplies the id it got back from the
        // MEMBER_ID_REQUIRED round-trip) → static rejoin into Stable skips the
        // rebalance and returns the cached assignment.
        let action = handle_join(&mut g, &join_req("m1", Some("inst-a")), "h");
        match action {
            JoinAction::Immediate(r) => {
                assert!(r.error_code == codes::NONE);
                assert!(r.generation_id == g.generation_id);
            }
            _ => panic!("expected Immediate success (static rejoin)"),
        }
    }

    #[test]
    fn join_all_members_rejoined_completes_now() {
        let mut g = ClassicState::new("g");
        let _ = handle_join(&mut g, &join_req("m1", None), "h");
        g.complete_rebalance("range"); // gen 1, Stable-ish path
        g.state = GroupState::Stable;
        // m2 joins → Preparing, only m2 joined this round → Park.
        assert!(matches!(
            handle_join(&mut g, &join_req("m2", None), "h"),
            JoinAction::Park
        ));
        // m1 rejoins → all members joined this round, from a live (Stable)
        // group → CompleteNow.
        assert!(matches!(
            handle_join(&mut g, &join_req("m1", None), "h"),
            JoinAction::CompleteNow
        ));
    }

    #[test]
    fn join_into_reemptied_group_parks_to_batch_herd() {
        // A group that has rebalanced before (gen > 0) and then went `Empty`
        // — e.g. a warm-up consumer joined and left — must NOT eager-complete
        // a solo generation when the first new member rejoins. It parks for
        // the configured initial-delay window so a herd of consumers starting
        // together batches into one generation. Regression test for the
        // produce+consume throughput collapse triggered by re-joining a group
        // a prior consumer had already used.
        let mut g = ClassicState::new("g");
        let _ = handle_join(&mut g, &join_req("warmup", None), "h");
        g.complete_rebalance("range"); // gen 1
        g.remove_member("warmup"); // group is now Empty, generation still 1
        check!(g.state == GroupState::Empty);
        check!(g.generation_id == 1);
        // First real member rejoins the re-emptied group: Park (batch the
        // herd), NOT CompleteNow, even though the group has rebalanced before.
        assert!(matches!(
            handle_join(&mut g, &join_req("m1", None), "h"),
            JoinAction::Park
        ));
        check!(g.rebalance_from_empty);
    }

    #[test]
    fn build_join_result_leader_lists_members_follower_empty() {
        let mut g = ClassicState::new("g");
        let _ = handle_join(&mut g, &join_req("m1", None), "h");
        let _ = handle_join(&mut g, &join_req("m2", None), "h");
        assert!(try_complete(&mut g).is_ok());
        let leader = g.leader_id.clone().unwrap();
        let follower = if leader == "m1" { "m2" } else { "m1" };
        assert!(!build_join_result(&g, &leader).members.is_empty());
        assert!(build_join_result(&g, follower).members.is_empty());
    }

    #[test]
    fn try_complete_empty_intersection_is_err() {
        let mut g = ClassicState::new("g");
        let mut a = join_req("m1", None);
        a.protocols = vec![JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: Bytes::new(),
            ..Default::default()
        }];
        let _ = handle_join(&mut g, &a, "h");
        let mut b = join_req("m2", None);
        b.protocols = vec![JoinGroupRequestProtocol {
            name: "cooperative-sticky".into(),
            metadata: Bytes::new(),
            ..Default::default()
        }];
        let _ = handle_join(&mut g, &b, "h");
        assert!(try_complete(&mut g).is_err());
    }
}
