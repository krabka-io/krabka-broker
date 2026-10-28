use super::*;

/// A follower's row that nothing has fenced: the epoch it asked under is
/// still the current one, and the leader named no other target.
const LIVE_ROW: ReplicaFetchFacts = ReplicaFetchFacts {
    request_leader_epoch: 4,
    current_leader_epoch: 4,
    target_matches: true,
    reported_target_matches: true,
    error_code: 0,
    diverging_epoch: -1,
};

/// Broker 1 takes its preferred partition back: it is `replicas[0]`,
/// alive, in the ISR and not a witness.
const PREFERRED_BACK: PreferredLeaderChange = PreferredLeaderChange {
    new_leader: 1,
    preferred_replica: Some(1),
    leader_in_isr: true,
    leader_alive: true,
    leader_is_witness: false,
};

mod replica_fetch_mutation_fences_every_input_and_selects_one_action;

mod fetch_visibility_matches_kafka_fetch_scenarios;

mod arithmetic_edges_are_explicit;
