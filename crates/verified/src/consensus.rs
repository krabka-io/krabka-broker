//! KIP-595 consensus decision kernels, extracted from `krabka-kraft-core` so
//! Creusot can verify them (the host crate's `Instant`/async surface is
//! untranslatable). The functions carry their Creusot preconditions,
//! postconditions, invariants, variants, and supporting lemmas directly beside
//! the executable bodies.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Majority size for a voter set: `floor(n / 2) + 1`.
#[ensures(result@ == voter_count@ / 2 + 1)]
#[must_use]
pub const fn majority_size(voter_count: usize) -> usize {
    voter_count / 2 + 1
}

/// Whether the unique grants from current voters reach a majority.
#[ensures(result == (current_grants@ >= voter_count@ / 2 + 1))]
#[must_use]
pub const fn election_has_quorum(voter_count: usize, current_grants: usize) -> bool {
    current_grants >= majority_size(voter_count)
}

/// Offset-aware recovery mode resolved for one partition.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FailoverRecovery {
    None,
    Balanced,
    Aggressive,
}

/// Safety action selected after the host classifies live ISR and replicas.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FailoverAction {
    ElectClean,
    ElectFromElr,
    Recover(FailoverRecovery),
    ElectUnclean,
    Unavailable,
    ShrinkIsr,
    NoChange,
}

/// What the live members of the partition's ISR can do once the leader is
/// gone. Every live ISR member holds every committed record; a witness is a
/// live member that never leads.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum LiveIsr {
    /// No ISR member is live.
    Empty,
    /// Every live ISR member is a witness, so none can lead.
    WitnessesOnly,
    /// A live ISR member that is not a witness can lead.
    Electable,
}

/// The out-of-ISR options, consulted only when no ISR member is live.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct OutOfIsrFacts {
    /// A live KIP-966 eligible leader replica can lead.
    pub has_electable_elr: bool,
    /// The topic's resolved offset-aware recovery strategy.
    pub recovery: FailoverRecovery,
    /// The KIP-841 out-of-ISR election is both permitted by
    /// `unclean.leader.election.enable` and has a live replica to elect.
    pub unclean_election_available: bool,
}

/// What the host established about one partition before the failover
/// decision. The kernel does not see replica sets; every field is the host's
/// classification of them.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FailoverFacts {
    /// The partition's leader is the replica that went away.
    pub leader_dead: bool,
    /// The live ISR is smaller than the recorded ISR.
    pub isr_shrunk: bool,
    /// What the live ISR members can do.
    pub live_isr: LiveIsr,
    /// The options left when no ISR member is live.
    pub out_of_isr: OutOfIsrFacts,
}

/// Select the failover action class by a fixed precedence ladder.
///
/// What the contract proves is the ladder, and only the ladder. For a dead
/// leader it picks, in order: a clean election when a live ISR member can
/// lead; otherwise, and only when no ISR member is live, an election from the
/// eligible leader replicas; otherwise the configured offset-aware recovery;
/// otherwise the KIP-841 election when it is available; otherwise the
/// partition stays unavailable. A live ISR of witnesses only stops the ladder
/// at unavailable. For a live leader it only shrinks the ISR or leaves it
/// alone. Each outcome is pinned to exactly one combination of
/// [`FailoverFacts`].
///
/// Why each rung is safe is not part of the proof. It rests on the host's
/// classification and on Kafka's semantics: an ISR member holds every
/// committed record; a KIP-966 eligible leader replica left the ISR while the
/// partition still held `min.insync.replicas` members, so it holds every
/// committed record too, and neither the `unclean.leader.election.enable`
/// toggle nor `unclean.recovery.strategy` gates it. Its rung is reachable only
/// once the live ISR is empty, the same guard Apache Kafka's
/// `PartitionChangeBuilder.isValidNewLeader` puts on its `targetElr` disjunct.
///
/// `unclean_election_available` joins the KIP-841 toggle and the existence of
/// a replica that can serve, because the ladder never separates them: an
/// election that is allowed with nobody to elect and one that has a candidate
/// and no permission are the same unavailable partition.
#[ensures(match result {
    FailoverAction::ElectClean => facts.leader_dead && facts.live_isr == LiveIsr::Electable,
    FailoverAction::ElectFromElr => facts.leader_dead
        && facts.live_isr == LiveIsr::Empty
        && facts.out_of_isr.has_electable_elr,
    FailoverAction::Recover(selected) => facts.leader_dead
        && facts.live_isr == LiveIsr::Empty
        && !facts.out_of_isr.has_electable_elr
        && facts.out_of_isr.recovery != FailoverRecovery::None
        && selected == facts.out_of_isr.recovery,
    FailoverAction::ElectUnclean => facts.leader_dead
        && facts.live_isr == LiveIsr::Empty
        && !facts.out_of_isr.has_electable_elr
        && facts.out_of_isr.recovery == FailoverRecovery::None
        && facts.out_of_isr.unclean_election_available,
    FailoverAction::Unavailable => facts.leader_dead
        && (facts.live_isr == LiveIsr::WitnessesOnly
            || (facts.live_isr == LiveIsr::Empty
                && !facts.out_of_isr.has_electable_elr
                && facts.out_of_isr.recovery == FailoverRecovery::None
                && !facts.out_of_isr.unclean_election_available)),
    FailoverAction::ShrinkIsr => !facts.leader_dead && facts.isr_shrunk,
    FailoverAction::NoChange => !facts.leader_dead && !facts.isr_shrunk,
})]
#[must_use]
pub fn failover_action(facts: FailoverFacts) -> FailoverAction {
    if !facts.leader_dead {
        return if facts.isr_shrunk {
            FailoverAction::ShrinkIsr
        } else {
            FailoverAction::NoChange
        };
    }
    match facts.live_isr {
        LiveIsr::Electable => return FailoverAction::ElectClean,
        LiveIsr::WitnessesOnly => return FailoverAction::Unavailable,
        LiveIsr::Empty => {}
    }
    let out_of_isr = facts.out_of_isr;
    if out_of_isr.has_electable_elr {
        return FailoverAction::ElectFromElr;
    }
    match out_of_isr.recovery {
        FailoverRecovery::Balanced | FailoverRecovery::Aggressive => {
            FailoverAction::Recover(out_of_isr.recovery)
        }
        FailoverRecovery::None if out_of_isr.unclean_election_available => {
            FailoverAction::ElectUnclean
        }
        FailoverRecovery::None => FailoverAction::Unavailable,
    }
}

/// One surviving replica's log as KIP-966 unclean recovery ranks it, from its
/// `GetReplicaLogInfo` answer.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RecoveryCandidate {
    /// The leader epoch of the last record the replica wrote.
    pub last_epoch: i32,
    /// The offset one past the replica's last record.
    pub log_end_offset: i64,
    /// The replica's broker id, which breaks a tie deterministically.
    pub broker_id: u64,
}

/// `a` ranks at or above `b` as a KIP-966 recovery candidate: the higher last
/// written leader epoch first, then the higher log end offset, then the lower
/// broker ID.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn ranks_ge(a: RecoveryCandidate, b: RecoveryCandidate) -> bool {
    pearlite! {
        a.last_epoch@ > b.last_epoch@
            || (a.last_epoch@ == b.last_epoch@
                && (a.log_end_offset@ > b.log_end_offset@
                    || (a.log_end_offset@ == b.log_end_offset@ && a.broker_id@ <= b.broker_id@)))
    }
}

/// Select the replica with highest `(last leader epoch, log end offset)` and
/// lowest broker ID as the deterministic tie-breaker.
///
/// The contract states that ranking once, as `ranks_ge`: the returned index
/// ranks at or above every candidate. `None` is returned exactly for an empty
/// slice.
#[ensures(match result {
    None => candidates@.len() == 0,
    Some(best) => best@ < candidates@.len()
        && forall<j: Int> 0 <= j && j < candidates@.len()
            ==> ranks_ge(candidates@[best@], candidates@[j]),
})]
#[must_use]
pub fn select_best_recovery_replica(candidates: &[RecoveryCandidate]) -> Option<usize> {
    let n = candidates.len();
    if n == 0 {
        return None;
    }
    let mut best = 0usize;
    let mut i = 1usize;
    #[invariant(1 <= i@ && i@ <= n@)]
    #[invariant(best@ < i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> ranks_ge(candidates@[best@], candidates@[j]))]
    #[variant(n@ - i@)]
    while i < n {
        let candidate = candidates[i];
        let current = candidates[best];
        if candidate.last_epoch > current.last_epoch
            || (candidate.last_epoch == current.last_epoch
                && candidate.log_end_offset > current.log_end_offset)
            || (candidate.last_epoch == current.last_epoch
                && candidate.log_end_offset == current.log_end_offset
                && candidate.broker_id < current.broker_id)
        {
            best = i;
        }
        i += 1;
    }
    Some(best)
}

/// Members of `{log_end} U s` with value >= `v`. This is the
/// majority-replication witness.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn hwm_member_at(log_end: Int, s: Seq<i64>, k: Int) -> Int {
    pearlite! {
        if k == 0 { log_end } else { s[k - 1]@ }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[variant(limit)]
pub fn count_ge_prefix(log_end: Int, s: Seq<i64>, v: Int, limit: Int, leader_counts: bool) -> Int {
    pearlite! {
        if limit <= 0 {
            0
        } else {
            count_ge_prefix(log_end, s, v, limit - 1, leader_counts)
                + (if (leader_counts || limit > 1)
                    && hwm_member_at(log_end, s, limit - 1) >= v { 1 } else { 0 })
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[variant(s.len())]
pub fn count_ge(log_end: Int, s: Seq<i64>, v: Int, leader_counts: bool) -> Int {
    pearlite! { count_ge_prefix(log_end, s, v, s.len() + 1, leader_counts) }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= limit && limit <= s.len() + 1)]
#[requires(low <= high)]
#[ensures(count_ge_prefix(log_end, s, low, limit, leader_counts)
    >= count_ge_prefix(log_end, s, high, limit, leader_counts))]
#[variant(limit)]
pub fn lemma_count_ge_prefix_monotone(
    log_end: Int,
    s: Seq<i64>,
    low: Int,
    high: Int,
    limit: Int,
    leader_counts: bool,
) {
    if limit > 0 {
        lemma_count_ge_prefix_monotone(log_end, s, low, high, limit - 1, leader_counts);
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= limit && limit <= s.len() + 1)]
#[ensures(count_ge_prefix(log_end, s, v, limit, leader_counts) >= 0)]
#[variant(limit)]
pub fn lemma_count_ge_prefix_nonnegative(
    log_end: Int,
    s: Seq<i64>,
    v: Int,
    limit: Int,
    leader_counts: bool,
) {
    if limit > 0 {
        lemma_count_ge_prefix_nonnegative(log_end, s, v, limit - 1, leader_counts);
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 < limit && limit <= s.len() + 1)]
#[requires(count_ge_prefix(log_end, s, v, limit, leader_counts) >= 1)]
#[ensures(0 <= result && result < limit)]
#[ensures(hwm_member_at(log_end, s, result) >= v)]
#[ensures(count_ge_prefix(log_end, s, hwm_member_at(log_end, s, result), limit, leader_counts)
    >= count_ge_prefix(log_end, s, v, limit, leader_counts))]
#[variant(limit)]
pub fn least_hwm_member_ge_index(
    log_end: Int,
    s: Seq<i64>,
    v: Int,
    limit: Int,
    leader_counts: bool,
) -> Int {
    // Every step below unfolds `count_ge_prefix` once at `limit`, where the
    // last member always counts because `limit > 1` puts it past the leader.
    if limit <= 1 {
        0
    } else {
        let last_index = limit - 1;
        let last_member = hwm_member_at(log_end, s, last_index);
        let previous_count = count_ge_prefix(log_end, s, v, last_index, leader_counts);
        if last_member >= v {
            if previous_count >= 1 {
                let previous_index =
                    least_hwm_member_ge_index(log_end, s, v, last_index, leader_counts);
                let previous_member = hwm_member_at(log_end, s, previous_index);
                if last_member <= previous_member {
                    // The last member is the smaller witness. Monotonicity
                    // carries the earlier witness's count down to it.
                    lemma_count_ge_prefix_monotone(
                        log_end,
                        s,
                        last_member,
                        previous_member,
                        last_index,
                        leader_counts,
                    );
                    last_index
                } else {
                    // The earlier witness stays smaller; the last member
                    // reaches it, so both counts gain exactly one at `limit`.
                    previous_index
                }
            } else {
                // No earlier member reaches `v`, so `v`'s count at `limit` is
                // exactly the last member's one, and the last member's own
                // count is at least that.
                lemma_count_ge_prefix_nonnegative(log_end, s, v, last_index, leader_counts);
                lemma_count_ge_prefix_nonnegative(
                    log_end,
                    s,
                    last_member,
                    last_index,
                    leader_counts,
                );
                last_index
            }
        } else {
            // The last member misses `v`, so the witness is an earlier one.
            least_hwm_member_ge_index(log_end, s, v, last_index, leader_counts)
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[requires(1 <= majority@ && majority@ <= s@.len() + 1)]
#[ensures(forall<v: Int> count_ge(log_end@, s@, v, leader_counts) >= majority@
    ==> exists<k: Int> 0 <= k && k < s@.len() + 1
        && hwm_member_at(log_end@, s@, k) >= v
        && count_ge(log_end@, s@, hwm_member_at(log_end@, s@, k), leader_counts) >= majority@)]
pub fn lemma_hwm_threshold_has_member(
    log_end: i64,
    s: &[i64],
    majority: usize,
    leader_counts: bool,
) {
    proof_assert!(forall<v: Int> count_ge(log_end@, s@, v, leader_counts) >= majority@ ==>
        exists<k: Int> k == least_hwm_member_ge_index(log_end@, s@, v, s@.len() + 1, leader_counts)
            && 0 <= k && k < s@.len() + 1
            && hwm_member_at(log_end@, s@, k) >= v
            && count_ge(log_end@, s@, hwm_member_at(log_end@, s@, k), leader_counts) >= majority@);
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[requires(1 <= majority@ && majority@ <= s@.len() + 1)]
#[requires(forall<k: Int> 0 <= k && k < s@.len() + 1
    && count_ge(log_end@, s@, hwm_member_at(log_end@, s@, k), leader_counts) >= majority@
    ==> hwm_member_at(log_end@, s@, k) <= best@)]
#[ensures(forall<v: Int> count_ge(log_end@, s@, v, leader_counts) >= majority@ ==> v <= best@)]
pub fn lemma_hwm_member_maximal(
    log_end: i64,
    s: &[i64],
    majority: usize,
    best: i64,
    leader_counts: bool,
) {
    lemma_hwm_threshold_has_member(log_end, s, majority, leader_counts);
}

/// Deterministic per-`(node, epoch)` election-timeout jitter in `[0, base_ms)`.
///
/// This is Raft's randomized backoff, made reproducible for the deterministic
/// sims: the jitter is a fixed hash of the node ID and the epoch, so the same
/// run replays the same way. The contract proves only the range, `0` for a
/// zero base and below `base_ms` otherwise. It does not prove that two nodes,
/// or two epochs of one node, get different values; the hash usually spreads
/// them, and the unit tests pin a few spread values, but a collision is
/// possible and nothing here rules it out.
#[ensures(base_ms@ == 0 ==> result@ == 0)]
#[ensures(base_ms@ > 0 ==> result@ < base_ms@)]
#[must_use]
pub fn election_jitter_ms(me: u64, epoch: u32, base_ms: u64) -> u64 {
    if base_ms == 0 {
        return 0;
    }
    // Cheap integer hash of (node id, epoch); avoids any RNG so the sims stay
    // deterministic.
    let mix = me.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ u64::from(epoch).wrapping_mul(0xD1B5_4A32_D192_ED03);
    mix % base_ms
}

/// `true` if the candidate's log is at least as up-to-date as ours.
///
/// KIP-595: the higher last epoch wins. On a tie, the higher or equal offset
/// wins.
#[ensures(result == (cand_epoch@ > my_epoch@
    || (cand_epoch@ == my_epoch@ && cand_offset@ >= my_end@)))]
#[must_use]
pub const fn log_is_up_to_date(
    my_epoch: u32,
    my_end: i64,
    cand_epoch: u32,
    cand_offset: i64,
) -> bool {
    cand_epoch > my_epoch || (cand_epoch == my_epoch && cand_offset >= my_end)
}

#[requires(1 <= majority@ && majority@ <= follower_offsets@.len() + 1)]
#[ensures(result == (count_ge(log_end@, follower_offsets@, cand@, leader_counts) >= majority@))]
fn candidate_has_majority(
    log_end: i64,
    follower_offsets: &[i64],
    cand: i64,
    majority: usize,
    leader_counts: bool,
) -> bool {
    let mut count: usize = 0;
    if leader_counts && log_end >= cand && count < majority {
        count += 1;
    }

    let n = follower_offsets.len();
    let mut j = 0;
    #[invariant(j@ <= n@)]
    #[invariant({
        let seen = count_ge_prefix(log_end@, follower_offsets@, cand@, j@ + 1, leader_counts);
        count@ == if seen < majority@ { seen } else { majority@ }
    })]
    #[invariant(count@ <= majority@)]
    #[variant(n@ - j@)]
    while j < n {
        let x = follower_offsets[j];
        if x >= cand && count < majority {
            count += 1;
        }
        j += 1;
    }

    count >= majority
}

/// The HWM as the majority-th largest match offset across the leader's own log
/// end, when `leader_counts`, and every follower's acknowledged fetch offset.
///
/// The leader-completeness rule of Raft Fig.8 and KIP-595 gates this value: the
/// HWM may only advance once the majority offset is strictly past
/// `epoch_start_offset`. The HWM never regresses below `current_hwm`.
///
/// When `leader_counts` is `false` (a leader that its own `VotersRecord`
/// removed), the followers alone must be able to reach `majority`, which is
/// the second precondition.
///
/// The function computes the majority-th largest by its definition, and not by
/// a sort. That definition is the greatest member m of
/// `{log_end} U follower_offsets` with at least `majority` members >= m. Voter
/// counts are tiny, at most about 7, and the Creusot proof quantifies over a
/// loop that mirrors the definition.
#[requires(1 <= majority@ && majority@ <= follower_offsets@.len() + 1)]
#[requires(leader_counts || majority@ <= follower_offsets@.len())]
#[requires(current_hwm@ <= log_end@)]
#[requires(forall<k: Int> 0 <= k && k < follower_offsets@.len()
    ==> follower_offsets@[k]@ <= log_end@)]
#[ensures(result@ >= current_hwm@)]
#[ensures(result@ <= log_end@)]
#[ensures(forall<v: Int> v > epoch_start_offset@
    && count_ge(log_end@, follower_offsets@, v, leader_counts) >= majority@
    ==> v <= result@)]
#[ensures(result@ > current_hwm@
    ==> result@ > epoch_start_offset@
        && count_ge(log_end@, follower_offsets@, result@, leader_counts) >= majority@)]
#[must_use]
pub fn recompute_high_watermark(
    log_end: i64,
    follower_offsets: &[i64],
    majority: usize,
    epoch_start_offset: i64,
    current_hwm: i64,
    leader_counts: bool,
) -> i64 {
    let n = follower_offsets.len();
    let mut majority_offset = i64::MIN;
    if candidate_has_majority(log_end, follower_offsets, log_end, majority, leader_counts) {
        majority_offset = log_end;
    }

    let mut i = 0;
    #[invariant(i@ <= n@)]
    #[invariant(majority_offset@ <= log_end@)]
    #[invariant(majority_offset@ == -9223372036854775807 - 1
        || count_ge(log_end@, follower_offsets@, majority_offset@, leader_counts) >= majority@)]
    #[invariant(forall<k: Int> 0 <= k && k < i@ + 1
        && count_ge(log_end@, follower_offsets@, hwm_member_at(log_end@, follower_offsets@, k), leader_counts) >= majority@
        ==> hwm_member_at(log_end@, follower_offsets@, k) <= majority_offset@)]
    #[variant(n@ - i@)]
    while i < n {
        let cand = follower_offsets[i];
        if cand > majority_offset
            && candidate_has_majority(log_end, follower_offsets, cand, majority, leader_counts)
        {
            majority_offset = cand;
        }
        i += 1;
    }
    #[cfg(creusot)]
    lemma_hwm_member_maximal(
        log_end,
        follower_offsets,
        majority,
        majority_offset,
        leader_counts,
    );
    let gated = if majority_offset > epoch_start_offset {
        majority_offset
    } else {
        current_hwm
    };
    gated.max(current_hwm)
}

/// The watermark a majority has acknowledged, never below `current`.
///
/// This is [`recompute_high_watermark`] with the leader's log end always
/// counted and no leader-epoch gate. The result is the greater of `current`
/// and the majority frontier: the greatest member of
/// `{log_end} U follower_offsets` that at least `majority` members reach.
/// Passing `current` as both the gate and the floor is what makes it that: the
/// frontier is taken only when it passes `current`, and `current` otherwise.
///
/// A caller that needs Raft's current-term rule, the gate on the leader's
/// first record of its own epoch, uses [`recompute_high_watermark`] instead.
#[requires(1 <= majority@ && majority@ <= follower_offsets@.len() + 1)]
#[requires(current@ <= log_end@)]
#[requires(forall<k: Int> 0 <= k && k < follower_offsets@.len()
    ==> follower_offsets@[k]@ <= log_end@)]
#[ensures(result@ >= current@)]
#[ensures(result@ <= log_end@)]
#[ensures(forall<v: Int> count_ge(log_end@, follower_offsets@, v, true) >= majority@
    ==> v <= result@)]
#[ensures(result@ > current@
    ==> count_ge(log_end@, follower_offsets@, result@, true) >= majority@)]
#[must_use]
pub fn majority_watermark(
    log_end: i64,
    follower_offsets: &[i64],
    majority: usize,
    current: i64,
) -> i64 {
    recompute_high_watermark(log_end, follower_offsets, majority, current, current, true)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use proptest::prelude::*;

    use super::*;

    /// The leader's own log end counts toward a candidate's majority only when
    /// it actually reaches that candidate.
    ///
    /// `count < majority` guards the increment so the tally saturates, which
    /// makes it invisible on its own -- the answer is `count >= majority`
    /// either way. What is visible is joining the two conditions with `||`
    /// instead of `&&`: an empty tally satisfies `count < majority` for any
    /// majority, so the leader would be counted for a candidate its log has
    /// not reached, and one follower would look like two.
    #[test]
    fn leader_counts_toward_a_candidate_only_when_its_log_reaches_it() {
        // One follower at 5 and a leader at 0, needing two of the three.
        check!(!candidate_has_majority(0, &[5], 5, 2, true));
        // The same shape with the leader's log at the candidate: now it counts.
        check!(candidate_has_majority(5, &[5], 5, 2, true));
    }

    /// The production implementation that this kernel replaced: sort
    /// descending, take the majority-th largest, gate on `epoch_start`, and
    /// clamp monotonic.
    fn hwm_sort_oracle(
        log_end: i64,
        follower_offsets: &[i64],
        majority: usize,
        epoch_start_offset: i64,
        current_hwm: i64,
        leader_counts: bool,
    ) -> i64 {
        let mut match_offsets: Vec<i64> = Vec::with_capacity(follower_offsets.len() + 1);
        if leader_counts {
            match_offsets.push(log_end);
        }
        match_offsets.extend_from_slice(follower_offsets);
        match_offsets.sort_unstable_by(|a, b| b.cmp(a));
        let majority_offset = match_offsets[majority - 1];
        let gated = if majority_offset > epoch_start_offset {
            majority_offset
        } else {
            current_hwm
        };
        gated.max(current_hwm)
    }

    proptest! {
        #[test]
        fn hwm_matches_sort_oracle(
            log_end in 0i64..1_000,
            followers in proptest::collection::vec(0i64..1_000, 1..7),
            majority_seed in 0usize..8,
            epoch_start_offset in 0i64..1_000,
            current_hwm in 0i64..1_000,
            leader_counts in any::<bool>(),
        ) {
            let majority = 1 + majority_seed % (followers.len() + usize::from(leader_counts));
            // Kernel precondition domain: clamp like the kraft-core call site does.
            let followers: Vec<i64> = followers.iter().map(|o| (*o).min(log_end)).collect();
            let current_hwm = current_hwm.min(log_end);
            prop_assert_eq!(
                recompute_high_watermark(
                    log_end,
                    &followers,
                    majority,
                    epoch_start_offset,
                    current_hwm,
                    leader_counts,
                ),
                hwm_sort_oracle(
                    log_end,
                    &followers,
                    majority,
                    epoch_start_offset,
                    current_hwm,
                    leader_counts,
                )
            );
        }

        #[test]
        fn jitter_in_range(me in any::<u64>(), epoch in any::<u32>(), base in 1u64..10_000) {
            prop_assert!(election_jitter_ms(me, epoch, base) < base);
        }
    }

    #[test]
    fn jitter_zero_base_is_zero() {
        assert2::assert!(election_jitter_ms(7, 3, 0) == 0);
    }

    #[test]
    fn jitter_uses_node_and_epoch_hash_inputs() {
        for (_name, node, epoch, expected) in [
            ("node one epoch zero", 1, 0, 485),
            ("node two epoch zero", 2, 0, 354),
            ("node one epoch one", 1, 1, 446),
        ] {
            assert2::assert!(election_jitter_ms(node, epoch, 1000) == expected);
        }
    }

    #[test]
    fn up_to_date_is_the_kip595_rule() {
        // higher epoch wins regardless of offset
        for (name, ours_epoch, ours_offset, candidate_epoch, candidate_offset, expected) in [
            ("higher epoch", 5, 100, 6, 0, true),
            ("same epoch equal offset", 5, 100, 5, 100, true),
            ("same epoch older offset", 5, 100, 5, 99, false),
            ("lower epoch", 5, 0, 4, i64::MAX, false),
        ] {
            check!(
                log_is_up_to_date(ours_epoch, ours_offset, candidate_epoch, candidate_offset)
                    == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn election_quorum_is_a_strict_majority() {
        for (voters, grants, expected) in [
            (1, 1, true),
            (2, 1, false),
            (2, 2, true),
            (3, 1, false),
            (3, 2, true),
            (4, 2, false),
            (4, 3, true),
        ] {
            check!(election_has_quorum(voters, grants) == expected);
        }
    }

    #[test]
    fn hwm_never_regresses_and_gates_on_epoch_start() {
        // majority offset (2 of {10, 3, 9} with majority=2 -> 9) is <= epoch_start 9: hold.
        for (name, followers, epoch_start, current, expected) in [
            ("gated at epoch start", &[3, 9][..], 9, 5, 5),
            ("advances past epoch start", &[3, 9][..], 8, 5, 9),
            ("never regresses", &[1, 1][..], 0, 7, 7),
        ] {
            check!(
                recompute_high_watermark(10, followers, 2, epoch_start, current, true) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn hwm_counts_leader_and_followers_until_majority() {
        for (name, followers, majority, expected) in [
            ("two of three", &[9, 8][..], 2, 9),
            ("all three at leader", &[10, 10][..], 3, 10),
            ("all three below leader", &[4, 4][..], 3, 4),
        ] {
            check!(
                recompute_high_watermark(10, followers, majority, 0, 0, true) == expected,
                "case {name}"
            );
        }
    }

    /// A three-voter WAL quorum: the leader's log end is one vote and a
    /// majority is two, so the watermark is the higher follower ack once it
    /// passes the current watermark, and the current watermark otherwise.
    #[test]
    fn majority_watermark_follows_the_second_highest_ack() {
        for (name, log_end, followers, current, expected) in [
            ("no follower has acked", 10, &[0, 0][..], 0, 0),
            ("one follower ack makes a majority", 10, &[7, 0][..], 0, 7),
            ("the higher of two acks wins", 10, &[4, 9][..], 0, 9),
            ("an ack at the leader end", 10, &[10, 3][..], 5, 10),
            ("a stale ack never lowers it", 10, &[3, 2][..], 5, 5),
            ("an empty log", 0, &[0, 0][..], 0, 0),
        ] {
            check!(
                majority_watermark(log_end, followers, 2, current) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn hwm_can_exclude_a_removed_leader() {
        check!(recompute_high_watermark(10, &[9, 4], 2, 0, 0, true) == 9);
        check!(recompute_high_watermark(10, &[9, 4], 2, 0, 0, false) == 4);
    }

    #[test]
    fn failover_action_covers_clean_unclean_recovery_and_shrink_paths() {
        use FailoverAction::{
            ElectClean, ElectFromElr, ElectUnclean, NoChange, Recover, ShrinkIsr, Unavailable,
        };
        use FailoverRecovery::{Aggressive, Balanced, None};
        use LiveIsr::{Electable, Empty, WitnessesOnly};

        let dead =
            |live_isr, has_electable_elr, recovery, unclean_election_available| FailoverFacts {
                leader_dead: true,
                isr_shrunk: true,
                live_isr,
                out_of_isr: OutOfIsrFacts {
                    has_electable_elr,
                    recovery,
                    unclean_election_available,
                },
            };
        let alive = |isr_shrunk| FailoverFacts {
            leader_dead: false,
            isr_shrunk,
            live_isr: Electable,
            out_of_isr: OutOfIsrFacts {
                has_electable_elr: false,
                recovery: None,
                unclean_election_available: false,
            },
        };
        for (name, facts, expected) in [
            (
                "a live ISR member leads, whatever the out-of-ISR options",
                dead(Electable, true, Aggressive, true),
                ElectClean,
            ),
            // An electable ELR member outranks every offset-aware strategy and
            // the KIP-841 election, and never reaches either.
            ("ELR alone", dead(Empty, true, None, false), ElectFromElr),
            (
                "ELR over the unclean election",
                dead(Empty, true, None, true),
                ElectFromElr,
            ),
            (
                "ELR over offset-aware recovery",
                dead(Empty, true, Balanced, false),
                ElectFromElr,
            ),
            (
                "balanced recovery",
                dead(Empty, false, Balanced, false),
                Recover(Balanced),
            ),
            (
                "aggressive recovery over the unclean election",
                dead(Empty, false, Aggressive, true),
                Recover(Aggressive),
            ),
            (
                "KIP-841 unclean election",
                dead(Empty, false, None, true),
                ElectUnclean,
            ),
            (
                "nothing to elect",
                dead(Empty, false, None, false),
                Unavailable,
            ),
            // A live ISR that holds only witnesses is unavailable even with an
            // ELR, a recovery strategy, and an unclean election: every
            // out-of-ISR rung is guarded on an empty ISR.
            (
                "witness-only ISR",
                dead(WitnessesOnly, true, Balanced, true),
                Unavailable,
            ),
            ("a follower left the ISR", alive(true), ShrinkIsr),
            ("nothing changed", alive(false), NoChange),
        ] {
            check!(failover_action(facts) == expected, "case {name}");
        }
    }

    #[test]
    fn recovery_replica_ranking_is_epoch_then_offset_then_lowest_node() {
        let at = |last_epoch, log_end_offset, broker_id| RecoveryCandidate {
            last_epoch,
            log_end_offset,
            broker_id,
        };
        for (name, candidates, expected) in [
            ("nobody answered", std::vec![], None),
            (
                "a newer epoch beats a longer log",
                std::vec![at(4, 100, 2), at(5, 10, 3)],
                Some(1),
            ),
            (
                "the longer log wins within an epoch",
                std::vec![at(5, 90, 2), at(5, 120, 3)],
                Some(1),
            ),
            (
                "the lowest broker id breaks a full tie",
                std::vec![at(5, 100, 3), at(5, 100, 1), at(5, 100, 2)],
                Some(1),
            ),
            (
                "identical answers keep the first",
                std::vec![at(5, 100, 1), at(5, 100, 1)],
                Some(0),
            ),
        ] {
            check!(
                select_best_recovery_replica(&candidates) == expected,
                "case {name}"
            );
        }
    }
}
