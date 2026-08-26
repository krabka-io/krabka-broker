//! Durability arithmetic for a stretch cluster.
//!
//! A stretch cluster holds one Kafka cluster in more than one site. The claim
//! that such a cluster keeps the data and stays available through the loss of
//! one whole site rests on three numbers: the replica count that survives a
//! site loss, the `min.insync.replicas` value that stays satisfiable after that
//! loss, and the split of the `KRaft` voters over the sites. This module states
//! those three numbers as kernels, and Creusot proves the contracts.
//!
//! The preconditions bound the inputs at 1024. A replication factor, a site
//! count, and a voter count are all small in a real deployment. The bounds only
//! keep the arithmetic away from `i64` overflow, and a stated bound is honest
//! about what the proof covers.

use creusot_std::prelude::*;

/// The sum of the first `limit` elements of `s`.
#[cfg(creusot)]
#[logic]
#[variant(limit)]
pub fn sum_prefix(s: Seq<i64>, limit: Int) -> Int {
    pearlite! {
        if limit <= 0 {
            0
        } else {
            sum_prefix(s, limit - 1) + s[limit - 1]@
        }
    }
}

/// The sum of every element of `s`. This is the total voter count.
#[cfg(creusot)]
#[logic]
pub fn sum_all(s: Seq<i64>) -> Int {
    pearlite! { sum_prefix(s, s.len()) }
}

/// One step of the `sum_prefix` recursion, as a fact about `limit - 1`.
#[cfg(creusot)]
#[logic]
#[requires(1 <= limit && limit <= s.len())]
#[ensures(sum_prefix(s, limit) == sum_prefix(s, limit - 1) + s[limit - 1]@)]
pub fn lemma_sum_prefix_step(s: Seq<i64>, limit: Int) {}

/// A prefix sum is not negative when no element of that prefix is negative.
#[cfg(creusot)]
#[logic]
#[requires(0 <= limit && limit <= s.len())]
#[requires(forall<k: Int> 0 <= k && k < limit ==> s[k]@ >= 0)]
#[ensures(sum_prefix(s, limit) >= 0)]
#[variant(limit)]
pub fn lemma_sum_prefix_nonnegative(s: Seq<i64>, limit: Int) {
    if limit > 0 {
        lemma_sum_prefix_nonnegative(s, limit - 1);
        lemma_sum_prefix_step(s, limit);
    }
}

/// Two sites never survive a site loss, whatever the split of the voters.
///
/// This is the reason a stretch cluster needs a third site. One of the two
/// sites holds at least half of the voters. The loss of that site leaves half
/// of the voters or less, and half is not a strict majority.
#[cfg(creusot)]
#[requires(voters_per_site@.len() == 2)]
#[requires(forall<k: Int> 0 <= k && k < 2 ==> voters_per_site@[k]@ >= 0)]
#[ensures(exists<k: Int> 0 <= k && k < voters_per_site@.len()
    && 2 * (sum_all(voters_per_site@) - voters_per_site@[k]@) <= sum_all(voters_per_site@))]
pub fn lemma_two_sites_never_survive(voters_per_site: &[i64]) {
    proof_assert!(sum_prefix(voters_per_site@, 1) == voters_per_site@[0]@);
    proof_assert!(sum_all(voters_per_site@) == voters_per_site@[0]@ + voters_per_site@[1]@);
    proof_assert!(voters_per_site@[0]@ >= voters_per_site@[1]@
        ==> 2 * (sum_all(voters_per_site@) - voters_per_site@[0]@) <= sum_all(voters_per_site@));
    proof_assert!(voters_per_site@[1]@ >= voters_per_site@[0]@
        ==> 2 * (sum_all(voters_per_site@) - voters_per_site@[1]@) <= sum_all(voters_per_site@));
}

/// The replica count that survives the loss of any one site.
///
/// The placement puts the `rf` replicas of one partition on `sites` sites, one
/// replica per site in turn. The largest count that lands in a single site is
/// then `ceil(rf / sites)`, so the loss of one site takes away at most that
/// many replicas. The result is the count that remains.
///
/// An operator reads this number to pick `min.insync.replicas`. Three replicas
/// on three sites leave 2, because each site holds one replica. Three replicas
/// on two sites leave 1, because two of the three replicas share a site. One
/// site leaves 0, because the loss of that site is the loss of the partition.
#[requires(rf@ >= 1 && rf@ <= 1024)]
#[requires(sites@ >= 1 && sites@ <= 1024)]
#[ensures(result@ == rf@ - (rf@ + sites@ - 1) / sites@)]
#[ensures(result@ >= 0)]
#[ensures(result@ < rf@)]
#[ensures(sites@ == 1 ==> result@ == 0)]
#[must_use]
pub fn site_loss_survivors(rf: i64, sites: i64) -> i64 {
    // `(rf - 1) * (sites - 1) >= 0` gives `rf + sites - 1 <= rf * sites`, which
    // is what bounds the ceiling by `rf` and keeps the result at 0 or above.
    proof_assert!((rf@ - 1) * (sites@ - 1) >= 0);
    proof_assert!(rf@ + sites@ - 1 <= rf@ * sites@);
    rf - (rf + sites - 1) / sites
}

/// `true` if `min_insync` keeps `acks=all` writes durable and available through
/// the loss of any one site.
///
/// Two bounds define the safe range. The lower bound of 2 makes an
/// acknowledged write durable on more than one broker, so the loss of one
/// broker cannot lose that write. The upper bound is
/// [`site_loss_survivors`], so enough in-sync replicas remain after a site
/// loss and the leader keeps accepting `acks=all` writes.
///
/// For three replicas on three sites the two bounds meet, and 2 is the only
/// safe value. For three replicas on two sites no value is safe: a site loss
/// can leave one replica, and one replica is under the durable lower bound.
/// A witness site closes that gap. It holds a replica of its own, so the
/// replicas spread one per site over three sites again.
#[requires(rf@ >= 1 && rf@ <= 1024)]
#[requires(sites@ >= 1 && sites@ <= 1024)]
#[ensures(result == (min_insync@ >= 2 && min_insync@ <= rf@ - (rf@ + sites@ - 1) / sites@))]
#[must_use]
pub fn min_insync_is_site_loss_safe(rf: i64, sites: i64, min_insync: i64) -> bool {
    min_insync >= 2 && min_insync <= site_loss_survivors(rf, sites)
}

/// `true` if the surviving `KRaft` voters still form a strict majority after
/// the loss of any one site.
///
/// `voters_per_site[k]` is the count of `KRaft` voters in site `k`. The check
/// asks for every site `k` whether `2 * (total - voters_per_site[k]) > total`
/// holds, where `total` is the sum of the slice. A strict majority is what a
/// `KRaft` quorum needs to elect a leader and to commit a metadata record, so a
/// cluster that fails this check stops its metadata writes when it loses a
/// site.
///
/// Two sites never pass this check. Any split of the voters over two sites
/// leaves one site with half of them or more, and the loss of that site leaves
/// half or less. This is why a third site must hold at least one voter. A
/// data-bearing witness is the smallest form of that third site: it holds one
/// voter, so `[1, 1, 1]` passes the check, and it holds a replica as well, so
/// it also counts toward `min.insync.replicas`.
///
/// An empty slice returns `true`. There is no site to lose, so the condition
/// holds for every site of the empty set.
///
/// The preconditions bound the slice at 1024 sites and each site at 1024
/// voters. The total is then 1048576 at most, and `2 * total` cannot overflow.
#[requires(voters_per_site@.len() <= 1024)]
#[requires(forall<k: Int> 0 <= k && k < voters_per_site@.len()
    ==> 0 <= voters_per_site@[k]@ && voters_per_site@[k]@ <= 1024)]
#[ensures(result == (forall<k: Int> 0 <= k && k < voters_per_site@.len()
    ==> 2 * (sum_all(voters_per_site@) - voters_per_site@[k]@) > sum_all(voters_per_site@)))]
#[must_use]
pub fn quorum_survives_any_single_site_loss(voters_per_site: &[i64]) -> bool {
    let n = voters_per_site.len();

    let mut total: i64 = 0;
    let mut i = 0;
    #[invariant(i@ <= n@)]
    #[invariant(total@ == sum_prefix(voters_per_site@, i@))]
    #[invariant(0 <= total@ && total@ <= i@ * 1024)]
    #[variant(n@ - i@)]
    while i < n {
        proof_assert!(sum_prefix(voters_per_site@, i@ + 1)
            == sum_prefix(voters_per_site@, i@) + voters_per_site@[i@]@);
        total += voters_per_site[i];
        i += 1;
    }

    let mut k = 0;
    let mut survives = true;
    #[invariant(k@ <= n@)]
    #[invariant(0 <= total@ && total@ <= 1024 * 1024)]
    #[invariant(total@ == sum_all(voters_per_site@))]
    #[invariant(survives == (forall<j: Int> 0 <= j && j < k@
        ==> 2 * (sum_all(voters_per_site@) - voters_per_site@[j]@) > sum_all(voters_per_site@)))]
    #[variant(n@ - k@)]
    while k < n {
        if 2 * (total - voters_per_site[k]) <= total {
            survives = false;
        }
        k += 1;
    }
    survives
}

#[cfg(test)]
mod tests {
    use std::iter;

    use assert2::check;
    use proptest::prelude::*;

    use super::*;

    /// Places `rf` replicas one per site in turn, then reports the count that
    /// remains after the loss of the site that holds the most replicas. This
    /// is an independent implementation, and not the ceiling formula again.
    fn round_robin_oracle(rf: i64, sites: i64) -> i64 {
        let site_count = usize::try_from(sites).expect("site count fits in usize");
        let mut buckets: Vec<i64> = iter::repeat_n(0, site_count).collect();
        let mut next = 0usize;
        let mut placed = 0i64;
        while placed < rf {
            buckets[next] += 1;
            next = (next + 1) % site_count;
            placed += 1;
        }
        let largest = buckets.iter().copied().max().expect("at least one site");
        rf - largest
    }

    /// Sums the slice and checks every site with an iterator chain.
    fn quorum_oracle(voters_per_site: &[i64]) -> bool {
        let total: i64 = voters_per_site.iter().sum();
        voters_per_site
            .iter()
            .copied()
            .all(|voters| 2 * (total - voters) > total)
    }

    proptest! {
        #[test]
        fn survivors_match_round_robin_placement(rf in 1i64..64, sites in 1i64..8) {
            prop_assert_eq!(site_loss_survivors(rf, sites), round_robin_oracle(rf, sites));
        }

        #[test]
        fn safe_min_insync_stays_inside_the_surviving_replicas(
            rf in 1i64..64,
            sites in 1i64..8,
            min_insync in 0i64..64,
        ) {
            let survivors = round_robin_oracle(rf, sites);
            prop_assert_eq!(
                min_insync_is_site_loss_safe(rf, sites, min_insync),
                min_insync >= 2 && min_insync <= survivors
            );
        }

        #[test]
        fn quorum_matches_iterator_oracle(
            voters_per_site in proptest::collection::vec(0i64..8, 0..7),
        ) {
            prop_assert_eq!(
                quorum_survives_any_single_site_loss(&voters_per_site),
                quorum_oracle(&voters_per_site)
            );
        }
    }

    #[test]
    fn site_loss_takes_away_the_largest_site() {
        for (name, rf, sites, expected) in [
            ("three replicas over three sites", 3, 3, 2),
            // Two of the three replicas share a site, so a site loss can leave
            // one replica. This is the two-site gap that a witness site closes.
            ("three replicas over two sites", 3, 2, 1),
            ("five replicas over three sites", 5, 3, 3),
            ("one site holds every replica", 4, 1, 0),
        ] {
            check!(site_loss_survivors(rf, sites) == expected, "case {name}");
        }
    }

    #[test]
    fn three_sites_pin_min_insync_replicas_to_two() {
        for (name, min_insync, expected) in [
            ("one replica is not durable", 1, false),
            ("two is the only safe value", 2, true),
            ("three cannot survive a site loss", 3, false),
        ] {
            check!(
                min_insync_is_site_loss_safe(3, 3, min_insync) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn two_sites_leave_no_safe_min_insync_replicas() {
        // Three replicas over two sites survive with one replica, and one
        // replica is under the durable lower bound of two. No value is safe,
        // which is the gap that a witness site in a third site closes.
        for (name, min_insync, expected) in [
            ("one replica is not durable", 1, false),
            ("two is more than the surviving replicas", 2, false),
            ("three cannot survive a site loss", 3, false),
        ] {
            check!(
                min_insync_is_site_loss_safe(3, 2, min_insync) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn quorum_needs_a_voter_in_a_third_site() {
        for (name, voters_per_site, expected) in [
            ("one voter in each of three sites", &[1, 1, 1][..], true),
            ("two sites lose the quorum either way", &[1, 1][..], false),
            ("five voters over three sites", &[2, 2, 1][..], true),
            ("one site holds three of five voters", &[3, 1, 1][..], false),
            ("no sites at all", &[][..], true),
        ] {
            check!(
                quorum_survives_any_single_site_loss(voters_per_site) == expected,
                "case {name}"
            );
        }
    }
}
