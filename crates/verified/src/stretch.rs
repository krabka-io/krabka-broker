//! Durability arithmetic for a stretch cluster.
//!
//! A stretch cluster holds one Kafka cluster in more than one site. The claim
//! that such a cluster keeps the data and stays available through the loss of
//! one whole site rests on three numbers: the replica count that survives a
//! site loss, the `min.insync.replicas` value that stays satisfiable after that
//! loss, and the split of the `KRaft` voters over the sites. This module states
//! those three numbers as kernels, and Creusot proves the contracts.
//!
//! The replica numbers are stated against a model of the placement,
//! `round_robin_load`: replica `r` of a partition lands on site `r % sites`.
//! The contracts say what each site then holds and what the loss of each site
//! leaves, and the proof derives the `ceil(rf / sites)` arithmetic from that
//! model. That the broker's placer actually places replicas this way is not
//! proved here; it is the host's responsibility.
//!
//! The preconditions bound the inputs at 1024. A replication factor, a site
//! count, and a voter count are all small in a real deployment. The bounds only
//! keep the arithmetic away from `i64` overflow, and a stated bound is honest
//! about what the proof covers.

use creusot_std::prelude::*;

/// The sum of the first `limit` elements of `s`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
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
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn sum_all(s: Seq<i64>) -> Int {
    pearlite! { sum_prefix(s, s.len()) }
}

/// One step of the `sum_prefix` recursion, as a fact about `limit - 1`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(1 <= limit && limit <= s.len())]
#[ensures(sum_prefix(s, limit) == sum_prefix(s, limit - 1) + s[limit - 1]@)]
pub fn lemma_sum_prefix_step(s: Seq<i64>, limit: Int) {}

/// A prefix sum is not negative when no element of that prefix is negative.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
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
/// This is the reason a stretch cluster needs a third site, and it is the
/// claim [`quorum_survives_any_single_site_loss`]'s documentation makes about
/// two sites. One of the two sites holds at least half of the voters. The loss
/// of that site leaves half of the voters or less, and half is not a strict
/// majority. No other proof calls this lemma; it exists so that the two-site
/// claim is proved rather than only asserted.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
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

/// The replicas that round-robin placement puts on `site`: the placement
/// visits sites `0, 1, ..., sites - 1, 0, 1, ...` in turn, so replica `r`
/// lands on site `r % sites`, and this counts the `r < rf` that land on
/// `site`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[variant(rf)]
pub fn round_robin_load(rf: Int, sites: Int, site: Int) -> Int {
    pearlite! {
        if rf <= 0 {
            0
        } else {
            round_robin_load(rf - 1, sites, site)
                + if (rf - 1) % sites == site { 1 } else { 0 }
        }
    }
}

/// Integer division by a positive divisor is monotone on nonnegative
/// numerators.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= low && low <= high && divisor >= 1)]
#[ensures(low / divisor <= high / divisor)]
pub fn lemma_div_monotone(low: Int, high: Int, divisor: Int) {
    // Both numerators split into quotient and remainder.
    proof_assert!(low == divisor * (low / divisor) + low % divisor && 0 <= low % divisor);
    proof_assert!(high == divisor * (high / divisor) + high % divisor && high % divisor < divisor);
    // A larger quotient for `low` would put `low` a whole divisor past `high`.
    proof_assert!(low / divisor > high / divisor
        ==> (low / divisor - high / divisor - 1) * divisor >= 0);
}

/// A numerator written as `divisor * quotient + remainder`, with the
/// remainder in `[0, divisor)`, divides to exactly that quotient.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(divisor >= 1 && quotient >= 0 && 0 <= remainder && remainder < divisor)]
#[ensures((divisor * quotient + remainder) / divisor == quotient)]
pub fn lemma_div_exact(divisor: Int, quotient: Int, remainder: Int) {
    // Why3's `Div_mult` peels the whole multiples off, and `Div_inf` makes the
    // remainder's own quotient 0.
    proof_assert!((divisor * quotient + remainder) / divisor == quotient + remainder / divisor);
    proof_assert!(remainder / divisor == 0);
}

/// One more replica raises the closed form `(rf - site + sites - 1) / sites`
/// by one exactly when that replica lands on `site`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(rf >= 1 && sites >= 1 && 0 <= site && site < sites)]
#[ensures((rf - site + sites - 1) / sites
    == (rf - 1 - site + sites - 1) / sites + if (rf - 1) % sites == site { 1 } else { 0 })]
pub fn lemma_round_robin_step(rf: Int, sites: Int, site: Int) {
    let q = (rf - 1) / sites;
    let r = (rf - 1) % sites;
    // Division of the nonnegative `rf - 1`: quotient `q`, remainder `r`.
    proof_assert!(rf - 1 == sites * q + r && 0 <= r && r < sites && q >= 0);
    // Each case writes both numerators as `sites * quotient + remainder` with
    // the remainder in `[0, sites)`, which fixes the quotient.
    if r == site {
        proof_assert!(rf - site + sites - 1 == sites * (q + 1) + 0);
        proof_assert!(rf - 1 - site + sites - 1 == sites * q + (sites - 1));
        lemma_div_exact(sites, q + 1, 0);
        lemma_div_exact(sites, q, sites - 1);
    } else if r > site {
        proof_assert!(rf - site + sites - 1 == sites * (q + 1) + (r - site));
        proof_assert!(rf - 1 - site + sites - 1 == sites * (q + 1) + (r - site - 1));
        lemma_div_exact(sites, q + 1, r - site);
        lemma_div_exact(sites, q + 1, r - site - 1);
    } else {
        proof_assert!(rf - site + sites - 1 == sites * q + (sites + r - site));
        proof_assert!(rf - 1 - site + sites - 1 == sites * q + (sites + r - site - 1));
        lemma_div_exact(sites, q, sites + r - site);
        lemma_div_exact(sites, q, sites + r - site - 1);
    }
}

/// The closed form of `round_robin_load`: site `site` holds
/// `(rf - site + sites - 1) / sites` replicas.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(rf >= 0 && sites >= 1 && 0 <= site && site < sites)]
#[ensures(round_robin_load(rf, sites, site) == (rf - site + sites - 1) / sites)]
#[variant(rf)]
pub fn lemma_round_robin_load(rf: Int, sites: Int, site: Int) {
    if rf > 0 {
        lemma_round_robin_load(rf - 1, sites, site);
        lemma_round_robin_step(rf, sites, site);
    }
}

/// No site holds more than site 0 under round-robin placement, so site 0 is
/// a fullest site.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(rf >= 0 && sites >= 1 && 0 <= site && site < sites)]
#[ensures(round_robin_load(rf, sites, site) <= round_robin_load(rf, sites, 0))]
pub fn lemma_site_load_at_most_first(rf: Int, sites: Int, site: Int) {
    // Both loads in closed form; site 0 has the largest numerator.
    lemma_round_robin_load(rf, sites, site);
    lemma_round_robin_load(rf, sites, 0);
    lemma_div_monotone(rf - site + sites - 1, rf + sites - 1, sites);
}

/// The replica count that survives the loss of any one site.
///
/// Under the round-robin placement, `round_robin_load`, site 0 is a fullest
/// site. The contract proves that the result is what the loss of site 0
/// leaves, and that the loss of any other site leaves at least as many, so it
/// is the fewest replicas any single site loss leaves. The body computes it as
/// `rf - ceil(rf / sites)`; the proof derives that ceiling from the model
/// rather than taking it as the definition.
///
/// An operator reads this number to pick `min.insync.replicas`. Three replicas
/// on three sites leave 2, because each site holds one replica. Three replicas
/// on two sites leave 1, because two of the three replicas share a site. One
/// site leaves 0, because the loss of that site is the loss of the partition.
#[requires(rf@ >= 1 && rf@ <= 1024)]
#[requires(sites@ >= 1 && sites@ <= 1024)]
#[ensures(result@ == rf@ - round_robin_load(rf@, sites@, 0))]
#[ensures(forall<site: Int> 0 <= site && site < sites@
    ==> rf@ - round_robin_load(rf@, sites@, site) >= result@)]
#[ensures(result@ >= 0)]
#[ensures(result@ < rf@)]
#[ensures(sites@ == 1 ==> result@ == 0)]
#[must_use]
pub fn site_loss_survivors(rf: i64, sites: i64) -> i64 {
    // `(rf - 1) * (sites - 1) >= 0` gives `rf + sites - 1 <= rf * sites`, which
    // is what bounds the ceiling by `rf` and keeps the result at 0 or above.
    proof_assert!((rf@ - 1) * (sites@ - 1) >= 0);
    proof_assert!(rf@ + sites@ - 1 <= sites@ * rf@ + 0);
    proof_assert!({
        lemma_div_monotone(rf@ + sites@ - 1, sites@ * rf@ + 0, sites@);
        lemma_div_exact(sites@, rf@, 0);
        (rf@ + sites@ - 1) / sites@ <= rf@
    });
    // The placement model: site 0 holds the ceiling, and no site holds more.
    proof_assert!({
        lemma_round_robin_load(rf@, sites@, 0);
        round_robin_load(rf@, sites@, 0) == (rf@ + sites@ - 1) / sites@
    });
    proof_assert!(forall<site: Int> 0 <= site && site < sites@ ==> {
        lemma_site_load_at_most_first(rf@, sites@, site);
        round_robin_load(rf@, sites@, site) <= round_robin_load(rf@, sites@, 0)
    });
    rf - (rf + sites - 1) / sites
}

/// `true` if `min_insync` keeps `acks=all` writes durable and available through
/// the loss of any one site.
///
/// Two bounds define the safe range, and the contract states both against
/// every site of the round-robin placement, `round_robin_load`.
///
/// The **lower** bound is that no single site holds `min_insync` replicas.
/// `min.insync.replicas` is a count and not a placement: the leader accepts an
/// `acks=all` write as soon as that many in-sync replicas hold it, wherever
/// they sit. If one site can hold `min_insync` of them, then the in-sync set
/// can shrink to that one site, and the leader then acknowledges a write that
/// only that site holds. The loss of that site loses an acknowledged write,
/// and the surviving sites still hold the voter majority, so they elect a
/// leader that never saw it. The shrink needs no second site loss to get
/// there: one site down and one lagging replica is enough, and a witness on a
/// cheap link is the replica most likely to lag.
///
/// The **upper** bound is that the loss of any one site leaves at least
/// `min_insync` replicas, so the leader keeps accepting `acks=all` writes. The
/// fewest that a site loss leaves is [`site_loss_survivors`].
///
/// The lower bound implies `min_insync >= 2`, because one site always holds at
/// least one replica. It is therefore the whole of the durability condition,
/// and 2 is not stated separately.
///
/// For three replicas on three sites each site holds one, so the bounds are
/// `min_insync > 1` and `min_insync <= 2`: they meet, and 2 is the only safe
/// value. Four replicas on three sites have no safe value at all, because one
/// site holds two of them: the bounds become `min_insync > 2` and
/// `min_insync <= 2`. Three replicas on two sites have none either, for the
/// same reason. A witness site closes that gap. It holds a replica of its own,
/// so three replicas spread one per site over three sites again.
#[requires(rf@ >= 1 && rf@ <= 1024)]
#[requires(sites@ >= 1 && sites@ <= 1024)]
#[ensures(result == (
    (forall<site: Int> 0 <= site && site < sites@
        ==> round_robin_load(rf@, sites@, site) < min_insync@)
    && (forall<site: Int> 0 <= site && site < sites@
        ==> rf@ - round_robin_load(rf@, sites@, site) >= min_insync@)
))]
#[must_use]
pub fn min_insync_is_site_loss_safe(rf: i64, sites: i64, min_insync: i64) -> bool {
    let survivors = site_loss_survivors(rf, sites);
    // What the fullest site holds. `site_loss_survivors` is `rf` less that
    // count, so this recovers it without restating the ceiling.
    let largest_site = rf - survivors;
    min_insync > largest_site && min_insync <= survivors
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

    /// Places `rf` replicas one per site in turn and reports what each site
    /// holds. This is an independent implementation, and not the ceiling
    /// formula again. Both oracles below read it.
    fn round_robin_buckets(rf: i64, sites: i64) -> Vec<i64> {
        let site_count = usize::try_from(sites).expect("site count fits in usize");
        let mut buckets: Vec<i64> = iter::repeat_n(0, site_count).collect();
        let mut next = 0usize;
        let mut placed = 0i64;
        while placed < rf {
            buckets[next] += 1;
            next = (next + 1) % site_count;
            placed += 1;
        }
        buckets
    }

    /// What the site holding the most replicas holds.
    fn largest_site_oracle(rf: i64, sites: i64) -> i64 {
        round_robin_buckets(rf, sites)
            .into_iter()
            .max()
            .expect("at least one site")
    }

    /// The count that remains after the loss of the site that holds the most
    /// replicas.
    fn round_robin_oracle(rf: i64, sites: i64) -> i64 {
        rf - largest_site_oracle(rf, sites)
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
            let buckets = round_robin_buckets(rf, sites);
            let survivors = rf - buckets.iter().copied().max().expect("a site");
            // Stated as the two properties themselves rather than as the
            // bounds: no single site can hold a full in-sync set, and a site
            // loss leaves one.
            let one_site_could_hold_the_whole_isr =
                buckets.iter().any(|&held| held >= min_insync);
            prop_assert_eq!(
                min_insync_is_site_loss_safe(rf, sites, min_insync),
                !one_site_could_hold_the_whole_isr && min_insync <= survivors
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

    /// A replication factor that puts two replicas in one site has no safe
    /// `min.insync.replicas` over three sites, and the reason is the lower
    /// bound rather than the upper one.
    ///
    /// Four replicas over three sites land 2-1-1. A `min.insync.replicas` of 2
    /// is then satisfiable inside the two-replica site alone, so that site can
    /// hold every copy of an acknowledged write. Raising it to 3 fixes the
    /// placement problem and breaks availability instead: the loss of the
    /// two-replica site leaves 2. Nothing in between exists, so the profile
    /// takes one replica per site and no more.
    #[test]
    fn a_site_holding_two_replicas_has_no_safe_min_insync() {
        for (name, rf, sites, min_insync, expected) in [
            (
                "a whole in-sync set fits in the doubled site",
                4,
                3,
                2,
                false,
            ),
            (
                "raising it past the doubled site loses a site loss",
                4,
                3,
                3,
                false,
            ),
            ("three over two sites is the same shape", 3, 2, 2, false),
            // Six over three sites is 2-2-2: three replicas cannot share a
            // site, and a site loss still leaves four.
            (
                "six over three sites has room for both bounds",
                6,
                3,
                3,
                true,
            ),
            ("and one more, still inside the survivors", 6, 3, 4, true),
            (
                "but not five, which a site loss cannot leave",
                6,
                3,
                5,
                false,
            ),
        ] {
            check!(
                min_insync_is_site_loss_safe(rf, sites, min_insync) == expected,
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
