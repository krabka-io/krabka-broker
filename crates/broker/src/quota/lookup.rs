//! Quota lookup with Kafka's 8-priority entity matching.

use krabka_metadata::{EntityKey, MetadataImage};
use krabka_verified::{
    IpQuotaPrecedence, QuotaCandidatePresence, UserClientQuotaFacts, UserClientQuotaPrecedence,
    ip_quota_precedence, user_client_quota_precedence,
};

/// Return the configured value for `quota_key` under the most-specific
/// matching entity for `(principal, client_id)`. First match wins, in the
/// order of Kafka's `ClientQuotaManager.DefaultQuotaCallback` and the
/// "Quotas" section of the Kafka documentation:
///   1. `/config/users/<user>/clients/<client-id>` (user=alice, client-id=app1)
///   2. `/config/users/<user>/clients/<default>` (user=alice, client-id=default)
///   3. `/config/users/<user>` (user=alice)
///   4. `/config/users/<default>/clients/<client-id>` (user=default, client-id=app1)
///   5. `/config/users/<default>/clients/<default>` (user=default, client-id=default)
///   6. `/config/users/<default>` (user=default)
///   7. `/config/clients/<client-id>` (client-id=app1)
///   8. `/config/clients/<default>` (client-id=default)
///
/// The verified [`user_client_quota_precedence`] kernel makes the choice.
/// This adapter supplies one presence fact per candidate.
// Disjoint from `lookup_ip_quota` (which checks `("ip", *)` candidates only).
#[must_use]
pub fn lookup_quota(
    image: &MetadataImage,
    principal: &str,
    client_id: &str,
    quota_key: &str,
) -> Option<f64> {
    lookup_quota_with_key(image, principal, client_id, quota_key).map(|(_, v)| v)
}

/// Like `lookup_quota`, but it also returns the entity key that enforcement
/// binds to a bucket in `QuotaBuckets`.
///
/// The bucket key follows Kafka's `quotaMetricTags`: a quota configured on a
/// user/client pair (exact or default on either side) throttles the exact
/// `(client-id, user)` pair, a user-level quota throttles the exact user, and
/// a client-level quota throttles the exact client id. Default entities are
/// therefore shared limits applied per exact principal or client, not one
/// bucket for everyone.
#[must_use]
pub fn lookup_quota_with_key(
    image: &MetadataImage,
    principal: &str,
    client_id: &str,
    quota_key: &str,
) -> Option<(EntityKey, f64)> {
    let rate = |candidate| {
        let key = candidate_key(candidate, principal, client_id)?;
        quota_for_key(image, key, quota_key)
    };
    let presence = |candidate| {
        if rate(candidate).is_some() {
            QuotaCandidatePresence::Present
        } else {
            QuotaCandidatePresence::Absent
        }
    };
    let selected = user_client_quota_precedence(UserClientQuotaFacts {
        user_client: presence(UserClientQuotaPrecedence::UserClient),
        user_default_client: presence(UserClientQuotaPrecedence::UserDefaultClient),
        user: presence(UserClientQuotaPrecedence::User),
        default_user_client: presence(UserClientQuotaPrecedence::DefaultUserClient),
        default_user_default_client: presence(UserClientQuotaPrecedence::DefaultUserDefaultClient),
        default_user: presence(UserClientQuotaPrecedence::DefaultUser),
        client: presence(UserClientQuotaPrecedence::Client),
        default_client: presence(UserClientQuotaPrecedence::DefaultClient),
    });
    let (_, rate) = rate(selected)?;
    Some((bucket_key(selected, principal, client_id)?, rate))
}

/// The canonical metadata key of one user/client candidate, or `None` for
/// [`UserClientQuotaPrecedence::None`].
///
/// Entity parts are sorted by `entity_type` ("client-id" < "user"), which is
/// the canonical order of the image map, so the key needs no further
/// canonicalization.
fn candidate_key(
    candidate: UserClientQuotaPrecedence,
    principal: &str,
    client_id: &str,
) -> Option<EntityKey> {
    let user = || ("user".to_owned(), Some(principal.to_owned()));
    let default_user = || ("user".to_owned(), None);
    let client = || ("client-id".to_owned(), Some(client_id.to_owned()));
    let default_client = || ("client-id".to_owned(), None);
    Some(match candidate {
        UserClientQuotaPrecedence::UserClient => vec![client(), user()],
        UserClientQuotaPrecedence::UserDefaultClient => vec![default_client(), user()],
        UserClientQuotaPrecedence::User => vec![user()],
        UserClientQuotaPrecedence::DefaultUserClient => vec![client(), default_user()],
        UserClientQuotaPrecedence::DefaultUserDefaultClient => {
            vec![default_client(), default_user()]
        }
        UserClientQuotaPrecedence::DefaultUser => vec![default_user()],
        UserClientQuotaPrecedence::Client => vec![client()],
        UserClientQuotaPrecedence::DefaultClient => vec![default_client()],
        UserClientQuotaPrecedence::None => return None,
    })
}

/// The bucket key Kafka's `quotaMetricTags` gives the selected candidate.
fn bucket_key(
    selected: UserClientQuotaPrecedence,
    principal: &str,
    client_id: &str,
) -> Option<EntityKey> {
    let user = || ("user".to_owned(), Some(principal.to_owned()));
    let client = || ("client-id".to_owned(), Some(client_id.to_owned()));
    Some(match selected {
        UserClientQuotaPrecedence::UserClient
        | UserClientQuotaPrecedence::UserDefaultClient
        | UserClientQuotaPrecedence::DefaultUserClient
        | UserClientQuotaPrecedence::DefaultUserDefaultClient => vec![client(), user()],
        UserClientQuotaPrecedence::User | UserClientQuotaPrecedence::DefaultUser => vec![user()],
        UserClientQuotaPrecedence::Client | UserClientQuotaPrecedence::DefaultClient => {
            vec![client()]
        }
        UserClientQuotaPrecedence::None => return None,
    })
}

/// Lookup an `ip`-scoped quota for `peer_ip`. Priority order:
///   1. (ip = `Some(peer_ip)`): specific
///   2. (ip = None): default
///
/// This accepts both IPv4 and IPv6 peers. Kafka keys IP quotas by the IP's
/// string form for either family, so the same two-priority match applies.
///
/// It is disjoint from `lookup_quota`, which checks only `("user", *)` and
/// `("client-id", *)` candidates. KIP-612 `connection_creation_rate`
/// enforcement uses this function.
#[must_use]
pub fn lookup_ip_quota(
    image: &MetadataImage,
    peer_ip: std::net::IpAddr,
    quota_key: &str,
) -> Option<f64> {
    lookup_ip_quota_with_key(image, peer_ip, quota_key).map(|(_, v)| v)
}

#[must_use]
pub fn lookup_ip_quota_with_key(
    image: &MetadataImage,
    peer_ip: std::net::IpAddr,
    quota_key: &str,
) -> Option<(EntityKey, f64)> {
    let candidates: [EntityKey; 2] = [
        vec![("ip".into(), Some(peer_ip.to_string()))],
        vec![("ip".into(), None)],
    ];
    let matches = candidates.map(|key| quota_for_key(image, key, quota_key));
    match ip_quota_precedence(matches[0].is_some(), matches[1].is_some()) {
        IpQuotaPrecedence::Exact => matches[0].clone(),
        IpQuotaPrecedence::Default => {
            let (_, rate) = matches[1].clone()?;
            Some((vec![("ip".into(), Some(peer_ip.to_string()))], rate))
        }
        IpQuotaPrecedence::None => None,
    }
}

fn quota_for_key(
    image: &MetadataImage,
    key: EntityKey,
    quota_key: &str,
) -> Option<(EntityKey, f64)> {
    image
        .client_quotas()
        .get(&key)
        .and_then(|configs| configs.get(quota_key))
        .map(|value| (key, *value))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{ClientQuotaRecord, MetadataImage};

    use super::*;
    use crate::quota::test_support::{image_with_quotas, quota_record};

    fn img_with(records: Vec<ClientQuotaRecord>) -> MetadataImage {
        image_with_quotas(records)
    }

    fn rec(entity: Vec<(&str, Option<&str>)>, key: &str, value: f64) -> ClientQuotaRecord {
        quota_record(entity, key, value)
    }

    #[test]
    fn exact_user_client_pair_match() {
        let img = img_with(vec![rec(
            vec![("user", Some("alice")), ("client-id", Some("app1"))],
            "producer_byte_rate",
            1024.0,
        )]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == Some(1024.0));
    }

    #[test]
    fn user_default_falls_back_to_client_specific() {
        // Only (client-id=app1) configured; user=alice should still match.
        let img = img_with(vec![rec(
            vec![("client-id", Some("app1"))],
            "producer_byte_rate",
            1024.0,
        )]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == Some(1024.0));
    }

    #[test]
    fn single_user_match_when_no_pair_exists() {
        let img = img_with(vec![rec(
            vec![("user", Some("alice"))],
            "producer_byte_rate",
            2048.0,
        )]);
        assert!(lookup_quota(&img, "alice", "anyclient", "producer_byte_rate") == Some(2048.0));
    }

    #[test]
    fn single_client_id_match_when_no_user_exists() {
        let img = img_with(vec![rec(
            vec![("client-id", Some("app1"))],
            "producer_byte_rate",
            512.0,
        )]);
        assert!(lookup_quota(&img, "anyuser", "app1", "producer_byte_rate") == Some(512.0));
    }

    #[test]
    fn default_user_default_client_pair() {
        let img = img_with(vec![rec(
            vec![("user", None), ("client-id", None)],
            "producer_byte_rate",
            256.0,
        )]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == Some(256.0));
    }

    #[test]
    fn default_user_alone() {
        let img = img_with(vec![rec(vec![("user", None)], "producer_byte_rate", 128.0)]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == Some(128.0));
    }

    #[test]
    fn default_client_alone() {
        let img = img_with(vec![rec(
            vec![("client-id", None)],
            "producer_byte_rate",
            64.0,
        )]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == Some(64.0));
    }

    #[test]
    fn no_match_returns_none() {
        let img = img_with(vec![]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == None);
    }

    #[test]
    fn pair_specific_wins_over_user_only() {
        let img = img_with(vec![
            rec(vec![("user", Some("alice"))], "producer_byte_rate", 8192.0),
            rec(
                vec![("user", Some("alice")), ("client-id", Some("app1"))],
                "producer_byte_rate",
                512.0,
            ),
        ]);
        assert!(lookup_quota(&img, "alice", "app1", "producer_byte_rate") == Some(512.0));
    }

    fn rec_ip(ip: Option<&str>, key: &str, value: f64) -> ClientQuotaRecord {
        quota_record(vec![("ip", ip)], key, value)
    }

    fn img_with_ip(records: Vec<ClientQuotaRecord>) -> MetadataImage {
        image_with_quotas(records)
    }

    #[test]
    fn ip_specific_match() {
        let img = img_with_ip(vec![rec_ip(
            Some("127.0.0.1"),
            "connection_creation_rate",
            1.0,
        )]);
        let ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        assert!(lookup_ip_quota(&img, ip, "connection_creation_rate") == Some(1.0));
    }

    #[test]
    fn ip_default_fallback() {
        let img = img_with_ip(vec![rec_ip(None, "connection_creation_rate", 2.0)]);
        let ip: std::net::IpAddr = "10.0.0.7".parse().unwrap();
        assert!(lookup_ip_quota(&img, ip, "connection_creation_rate") == Some(2.0));
    }

    #[test]
    fn ip_specific_wins_over_default() {
        let img = img_with_ip(vec![
            rec_ip(None, "connection_creation_rate", 8.0),
            rec_ip(Some("127.0.0.1"), "connection_creation_rate", 1.0),
        ]);
        let ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        assert!(lookup_ip_quota(&img, ip, "connection_creation_rate") == Some(1.0));
    }

    #[test]
    fn ip_no_match_returns_none() {
        let img = img_with_ip(vec![]);
        let ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        assert!(lookup_ip_quota(&img, ip, "connection_creation_rate").is_none());
    }

    #[test]
    fn ipv6_specific_match() {
        // KIP-612: the connection-creation-rate quota must resolve for an
        // IPv6 peer keyed by its canonical string form, not just IPv4.
        let img = img_with_ip(vec![rec_ip(Some("::1"), "connection_creation_rate", 3.0)]);
        let ip: std::net::IpAddr = "::1".parse().unwrap();
        assert!(lookup_ip_quota(&img, ip, "connection_creation_rate") == Some(3.0));
    }

    #[test]
    fn ipv6_default_fallback() {
        // An IPv6 peer with no specific entry falls back to the (ip=None)
        // default, proving IPv6 is no longer skipped by the quota path.
        let img = img_with_ip(vec![rec_ip(None, "connection_creation_rate", 5.0)]);
        let ip: std::net::IpAddr = "2001:db8::42".parse().unwrap();
        assert!(lookup_ip_quota(&img, ip, "connection_creation_rate") == Some(5.0));
    }

    // ── precedence verification: Kafka scenarios, exhaustive enumeration, proptest
    //
    // The 8-priority (user/client) and 2-priority (IP) orders are declared
    // HERE from Kafka's documented table ("Quotas" section of the Kafka docs,
    // `ClientQuotaManager.DefaultQuotaCallback.findUserClientQuota` /
    // `findUserQuota` / `findClientQuota`), independently of production, so a
    // reordering in the kernel or in this adapter is caught. Index = priority
    // (lower wins). Parameterized by the probe `(principal, client_id)`.
    fn uc_candidates<'a>(
        principal: &'a str,
        client_id: &'a str,
    ) -> [Vec<(&'a str, Option<&'a str>)>; 8] {
        [
            // /config/users/<user>/clients/<client-id>
            vec![("user", Some(principal)), ("client-id", Some(client_id))],
            // /config/users/<user>/clients/<default>
            vec![("user", Some(principal)), ("client-id", None)],
            // /config/users/<user>
            vec![("user", Some(principal))],
            // /config/users/<default>/clients/<client-id>
            vec![("user", None), ("client-id", Some(client_id))],
            // /config/users/<default>/clients/<default>
            vec![("user", None), ("client-id", None)],
            // /config/users/<default>
            vec![("user", None)],
            // /config/clients/<client-id>
            vec![("client-id", Some(client_id))],
            // /config/clients/<default>
            vec![("client-id", None)],
        ]
    }

    fn entity(parts: &[(&str, Option<&str>)]) -> EntityKey {
        parts
            .iter()
            .map(|(kind, name)| ((*kind).to_owned(), name.map(str::to_owned)))
            .collect()
    }

    /// Kafka scenarios for `(user=alice, client-id=app1)`. Each row lists the
    /// configured `producer_byte_rate` entities and the whole expected result:
    /// the rate Kafka applies and the bucket entity Kafka's `quotaMetricTags`
    /// throttles.
    #[test]
    fn user_client_lookup_matches_kafka_scenarios() {
        const MB: f64 = 1_048_576.0;
        type Row<'a> = (
            &'a str,
            Vec<(Vec<(&'a str, Option<&'a str>)>, f64)>,
            Option<(EntityKey, f64)>,
        );
        let pair = entity(&[("client-id", Some("app1")), ("user", Some("alice"))]);
        let alice = entity(&[("user", Some("alice"))]);
        let app1 = entity(&[("client-id", Some("app1"))]);
        let rows: Vec<Row<'_>> = vec![
            (
                "a user quota beats a default-user quota for the same client id",
                vec![
                    (vec![("user", Some("alice"))], 10.0 * MB),
                    (vec![("user", None), ("client-id", Some("app1"))], MB),
                ],
                Some((alice.clone(), 10.0 * MB)),
            ),
            (
                "a user quota beats the default pair",
                vec![
                    (vec![("user", Some("alice"))], 10.0 * MB),
                    (vec![("user", None), ("client-id", None)], MB),
                ],
                Some((alice.clone(), 10.0 * MB)),
            ),
            (
                "the user's default-client quota beats the user quota",
                vec![
                    (vec![("user", Some("alice"))], 10.0 * MB),
                    (vec![("user", Some("alice")), ("client-id", None)], 2.0 * MB),
                ],
                Some((pair.clone(), 2.0 * MB)),
            ),
            (
                "the exact pair beats the user's default client",
                vec![
                    (vec![("user", Some("alice")), ("client-id", None)], 2.0 * MB),
                    (
                        vec![("user", Some("alice")), ("client-id", Some("app1"))],
                        3.0 * MB,
                    ),
                ],
                Some((pair.clone(), 3.0 * MB)),
            ),
            (
                "a default-user quota for the client beats the default pair",
                vec![
                    (vec![("user", None), ("client-id", None)], 4.0 * MB),
                    (vec![("user", None), ("client-id", Some("app1"))], MB),
                ],
                Some((pair.clone(), MB)),
            ),
            (
                "the default user beats a client-id quota",
                vec![
                    (vec![("client-id", Some("app1"))], 5.0 * MB),
                    (vec![("user", None)], 6.0 * MB),
                ],
                Some((alice.clone(), 6.0 * MB)),
            ),
            (
                "a client-id quota beats the default client",
                vec![
                    (vec![("client-id", None)], 7.0 * MB),
                    (vec![("client-id", Some("app1"))], 5.0 * MB),
                ],
                Some((app1.clone(), 5.0 * MB)),
            ),
            (
                "the default client applies when nothing else is configured",
                vec![(vec![("client-id", None)], 7.0 * MB)],
                Some((app1.clone(), 7.0 * MB)),
            ),
            (
                "another user's and another client's quotas never apply",
                vec![
                    (vec![("user", Some("bob"))], 8.0 * MB),
                    (vec![("client-id", Some("app2"))], 9.0 * MB),
                ],
                None,
            ),
        ];
        for (scenario, configured, expected) in rows {
            let img = img_with(
                configured
                    .into_iter()
                    .map(|(e, v)| rec(e, "producer_byte_rate", v))
                    .collect(),
            );
            let got = lookup_quota_with_key(&img, "alice", "app1", "producer_byte_rate");
            assert!(got == expected, "{scenario}");
        }
    }

    /// Distinct per-candidate sentinel values, where the index is the priority.
    /// The value alone then identifies the matched candidate, with no int→float
    /// cast.
    const CAND_VALS: [f64; 8] = [
        1000.0, 1001.0, 1002.0, 1003.0, 1004.0, 1005.0, 1006.0, 1007.0,
    ];

    /// Exhaustive test. Every one of the 2^8 presence configs of the 8
    /// candidates, for a fixed probe, resolves to the present candidate with
    /// the lowest index and returns its value. An empty config returns `None`.
    /// Each candidate has a distinct value (`1000 + i`), so the value
    /// identifies the matched candidate.
    #[test]
    fn quota_precedence_exhaustive() {
        let cands = uc_candidates("u", "c");
        for mask in 0u16..256 {
            let records: Vec<_> = cands
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(i, c)| rec(c.clone(), "k", CAND_VALS[i]))
                .collect();
            let img = img_with(records);
            let got = lookup_quota_with_key(&img, "u", "c", "k");
            match (0..8usize).find(|i| mask & (1 << i) != 0) {
                None => assert!(
                    got.is_none(),
                    "mask {mask:#010b}: expected None, got {got:?}"
                ),
                Some(j) => {
                    let (_key, val) = got.expect("a candidate is present");
                    // Exact sentinel values, stored and retrieved verbatim —
                    // compare bit patterns to sidestep float_cmp.
                    assert!(
                        val.to_bits() == CAND_VALS[j].to_bits(),
                        "mask {mask:#010b}: expected candidate {j} (value {}), got {val}",
                        CAND_VALS[j]
                    );
                }
            }
            // A quota_key no candidate carries never resolves.
            assert!(lookup_quota_with_key(&img, "u", "c", "absent_key").is_none());
        }
    }

    /// Exhaustive test of the 2-priority IP order. Specific beats default.
    #[test]
    fn ip_quota_precedence_exhaustive() {
        let ip: std::net::IpAddr = "10.1.2.3".parse().unwrap();
        let cands = [vec![("ip", Some("10.1.2.3"))], vec![("ip", None)]];
        for mask in 0u8..4 {
            let records: Vec<_> = cands
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(i, c)| rec(c.clone(), "connection_creation_rate", CAND_VALS[i]))
                .collect();
            let img = img_with(records);
            let got = lookup_ip_quota_with_key(&img, ip, "connection_creation_rate");
            match (0..2usize).find(|i| mask & (1 << i) != 0) {
                None => assert!(got.is_none(), "mask {mask:#04b}: expected None"),
                Some(j) => {
                    assert!(got.expect("present").1.to_bits() == CAND_VALS[j].to_bits());
                }
            }
        }
    }

    proptest::proptest! {
        /// Random probes, random candidate subsets, and a non-matching decoy.
        /// The present candidate with the lowest index wins, the decoy is never
        /// returned, and the user/client path never returns an IP entity.
        #[test]
        fn quota_precedence_random(
            principal in "[uv][12]",
            client_id in "[cd][12]",
            qkey_idx in 0usize..2,
            present in proptest::collection::vec(proptest::bool::ANY, 8),
            decoy in proptest::bool::ANY,
        ) {
            let qkey = ["producer_byte_rate", "consumer_byte_rate"][qkey_idx];
            let cands = uc_candidates(&principal, &client_id);
            let mut records: Vec<_> = cands
                .iter()
                .enumerate()
                .filter(|(i, _)| present[*i])
                .map(|(i, c)| rec(c.clone(), qkey, CAND_VALS[i]))
                .collect();
            if decoy {
                // Non-matching entity (never a candidate for a [uv][12]/[cd][12]
                // probe) — must never be returned.
                records.push(rec(
                    vec![("client-id", Some("ZZZ")), ("user", Some("ZZZ"))],
                    qkey,
                    9999.0,
                ));
                records.push(rec(vec![("user", Some("ZZZ"))], qkey, 9998.0));
            }
            let img = img_with(records);
            let got = lookup_quota_with_key(&img, &principal, &client_id, qkey);
            match (0..8usize).find(|i| present[*i]) {
                None => proptest::prop_assert!(got.is_none(), "no candidate present, got {got:?}"),
                Some(j) => {
                    let (_k, v) = got.expect("a candidate is present");
                    proptest::prop_assert_eq!(v.to_bits(), CAND_VALS[j].to_bits());
                }
            }
        }

        /// Random IP precedence. Specific beats default, and the IP path never
        /// returns a user/client entity.
        #[test]
        fn ip_precedence_random(specific in proptest::bool::ANY, default in proptest::bool::ANY) {
            let ip: std::net::IpAddr = "10.9.8.7".parse().unwrap();
            let mut records = vec![];
            if specific {
                records.push(rec(vec![("ip", Some("10.9.8.7"))], "connection_creation_rate", 1.0));
            }
            if default {
                records.push(rec(vec![("ip", None)], "connection_creation_rate", 2.0));
            }
            // A user/client entry must not leak into the IP path.
            records.push(rec(vec![("user", Some("u"))], "connection_creation_rate", 50.0));
            let img = img_with(records);
            let got = lookup_ip_quota_with_key(&img, ip, "connection_creation_rate");
            if specific {
                proptest::prop_assert_eq!(got.expect("specific present").1.to_bits(), 1.0_f64.to_bits());
            } else if default {
                proptest::prop_assert_eq!(got.expect("default present").1.to_bits(), 2.0_f64.to_bits());
            } else {
                proptest::prop_assert!(got.is_none());
            }
        }
    }
}
