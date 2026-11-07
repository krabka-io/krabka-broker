//! The api keys that only krabka speaks, and the floor that keeps them clear
//! of the Apache Kafka assignments.
//!
//! The barrier, topic write-freeze, and break-glass control planes are krabka
//! features with no Kafka counterpart, so each one takes a number from this
//! private range instead of a Kafka one.

use {super::wire_types::ApiKeyCode, krabka_protocol::api_key::ApiKey};

/// Lowest `api_key` in the krabka-private range.
///
/// Apache Kafka assigns api keys upward from 0, so krabka reserves 1000 and
/// above for RPCs that only krabka speaks. A later Kafka assignment cannot
/// reach that far by normal growth. `crates/raft/src/wire.rs` follows the same
/// convention for the controller-only RPCs at 1003 and 1004.
///
/// The broker registers a krabka-private key for dispatch but never advertises
/// it in [`crate::api_catalog::supported_apis`]. An advertised row would print
/// as `UNKNOWN(1010)` in `kafka-broker-api-versions.sh` output, which a real
/// Kafka broker never prints. A client that finds no row negotiates version
/// `(0, 0)`, and every krabka-private api is version 0 only.
pub const KRABKA_PRIVATE_API_KEY_FLOOR: ApiKeyCode = 1000;

/// `AlterBarrierGroups` (1010): creates, updates, and deletes barrier groups.
pub const ALTER_BARRIER_GROUPS_API_KEY: ApiKeyCode = 1010;

/// `DescribeBarrierGroups` (1011): reads back the barrier group definitions.
pub const DESCRIBE_BARRIER_GROUPS_API_KEY: ApiKeyCode = 1011;

/// `TriggerBarrier` (1012): starts one injection for a barrier group.
pub const TRIGGER_BARRIER_API_KEY: ApiKeyCode = 1012;

/// `ListBarrierCuts` (1013): lists the cuts that a barrier group retains.
pub const LIST_BARRIER_CUTS_API_KEY: ApiKeyCode = 1013;

/// `WriteBarrierMarkers` (1014): inter-broker append of markers into the
/// partitions that the receiving broker leads.
pub const WRITE_BARRIER_MARKERS_API_KEY: ApiKeyCode = 1014;

/// `SetTopicFreeze` (1015): sets or clears one topic write-freeze entry.
pub const SET_TOPIC_FREEZE_API_KEY: ApiKeyCode = 1015;

/// `DescribeTopicFreezes` (1016): reads back the topic write-freeze registry.
pub const DESCRIBE_TOPIC_FREEZES_API_KEY: ApiKeyCode = 1016;

/// `ProposeBreakGlass` (1017): opens one break-glass proposal.
pub const PROPOSE_BREAK_GLASS_API_KEY: ApiKeyCode = 1017;

/// `ApproveBreakGlass` (1018): adds one approval to a break-glass proposal.
pub const APPROVE_BREAK_GLASS_API_KEY: ApiKeyCode = 1018;

/// `DescribeBreakGlass` (1019): lists break-glass proposals and their approvals.
pub const DESCRIBE_BREAK_GLASS_API_KEY: ApiKeyCode = 1019;

/// One krabka-private RPC: its wire `api_key` and the name operators see.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KrabkaPrivateApi {
    /// The wire `api_key`.
    pub api_key: ApiKeyCode,
    /// The RPC name, used as its metric label.
    pub name: &'static str,
}

/// Every krabka-private RPC without a generated schema, each with its name.
///
/// This is the one place a key is paired with its name. A new private key
/// takes a row here, and the request metrics then label it by name rather
/// than as `Unknown`.
pub const KRABKA_PRIVATE_APIS: [KrabkaPrivateApi; 10] = [
    KrabkaPrivateApi {
        api_key: ALTER_BARRIER_GROUPS_API_KEY,
        name: "AlterBarrierGroups",
    },
    KrabkaPrivateApi {
        api_key: DESCRIBE_BARRIER_GROUPS_API_KEY,
        name: "DescribeBarrierGroups",
    },
    KrabkaPrivateApi {
        api_key: TRIGGER_BARRIER_API_KEY,
        name: "TriggerBarrier",
    },
    KrabkaPrivateApi {
        api_key: LIST_BARRIER_CUTS_API_KEY,
        name: "ListBarrierCuts",
    },
    KrabkaPrivateApi {
        api_key: WRITE_BARRIER_MARKERS_API_KEY,
        name: "WriteBarrierMarkers",
    },
    KrabkaPrivateApi {
        api_key: SET_TOPIC_FREEZE_API_KEY,
        name: "SetTopicFreeze",
    },
    KrabkaPrivateApi {
        api_key: DESCRIBE_TOPIC_FREEZES_API_KEY,
        name: "DescribeTopicFreezes",
    },
    KrabkaPrivateApi {
        api_key: PROPOSE_BREAK_GLASS_API_KEY,
        name: "ProposeBreakGlass",
    },
    KrabkaPrivateApi {
        api_key: APPROVE_BREAK_GLASS_API_KEY,
        name: "ApproveBreakGlass",
    },
    KrabkaPrivateApi {
        api_key: DESCRIBE_BREAK_GLASS_API_KEY,
        name: "DescribeBreakGlass",
    },
];

/// The name of the krabka-private RPC at `api_key`, if one is registered.
#[must_use]
pub fn krabka_private_api_name(api_key: ApiKeyCode) -> Option<&'static str> {
    KRABKA_PRIVATE_APIS
        .iter()
        .find(|api| api.api_key == api_key)
        .map(|api| api.name)
}

/// The name of the RPC the broker knows at `api_key`, if any.
///
/// A key the generated [`ApiKey`] enum knows resolves to its variant name.
/// That covers every Kafka key and `GetReplicaLogInfo` (1020), the one
/// krabka-private key with a generated schema. Any other key at or above
/// [`KRABKA_PRIVATE_API_KEY_FLOOR`] resolves through
/// [`krabka_private_api_name`]. The request metrics and the request spans
/// both name an api this way, so a dashboard and a trace agree.
#[must_use]
pub fn api_name(api_key: ApiKeyCode) -> Option<&'static str> {
    match ApiKey::from_i16(api_key) {
        Some(known) => Some(known.into()),
        None if api_key >= KRABKA_PRIVATE_API_KEY_FLOOR => krabka_private_api_name(api_key),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// Each krabka-private RPC name and the wire key it must keep. A client
    /// that speaks the RPC relies on the key, and an operator's dashboards on
    /// the name.
    const WANT: [(&str, ApiKeyCode); 10] = [
        ("AlterBarrierGroups", 1010),
        ("DescribeBarrierGroups", 1011),
        ("TriggerBarrier", 1012),
        ("ListBarrierCuts", 1013),
        ("WriteBarrierMarkers", 1014),
        ("SetTopicFreeze", 1015),
        ("DescribeTopicFreezes", 1016),
        ("ProposeBreakGlass", 1017),
        ("ApproveBreakGlass", 1018),
        ("DescribeBreakGlass", 1019),
    ];

    #[test]
    fn krabka_private_apis_keep_their_names_and_wire_keys() {
        let got = KRABKA_PRIVATE_APIS.map(|api| (api.name, api.api_key));
        assert!(got == WANT);
    }

    #[test]
    fn krabka_private_api_keys_sit_in_the_private_range() {
        for KrabkaPrivateApi { api_key, name } in KRABKA_PRIVATE_APIS {
            assert!(api_key >= KRABKA_PRIVATE_API_KEY_FLOOR, "{name}");
        }
    }

    #[test]
    fn krabka_private_api_keys_are_pairwise_distinct() {
        for (index, left) in KRABKA_PRIVATE_APIS.iter().enumerate() {
            for right in &KRABKA_PRIVATE_APIS[index + 1..] {
                assert!(
                    left.api_key != right.api_key,
                    "{} and {}",
                    left.name,
                    right.name
                );
            }
        }
    }

    #[test]
    fn krabka_private_api_name_finds_registered_keys_only() {
        assert!(krabka_private_api_name(SET_TOPIC_FREEZE_API_KEY) == Some("SetTopicFreeze"));
        assert!(krabka_private_api_name(KRABKA_PRIVATE_API_KEY_FLOOR).is_none());
    }
}
