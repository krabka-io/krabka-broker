//! The classic-protocol embedded-protocol vote and the protocol support check.
//!
//! `select_protocol` decides which assignor name a rebalance round settles on,
//! from the `protocols` list every member offered in its `JoinGroup`.
//! [`ClassicGroup::supports_protocols`] is the gate a joining member passes
//! before the group admits it, so the vote always has a candidate.

use {
    super::{
        group::{ClassicGroup, GroupState},
        member::Member,
    },
    std::collections::{HashMap, HashSet},
};

/// The protocol names every member proposed, Kafka's
/// `ClassicGroup.candidateProtocols`.
fn candidate_protocols(members: &HashMap<String, Member>) -> HashSet<&str> {
    let mut support: HashMap<&str, usize> = HashMap::new();
    for member in members.values() {
        let names: HashSet<&str> = member.protocols.iter().map(|(n, _)| n.as_str()).collect();
        for name in names {
            *support.entry(name).or_insert(0) += 1;
        }
    }
    support
        .into_iter()
        .filter(|&(_, count)| count == members.len())
        .map(|(name, _)| name)
        .collect()
}

/// Kafka's `ClassicGroup.selectProtocol`. Each member votes for its most
/// preferred protocol among the names every member proposed
/// (`ClassicGroupMember.vote`), and the name with the most votes wins. Kafka
/// breaks a tie by hash-map order; this picks the lexicographically smallest
/// name, so the choice is deterministic. It returns `None` when the
/// intersection is empty, and when there are no members.
#[must_use]
pub fn select_protocol(members: &HashMap<String, Member>) -> Option<String> {
    let candidates = candidate_protocols(members);
    let mut votes: HashMap<&str, usize> = HashMap::new();
    for member in members.values() {
        if let Some((name, _)) = member
            .protocols
            .iter()
            .find(|(name, _)| candidates.contains(name.as_str()))
        {
            *votes.entry(name.as_str()).or_insert(0) += 1;
        }
    }
    votes
        .into_iter()
        .max_by(|(a, va), (b, vb)| va.cmp(vb).then_with(|| b.cmp(a)))
        .map(|(name, _)| name.to_owned())
}

impl ClassicGroup {
    /// Kafka's `ClassicGroup.supportsProtocols`. An `Empty` group accepts any
    /// non-empty protocol type with at least one protocol. A group with
    /// members needs the same protocol type and one protocol name that every
    /// current member supports.
    #[must_use]
    pub fn supports_protocols<'a>(
        &self,
        protocol_type: &str,
        mut protocol_names: impl Iterator<Item = &'a str>,
    ) -> bool {
        if self.state == GroupState::Empty {
            return !protocol_type.is_empty() && protocol_names.next().is_some();
        }
        if self.protocol_type.as_deref() != Some(protocol_type) {
            return false;
        }
        // With no members every name has the full support count of zero,
        // exactly as Kafka's `supportedProtocols` count compares.
        let candidates = candidate_protocols(&self.members);
        protocol_names.any(|name| self.members.is_empty() || candidates.contains(name))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::classic_state::test_support::member_with_protocols,
        krabka_bdd::Given,
        std::{convert::Infallible, str::FromStr},
    };

    /// #788: Kafka's `ClassicGroup.supportsProtocols`.
    fn empty_protocol_group() -> ClassicGroup { ClassicGroup::new("g") }

    fn stable_protocol_group() -> ClassicGroup {
        let mut stable = ClassicGroup::new("g");
        stable.add_member(member_with_protocols("m1", vec![
            ("range", b""),
            ("sticky", b""),
        ]));
        stable.add_member(member_with_protocols("m2", vec![("sticky", b"")]));
        stable.protocol_type = Some("consumer".into());
        stable.state = GroupState::Stable;
        stable
    }

    use rstest_bdd_macros::{given, then, when};
    #[derive(Default)]
    struct ProtocolNegotiation {
        group: Option<ClassicGroup>,
        supported: Option<bool>,
    }
    #[rstest::fixture]
    fn negotiation() -> ProtocolNegotiation {
        ProtocolNegotiation {
            ..ProtocolNegotiation::default()
        }
    }
    #[given("the classic group is {state} with existing consumer protocol support")]
    fn classic_protocol_group(negotiation: &mut ProtocolNegotiation, state: String) {
        negotiation.group = Some(match state.as_str() {
            "empty" => empty_protocol_group(),
            "stable" => stable_protocol_group(),
            _ => panic!("unknown classic group state"),
        });
    }
    #[when("a joining member offers protocol type {kind} with preference list {names}")]
    fn negotiate_protocol(negotiation: &mut ProtocolNegotiation, kind: String, names: String) {
        let kind = if kind == "empty" { "" } else { &kind };
        let names = if names == "none" {
            vec![]
        } else {
            names.split(',').collect::<Vec<_>>()
        };
        negotiation.supported = Some(
            negotiation
                .group
                .as_ref()
                .expect("group prepared")
                .supports_protocols(kind, names.into_iter()),
        );
    }
    #[then("the offered protocols are compatible {compatible}")]
    fn compatible_protocols(negotiation: &ProtocolNegotiation, compatible: bool) {
        assert2::assert!(negotiation.supported == Some(compatible));
    }

    /// Each member's protocol names in its order of preference, as a scenario
    /// writes them: members apart by ` / `, names by `,`, and `none` for a
    /// group with no members.
    struct ProtocolPreferences(Vec<Vec<String>>);

    impl FromStr for ProtocolPreferences {
        type Err = Infallible;

        fn from_str(text: &str) -> Result<Self, Self::Err> {
            if text == "none" {
                return Ok(Self(Vec::new()));
            }
            Ok(Self(
                text.split(" / ")
                    .map(|names| names.split(',').map(str::to_owned).collect())
                    .collect(),
            ))
        }
    }

    /// The members of a group and the protocol the vote picks for it.
    struct ProtocolVote {
        members: Given<HashMap<String, Member>>,
        selected: Given<Option<String>>,
    }

    #[rstest::fixture]
    fn protocol_vote() -> ProtocolVote {
        ProtocolVote {
            members: Given::new(
                "members",
                "Given the members of a classic group prefer the protocols <preferences>",
            ),
            selected: Given::new(
                "selected",
                "When the coordinator selects the group protocol",
            ),
        }
    }

    #[krabka_bdd::given("the members of a classic group prefer the protocols {preferences}")]
    fn members_prefer_protocols(
        protocol_vote: &mut ProtocolVote,
        preferences: ProtocolPreferences,
    ) {
        let members = preferences
            .0
            .iter()
            .enumerate()
            .map(|(index, names)| {
                let id = format!("m{index}");
                let protocols = names.iter().map(|name| (name.as_str(), &b""[..])).collect();
                (id.clone(), member_with_protocols(&id, protocols))
            })
            .collect();
        protocol_vote.members.set(members);
    }

    #[krabka_bdd::when("the coordinator selects the group protocol")]
    fn select_group_protocol(protocol_vote: &mut ProtocolVote) {
        let selected = select_protocol(protocol_vote.members.get());
        protocol_vote.selected.set(selected);
    }

    #[krabka_bdd::then("the group protocol is {protocol}")]
    fn group_protocol_is(protocol_vote: &ProtocolVote, protocol: String) {
        assert2::assert!(*protocol_vote.selected.get() == Some(protocol));
    }

    #[krabka_bdd::then("the group has no protocol")]
    fn group_has_no_protocol(protocol_vote: &ProtocolVote) {
        assert2::assert!(*protocol_vote.selected.get() == None);
    }

    krabka_bdd::scenarios!(
        "tests/features/migrated/src/coordinator/unified/classic_state/protocol.feature",
        fixtures = [negotiation: ProtocolNegotiation, protocol_vote: ProtocolVote]
    );
}
