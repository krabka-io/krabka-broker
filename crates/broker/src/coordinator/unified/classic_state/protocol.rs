//! The classic-protocol embedded-protocol vote.
//!
//! `select_protocol` decides which assignor name a rebalance round settles on,
//! from the `protocols` list every member offered in its `JoinGroup`.

use std::collections::HashMap;

use super::member::Member;

/// Picks the protocol name with the most first-place votes, among the names
/// that every member proposed. It breaks a tie lexicographically. It returns
/// `None` when the intersection is empty, and when there are no members.
#[must_use]
pub fn select_protocol(members: &HashMap<String, Member>) -> Option<String> {
    if members.is_empty() {
        return None;
    }
    let mut iter = members.values();
    let first = iter.next()?;
    let mut intersection: std::collections::HashSet<String> =
        first.protocols.iter().map(|(n, _)| n.clone()).collect();
    for m in iter {
        let names: std::collections::HashSet<String> =
            m.protocols.iter().map(|(n, _)| n.clone()).collect();
        intersection = intersection.intersection(&names).cloned().collect();
    }
    if intersection.is_empty() {
        return None;
    }
    let mut votes: HashMap<&str, usize> = HashMap::new();
    for m in members.values() {
        if let Some((name, _)) = m.protocols.first()
            && intersection.contains(name)
        {
            *votes.entry(name.as_str()).or_insert(0) += 1;
        }
    }
    intersection
        .iter()
        .max_by(|a, b| {
            let va = votes.get(a.as_str()).copied().unwrap_or(0);
            let vb = votes.get(b.as_str()).copied().unwrap_or(0);
            va.cmp(&vb).then_with(|| b.cmp(a))
        })
        .cloned()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::classic_state::test_support::member_with_protocols;

    #[test]
    fn select_protocol_single_member_picks_first() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            member_with_protocols("m1", vec![("range", b""), ("cooperative_sticky", b"")]),
        );
        assert!(select_protocol(&members).as_deref() == Some("range"));
    }

    #[test]
    fn select_protocol_intersection_empty_returns_none() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            member_with_protocols("m1", vec![("range", b"")]),
        );
        members.insert(
            "m2".to_string(),
            member_with_protocols("m2", vec![("cooperative_sticky", b"")]),
        );
        assert!(select_protocol(&members) == None);
    }

    #[test]
    fn select_protocol_max_votes_wins() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            member_with_protocols("m1", vec![("range", b""), ("cooperative_sticky", b"")]),
        );
        members.insert(
            "m2".to_string(),
            member_with_protocols("m2", vec![("range", b""), ("cooperative_sticky", b"")]),
        );
        members.insert(
            "m3".to_string(),
            member_with_protocols("m3", vec![("cooperative_sticky", b""), ("range", b"")]),
        );
        assert!(select_protocol(&members).as_deref() == Some("range"));
    }

    #[test]
    fn select_protocol_tie_breaks_lexicographically() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            member_with_protocols("m1", vec![("range", b""), ("cooperative_sticky", b"")]),
        );
        members.insert(
            "m2".to_string(),
            member_with_protocols("m2", vec![("cooperative_sticky", b""), ("range", b"")]),
        );
        assert!(select_protocol(&members).as_deref() == Some("cooperative_sticky"));
    }

    #[test]
    fn select_protocol_empty_members_returns_none() {
        let members = HashMap::new();
        assert!(select_protocol(&members) == None);
    }
}
