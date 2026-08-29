//! The two membership invariants the model asserts after every transition and
//! states again as `always` properties.
//!
//! They are predicates over a real `ClassicGroup`, so the exhaustive search, the
//! stateright properties, and the proptest fuzz all check the same code rather
//! than three restatements of it.

use crate::coordinator::unified::classic_state::ClassicGroup;

/// Every index entry points at a live member that carries the matching instance
/// id, and every static member has a matching index entry. The two mirror each
/// other in both directions.
pub(super) fn index_coherent(g: &ClassicGroup) -> bool {
    for (iid, mid) in &g.static_members {
        match g.members.get(mid) {
            Some(m) if m.group_instance_id.as_deref() == Some(iid.as_str()) => {}
            _ => return false,
        }
    }
    for (mid, m) in &g.members {
        if let Some(iid) = &m.group_instance_id
            && g.static_members.get(iid).map(String::as_str) != Some(mid.as_str())
        {
            return false;
        }
    }
    true
}

/// No two live members share a `group.instance.id`, so nothing bypasses
/// fencing.
pub(super) fn single_owner(g: &ClassicGroup) -> bool {
    let mut seen = std::collections::HashSet::new();
    for m in g.members.values() {
        if let Some(iid) = &m.group_instance_id
            && !seen.insert(iid.clone())
        {
            return false;
        }
    }
    true
}
