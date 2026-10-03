use super::*;

pub(super) fn id_of(tag: u8) -> WireUuid {
    WireUuid([tag; 16])
}

fn ref_name(r: Ref) -> String {
    match r {
        Ref::Both(n, _) | Ref::NameOnly(n) => n.to_string(),
        Ref::IdOnly(_) => String::new(),
    }
}

fn ref_id(r: Ref) -> WireUuid {
    match r {
        Ref::Both(_, t) | Ref::IdOnly(t) => id_of(t),
        Ref::NameOnly(_) => WireUuid::ZERO,
    }
}

/// Mirror of the merge find-predicate: does cached key `k` match reference `r`
/// for partition `p`? A match needs an equal non-empty name or an equal
/// non-zero id.
pub(super) fn ref_matches(k: &FetchSessionKey, r: Ref, p: i32) -> bool {
    let name = ref_name(r);
    let id = ref_id(r);
    k.partition == p
        && ((!name.is_empty() && k.topic_name == name)
            || (id != WireUuid::ZERO && k.topic_id == id))
}

pub(super) fn forgotten_topic(r: Ref, p: i32) -> ForgottenTopic {
    ForgottenTopic {
        topic: ref_name(r),
        topic_id: ref_id(r),
        partitions: vec![p],
        ..Default::default()
    }
}

pub(super) fn fetch_topic(r: Ref, p: i32, mb: i32) -> FetchTopic {
    FetchTopic {
        topic: ref_name(r),
        topic_id: ref_id(r),
        partitions: vec![FetchPartition {
            partition: p,
            partition_max_bytes: mb,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// No two distinct keys refer to one logical partition: same partition AND
/// (both names non-empty & equal) OR (both ids non-zero & equal).
pub(super) fn no_shadow(partitions: &HashMap<FetchSessionKey, CachedPartitionState>) -> bool {
    let keys: Vec<&FetchSessionKey> = partitions.keys().collect();
    for (i, a) in keys.iter().enumerate() {
        for b in &keys[i + 1..] {
            if a.partition == b.partition
                && ((!a.topic_name.is_empty() && a.topic_name == b.topic_name)
                    || (a.topic_id != WireUuid::ZERO && a.topic_id == b.topic_id))
            {
                return false;
            }
        }
    }
    true
}
