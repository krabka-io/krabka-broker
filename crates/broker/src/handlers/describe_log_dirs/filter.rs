//! The topic and partition filter that a `DescribeLogDirs` request carries.
//!
//! The request's `topics` field is optional, and an empty partition list for a
//! named topic means every partition of that topic. This module turns that wire
//! shape into a single predicate, so the directory scan in the parent module
//! stays about directories.

use std::collections::BTreeMap;

use krabka_protocol::owned::describe_log_dirs_request::DescribeLogDirsRequest;

/// Filter that the handler derives from the request `topics` field.
///
/// - `None`  → report every partition. This is the admin-client default.
/// - `Some`  → report only the listed topics. An empty partition list for a
///   topic means "all partitions of that topic".
pub(super) enum Filter {
    All,
    Topics(BTreeMap<String, Vec<i32>>),
}

impl Filter {
    pub(super) fn allows(&self, topic: &str, partition: i32) -> bool {
        match self {
            Filter::All => true,
            Filter::Topics(map) => match map.get(topic) {
                None => false,
                Some(parts) => parts.is_empty() || parts.contains(&partition),
            },
        }
    }
}

pub(super) fn request_filter(req: DescribeLogDirsRequest) -> Filter {
    req.topics.map_or(Filter::All, |topics| {
        Filter::Topics(
            topics
                .into_iter()
                .map(|topic| (topic.topic, topic.partitions))
                .collect(),
        )
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn filter_all_allows_everything() {
        let f = Filter::All;
        assert!(f.allows("any", 0));
        assert!(f.allows("other", 99));
    }

    #[test]
    fn filter_topics_respects_partition_list() {
        let mut m = BTreeMap::new();
        m.insert("t".to_string(), vec![0, 2]);
        let f = Filter::Topics(m);
        for (topic, partition, want) in [
            ("t", 0, true),
            ("t", 1, false),
            ("t", 2, true),
            ("other", 0, false),
        ] {
            assert!(f.allows(topic, partition) == want, "{topic}-{partition}");
        }
    }

    #[test]
    fn filter_topics_empty_partition_list_means_all() {
        let mut m = BTreeMap::new();
        m.insert("t".to_string(), vec![]);
        let f = Filter::Topics(m);
        for (topic, partition, want) in [("t", 0, true), ("t", 7, true), ("u", 0, false)] {
            assert!(f.allows(topic, partition) == want, "{topic}-{partition}");
        }
    }
}
