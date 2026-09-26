//! KIP-73 throttled replication: the value types and the parser.

pub use krabka_throttle::{ThrottleState, TokenBucket};

mod refresh;
use krabka_metadata::{MetadataImage, NodeId};
pub(crate) use refresh::apply_image;
pub use refresh::run;

/// Topic-level `*.throttled.replicas` config value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThrottledReplicas {
    /// Empty string: no replicas are throttled.
    None,
    /// `"*"` wildcard: all replicas of this topic are throttled.
    All,
    /// `"partition:broker,partition:broker,..."`: specific pairs.
    List(Vec<(i32, NodeId)>),
}

impl ThrottledReplicas {
    /// Parse a `*.throttled.replicas` value with Kafka's
    /// `ThrottledReplicaListValidator` rule. The value is split on commas and
    /// each element is trimmed. The list is the literal `*`, or a list of
    /// `[partitionId]:[brokerId]` pairs of unsigned decimal digits. An empty
    /// element matches Kafka's `([0-9]+:[0-9]+)?` and names no pair.
    ///
    /// # Errors
    /// Returns an error when an element is not an unsigned `partition:broker`
    /// pair, or when a number overflows its type.
    pub fn parse(value: &str) -> Result<Self, String> {
        let elements: Vec<&str> = value
            .split(',')
            .map(|element| element.trim_matches(|c: char| c <= ' '))
            .collect();
        if elements.concat() == "*" {
            return Ok(Self::All);
        }
        let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
        let mut out = Vec::new();
        for element in elements.into_iter().filter(|element| !element.is_empty()) {
            let (p_str, n_str) = element
                .split_once(':')
                .filter(|(p, n)| digits(p) && digits(n))
                .ok_or_else(|| format!("invalid pair {element:?}"))?;
            let p: i32 = p_str.parse().map_err(|e| format!("partition: {e}"))?;
            let n: u64 = n_str.parse().map_err(|e| format!("broker: {e}"))?;
            out.push((p, NodeId(n)));
        }
        if out.is_empty() {
            return Ok(Self::None);
        }
        Ok(Self::List(out))
    }

    #[must_use]
    pub fn contains(&self, partition: i32, node: NodeId) -> bool {
        match self {
            Self::None => false,
            Self::All => true,
            Self::List(v) => v.iter().any(|&(p, n)| p == partition && n == node),
        }
    }
}

/// Both leader-side and follower-side throttled replicas for a topic.
#[derive(Debug, Clone)]
pub struct TopicThrottle {
    pub leader: ThrottledReplicas,
    pub follower: ThrottledReplicas,
}

impl TopicThrottle {
    #[must_use]
    pub fn for_topic(image: &MetadataImage, topic: &str) -> Self {
        let configs = image.topic_config(topic);
        let read = |key: &str| -> ThrottledReplicas {
            configs
                .and_then(|c| c.get(key))
                .and_then(|v| ThrottledReplicas::parse(v).ok())
                .unwrap_or(ThrottledReplicas::None)
        };
        Self {
            leader: read("leader.replication.throttled.replicas"),
            follower: read("follower.replication.throttled.replicas"),
        }
    }
}

pub const LEADER_THROTTLED_REPLICAS_KEY: &str = "leader.replication.throttled.replicas";
pub const FOLLOWER_THROTTLED_REPLICAS_KEY: &str = "follower.replication.throttled.replicas";
pub const LEADER_THROTTLED_RATE_KEY: &str = "leader.replication.throttled.rate";
pub const FOLLOWER_THROTTLED_RATE_KEY: &str = "follower.replication.throttled.rate";
pub const ALTER_LOG_DIRS_THROTTLED_RATE_KEY: &str =
    "replica.alter.log.dirs.io.max.bytes.per.second";

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn empty_string_parses_as_none() {
        assert!(ThrottledReplicas::parse("").unwrap() == ThrottledReplicas::None);
    }

    #[test]
    fn wildcard_parses_as_all() {
        assert!(ThrottledReplicas::parse("*").unwrap() == ThrottledReplicas::All);
    }

    #[test]
    fn single_pair_parses() {
        let r = ThrottledReplicas::parse("0:1").unwrap();
        for (partition, broker, want) in [(0, 1, true), (0, 2, false), (1, 1, false)] {
            assert!(
                r.contains(partition, NodeId(broker)) == want,
                "{partition}:{broker}"
            );
        }
    }

    #[test]
    fn multiple_pairs_parse() {
        let r = ThrottledReplicas::parse("0:1,0:2,1:3").unwrap();
        for (partition, broker, want) in [(0, 1, true), (0, 2, true), (1, 3, true), (1, 1, false)] {
            assert!(
                r.contains(partition, NodeId(broker)) == want,
                "{partition}:{broker}"
            );
        }
    }

    #[test]
    fn malformed_pair_rejected() {
        // Kafka's `([0-9]+:[0-9]+)?` has no sign and no space around the colon.
        for input in ["not-a-pair", "0:x", "x:1", "-1:1", "+0:1", "0 : 1"] {
            assert!(ThrottledReplicas::parse(input).is_err(), "{input}");
        }
    }

    #[test]
    fn kafka_list_spellings_parse_as_kafka_reads_them() {
        let cases = [
            (" ", ThrottledReplicas::None),
            (" * ", ThrottledReplicas::All),
            (
                " 0:1 , 2:3 ",
                ThrottledReplicas::List(vec![(0, NodeId(1)), (2, NodeId(3))]),
            ),
            (
                "0:1,,1:2",
                ThrottledReplicas::List(vec![(0, NodeId(1)), (1, NodeId(2))]),
            ),
        ];
        for (input, want) in cases {
            assert!(ThrottledReplicas::parse(input) == Ok(want), "{input:?}");
        }
    }
}
