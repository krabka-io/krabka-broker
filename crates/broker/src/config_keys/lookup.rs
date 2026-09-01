//! The topic-over-cluster-default config lookup the controller-side resolvers
//! share.
//!
//! Kafka resolves a topic config by asking the topic for an override and
//! falling back to the cluster-wide broker default from the config schema.
//! Several krabka keys need exactly that -- the two unclean-recovery keys and
//! `min.insync.replicas` -- so the walk lives here rather than once per key.

/// The value of `key` for `topic`: the topic override if it has one, else the
/// cluster-wide default broker config, else `None`.
pub(super) fn topic_or_cluster_default<'a>(
    image: &'a krabka_metadata::MetadataImage,
    topic: &str,
    key: &str,
) -> Option<&'a str> {
    image
        .topic_config(topic)
        .and_then(|configs| configs.get(key))
        .or_else(|| image.default_broker_config()?.get(key))
        .map(String::as_str)
}
