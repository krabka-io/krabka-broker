//! The topic-scoped config keys that only the controller writes: KFC-9's
//! synthesised write-freeze key, KIP-966's eligible-leader-replica state, and
//! the refusal that both alter paths give for them. This is the topic-side
//! counterpart of [`super::broker_scope`].

/// KFC-9: the write-freeze state of one topic.
///
/// The broker synthesises this key. It is never stored in `V1TopicConfig`,
/// and it never reaches the topic-config record in the metadata log. The
/// freeze itself lives in the metadata log as
/// [`krabka_metadata::TopicFreezeRecord`], so no snapshot and no restore can
/// bring back a stale freeze through a topic config.
///
/// The key is controller-managed and read-only. `DescribeConfigs` reports it
/// with `read_only` set, and both `AlterConfigs` and
/// `IncrementalAlterConfigs` refuse it with `INVALID_CONFIG`. The
/// krabka-private `SetTopicFreeze` API (key 1015) and the `krabka-guard` CLI
/// are the one path that sets and clears it.
///
/// An operator who holds only the JVM tools reads the freeze with
/// `kafka-configs --entity-type topics --describe`.
pub(crate) const WRITE_FREEZE: &str = "write.freeze";

/// KIP-966: the eligible-leader-replica state of one topic's partitions.
///
/// `PartitionRecord` lives in the protocol crate and carries no ELR field, so
/// krabka publishes the state as a topic config, exactly as it publishes
/// broker fencing as [`super::broker_scope::BROKER_FENCED`]. The value holds
/// every partition that has ELR state;
/// [`TopicElr::parse`](crate::elr::state::TopicElr::parse) documents the
/// grammar, and [`crate::elr::maintain`] is the only writer.
///
/// The name is krabka-private, like [`super::DISKLESS`], because Kafka has no
/// topic config for this: `eligible.leader.replicas.version` is a cluster
/// feature flag, not a per-topic override, so an operator cannot confuse the
/// two. krabka registers that feature as well, and the controller keeps this
/// key only while it is finalized at 1; a downgrade to 0 drops every value
/// the feature published, as Kafka's controller clears its own ELR state.
///
/// The key is controller-managed and read-only. `DescribeConfigs` reports it
/// with `read_only` set, and both `AlterConfigs` and `IncrementalAlterConfigs`
/// refuse it with `INVALID_CONFIG`; the ISR transitions that move a replica
/// in and out of ELR are the only writer.
pub(crate) const ELIGIBLE_LEADER_REPLICAS: &str = "krabka.elr";

/// Topic-scoped config keys that only the controller writes. This is the
/// topic-side analogue of [`super::broker_scope::CONTROLLER_MANAGED_BROKER_CONFIGS`]:
/// `AlterConfigs` and `IncrementalAlterConfigs` must reject every key in this
/// list, and `DescribeConfigs` must report each one as read-only.
pub(crate) const CONTROLLER_MANAGED_TOPIC_CONFIGS: [&str; 2] =
    [ELIGIBLE_LEADER_REPLICAS, WRITE_FREEZE];

/// `true` when `key` is a topic config that only the controller writes.
pub(crate) fn is_controller_managed_topic_config(key: &str) -> bool {
    CONTROLLER_MANAGED_TOPIC_CONFIGS.contains(&key)
}

/// The refusal both alter paths give for a controller-managed topic config.
///
/// Both handlers build the message here, so an operator reads one wording
/// from `AlterConfigs` and from `IncrementalAlterConfigs`. The message names
/// what does change the key. A refusal that names nothing leaves the operator
/// with no next step.
pub(crate) fn controller_managed_topic_config_message(key: &str) -> String {
    let remedy = if key == ELIGIBLE_LEADER_REPLICAS {
        "the controller publishes it as replicas enter and leave the ISR; \
         read it with `kafka-topics --describe`"
    } else {
        "use `krabka-guard freeze set` to set it and `krabka-guard freeze clear` to clear it"
    };
    format!("topic config {key} is controller-managed and read-only; {remedy}")
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{
        super::{
            RETENTION_MS,
            broker_scope::BROKER_WITNESS,
            validation::{is_recognized, validate_topic_config},
        },
        *,
    };

    #[test]
    fn a_controller_managed_topic_config_is_named_as_one() {
        for (label, key, expected) in [
            ("the write-freeze key", WRITE_FREEZE, true),
            ("the ELR key", ELIGIBLE_LEADER_REPLICAS, true),
            ("an ordinary topic key", RETENTION_MS, false),
            (
                "a broker-scoped controller-managed key",
                BROKER_WITNESS,
                false,
            ),
            ("an unknown key", "not.a.topic.config", false),
            ("an empty key", "", false),
        ] {
            check!(
                is_controller_managed_topic_config(key) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn a_controller_managed_key_stays_outside_the_stored_whitelist() {
        // Both are written by the controller alone, so the validator must not
        // accept either as an ordinary override an alter path could set.
        for key in CONTROLLER_MANAGED_TOPIC_CONFIGS {
            check!(!is_recognized(key), "{key}");
            check!(validate_topic_config(key, "true").is_err(), "{key}");
        }
    }

    #[test]
    fn the_refusal_names_the_key_and_the_commands_that_change_it() {
        let message = controller_managed_topic_config_message(WRITE_FREEZE);

        check!(message.contains(WRITE_FREEZE), "got: {message}");
        check!(
            message.contains("krabka-guard freeze set"),
            "got: {message}"
        );
        check!(
            message.contains("krabka-guard freeze clear"),
            "got: {message}"
        );
    }

    /// The ELR key has no operator-facing setter, so its refusal must not
    /// send one to `krabka-guard freeze`, which would not touch it.
    #[test]
    fn the_elr_refusal_points_at_the_controller_and_not_at_the_freeze_cli() {
        let message = controller_managed_topic_config_message(ELIGIBLE_LEADER_REPLICAS);

        check!(message.contains(ELIGIBLE_LEADER_REPLICAS), "got: {message}");
        check!(!message.contains("krabka-guard"), "got: {message}");
        check!(
            message.contains("kafka-topics --describe"),
            "got: {message}"
        );
    }
}
