//! The topic-scoped config keys that only the controller writes: KFC-9's
//! synthesised write-freeze key, and the refusal that both alter paths give
//! for it. This is the topic-side counterpart of [`super::broker_scope`].

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

/// Topic-scoped config keys that only the controller writes. This is the
/// topic-side analogue of [`super::broker_scope::CONTROLLER_MANAGED_BROKER_CONFIGS`]:
/// `AlterConfigs` and `IncrementalAlterConfigs` must reject every key in this
/// list, and `DescribeConfigs` must report each one as read-only.
pub(crate) const CONTROLLER_MANAGED_TOPIC_CONFIGS: [&str; 1] = [WRITE_FREEZE];

/// `true` when `key` is a topic config that only the controller writes.
pub(crate) fn is_controller_managed_topic_config(key: &str) -> bool {
    CONTROLLER_MANAGED_TOPIC_CONFIGS.contains(&key)
}

/// The refusal both alter paths give for a controller-managed topic config.
///
/// Both handlers build the message here, so an operator reads one wording
/// from `AlterConfigs` and from `IncrementalAlterConfigs`. The message names
/// the commands that change the key. A refusal that names no command leaves
/// the operator with no next step.
pub(crate) fn controller_managed_topic_config_message(key: &str) -> String {
    format!(
        "topic config {key} is controller-managed and read-only; \
         use `krabka-guard freeze set` to set it and `krabka-guard freeze clear` to clear it"
    )
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
            ("an ordinary topic key", RETENTION_MS, false),
            (
                "a broker-scoped controller-managed key",
                BROKER_WITNESS,
                false,
            ),
            ("an unknown key", "flush.ms", false),
            ("an empty key", "", false),
        ] {
            check!(
                is_controller_managed_topic_config(key) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn the_write_freeze_key_stays_outside_the_stored_whitelist() {
        // It is synthesised for `DescribeConfigs` and never stored, so the
        // validator must not accept it as an ordinary override.
        check!(!is_recognized(WRITE_FREEZE));
        check!(validate_topic_config(WRITE_FREEZE, "true").is_err());
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
}
