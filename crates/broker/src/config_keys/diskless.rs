//! krabka's diskless opt-in topic key, and the refusal both alter paths give
//! for it. It is the topic-side counterpart of [`super::topic_scope`]: that
//! module holds the keys only the controller writes, this one holds the keys
//! only `CreateTopics` writes.

/// Route this topic's writes through the diskless WAL quorum and the
/// object-store tier instead of the ordinary replicated log.
///
/// A partition reads the flag once, when its runtime is opened, and wires the
/// WAL store, the controller offset sequencer, and the hot-tail cache from it.
/// Nothing re-reads it afterwards, so the flag is **create-only**: both
/// `AlterConfigs` and `IncrementalAlterConfigs` refuse it with
/// `INVALID_CONFIG` rather than store a value the running partitions would
/// ignore. Recreate the topic to change it.
pub(crate) const DISKLESS: &str = "krabka.diskless";

/// Topic-scoped config keys that only `CreateTopics` writes. Both alter paths
/// must reject every key in this list, and `DescribeConfigs` must report each
/// one as read-only.
pub(crate) const CREATE_ONLY_TOPIC_CONFIGS: [&str; 1] = [DISKLESS];

/// `true` when `key` is a topic config that only `CreateTopics` writes.
pub(crate) fn is_create_only_topic_config(key: &str) -> bool {
    CREATE_ONLY_TOPIC_CONFIGS.contains(&key)
}

/// The refusal both alter paths give for a create-only topic config. The
/// message names the one way an operator can change the setting, because a
/// refusal that names no next step leaves them stuck.
pub(crate) fn create_only_topic_config_message(key: &str) -> String {
    format!(
        "topic config {key} is fixed at topic creation and cannot be altered; \
         a partition reads it once when it opens, so a later change would not \
         take effect -- recreate the topic with the new value instead"
    )
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{
        super::{
            RETENTION_MS,
            topic_scope::WRITE_FREEZE,
            validation::{is_recognized, validate_topic_config},
        },
        *,
    };

    #[test]
    fn a_create_only_topic_config_is_named_as_one() {
        for (label, key, expected) in [
            ("the diskless key", DISKLESS, true),
            ("an ordinary topic key", RETENTION_MS, false),
            ("a controller-managed key", WRITE_FREEZE, false),
            ("an unknown key", "flush.ms", false),
            ("an empty key", "", false),
        ] {
            check!(is_create_only_topic_config(key) == expected, "{label}");
        }
    }

    #[test]
    fn the_diskless_key_is_stored_so_it_stays_inside_the_whitelist() {
        // Unlike the write-freeze key, `krabka.diskless` IS stored in the
        // topic's override map: `CreateTopics` validates it through the same
        // whitelist every other override goes through.
        check!(is_recognized(DISKLESS));
        check!(validate_topic_config(DISKLESS, "true").is_ok());
        check!(validate_topic_config(DISKLESS, "false").is_ok());
        check!(validate_topic_config(DISKLESS, "yes").is_err());
    }

    #[test]
    fn the_refusal_names_the_key_and_the_way_to_change_it() {
        let message = create_only_topic_config_message(DISKLESS);

        check!(message.contains(DISKLESS), "got: {message}");
        check!(message.contains("recreate the topic"), "got: {message}");
    }
}
