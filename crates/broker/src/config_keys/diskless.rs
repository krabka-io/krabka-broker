//! krabka's diskless opt-in topic key, and the refusal both alter paths give
//! for it. It is the topic-side counterpart of [`super::topic_scope`]: that
//! module holds the keys only the controller writes, this one holds the keys
//! only `CreateTopics` writes.

/// Route this topic's writes through the diskless WAL quorum and the
/// object-store tier instead of the ordinary replicated log.
///
/// A partition reads the flag once, when its runtime is opened, and wires the
/// WAL store, the controller offset sequencer, and the hot-tail cache from it.
/// Nothing re-reads it afterwards, so the flag is **create-only**: neither
/// alter path may change it, because a stored change the running partitions
/// ignore is worse than a refusal. Recreate the topic to change it.
pub(crate) const DISKLESS: &str = "krabka.diskless";

/// The value a diskless topic config takes when the topic stores no override:
/// [`crate::broker::diskless_topic_config`] treats an absent key and an
/// explicit `false` alike.
const DISKLESS_DEFAULT: &str = "false";

/// Topic-scoped config keys that only `CreateTopics` writes, each beside the
/// value it has when the topic stores no override.
///
/// The rule both alter paths enforce is that the *effective* value cannot
/// change, not that the key cannot be named. `AlterConfigs` sends a topic's
/// complete override map, so an operator who changes `retention.ms` on a
/// diskless topic has to restate `krabka.diskless=true` in the same request --
/// and a `kafka-configs --describe` round-trip does exactly that. Refusing
/// that request would leave no way to alter any config on a diskless topic,
/// and dropping the key from the replacement would silently un-diskless the
/// topic and tear its WAL placement down on the next reconcile. So a restated
/// value is accepted, an omitted one is carried forward, and only a real
/// change is refused.
///
/// `DescribeConfigs` reports every key here as read-only.
pub(crate) const CREATE_ONLY_TOPIC_CONFIGS: [(&str, &str); 1] = [(DISKLESS, DISKLESS_DEFAULT)];

/// `true` when `key` is a topic config that only `CreateTopics` writes.
pub(crate) fn is_create_only_topic_config(key: &str) -> bool {
    CREATE_ONLY_TOPIC_CONFIGS
        .iter()
        .any(|(name, _)| *name == key)
}

/// The create-only key whose *effective* value differs between the map a
/// topic stores now and the map an alter path proposes, if any.
///
/// Absence is not distinguishable from the default here, which is the point:
/// an `IncrementalAlterConfigs` DELETE of a live `krabka.diskless=true`
/// leaves the merged map without the key, and that is a change from `true` to
/// `false` as surely as a SET would be.
pub(crate) fn create_only_topic_config_change(
    proposed: &std::collections::BTreeMap<String, String>,
    current: Option<&std::collections::BTreeMap<String, String>>,
) -> Option<&'static str> {
    CREATE_ONLY_TOPIC_CONFIGS
        .into_iter()
        .find_map(|(key, default)| {
            let before = current
                .and_then(|current| current.get(key))
                .map_or(default, String::as_str);
            let after = proposed.get(key).map_or(default, String::as_str);
            (before != after).then_some(key)
        })
}

/// Carry the topic's create-only overrides into a *whole-map replacement*
/// that left them out, then check that the result changes none of them.
///
/// Only `AlterConfigs` needs this. Its request is the topic's complete
/// override map, so a key the operator never mentioned is absent for the same
/// reason a key they deliberately cleared is, and the two cannot be told
/// apart. Preserving is the only reading that does not silently un-diskless a
/// live topic. `IncrementalAlterConfigs` has no such ambiguity -- its merged
/// map starts from the stored one, so an unmentioned key is already carried
/// forward and a mentioned one is an explicit op -- and it calls
/// [`create_only_topic_config_change`] directly.
pub(crate) fn preserve_create_only_topic_configs(
    proposed: &mut std::collections::BTreeMap<String, String>,
    current: Option<&std::collections::BTreeMap<String, String>>,
) -> Result<(), &'static str> {
    for (key, default) in CREATE_ONLY_TOPIC_CONFIGS {
        if proposed.contains_key(key) {
            continue;
        }
        // Nothing is written back for a topic already at the default: an
        // explicit `false` beside no `false` is the same topic, and the map
        // stays as small as `CreateTopics` left it.
        if let Some(stored) = current
            .and_then(|current| current.get(key))
            .filter(|stored| stored.as_str() != default)
        {
            proposed.insert(key.to_owned(), stored.clone());
        }
    }
    create_only_topic_config_change(proposed, current).map_or(Ok(()), Err)
}

/// The refusal both alter paths give for an attempt to *change* a create-only
/// topic config. The message names the one way an operator can change the
/// setting, because a refusal that names no next step leaves them stuck.
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
