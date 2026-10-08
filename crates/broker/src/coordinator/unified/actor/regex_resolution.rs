//! The resolution of the regular expressions that a consumer group's members
//! subscribe to: Kafka's `GroupMetadataManager.maybeUpdateRegularExpressions`
//! and `handleRegularExpressionsResult`.
//!
//! A member that subscribes to a regular expression gets the topics that the
//! group resolved the pattern to (see `consumer_state::regex`). The group
//! resolves a pattern when a member brings a new one, and refreshes every
//! resolution when a heartbeat finds them stale. Kafka runs the resolution as
//! an asynchronous task of the coordinator and writes its outcome when it
//! completes. Here the heartbeat resolves in line, so the member that brought
//! a pattern is assigned the topics in the same response. The requesting
//! member's principal decides which matches the group keeps, as in Kafka.
//!
//! Each resolution is written as a `ConsumerGroupRegularExpression` record,
//! and a record is tombstoned when its last subscriber goes, so a coordinator
//! failover restores what every pattern resolved to.

use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};

use crate::{
    coordinator::unified::{
        config::NextGenConfig,
        consumer_state::{GroupState, ResolvedRegularExpression},
        persistence_next_gen::RegularExpressionValue,
        regex_resolver::TopicRegexResolver,
    },
    time_util::duration_millis,
};

/// What a heartbeat brings to resolve a group's regular expressions.
#[derive(Clone, Copy)]
pub(crate) struct RegexResolution<'a> {
    /// Resolves patterns with the principal of the heartbeat.
    pub resolver: &'a dyn TopicRegexResolver,
    /// Kafka's `lastMetadataImageWithNewTopics`: the metadata version of the
    /// latest image that can change what a pattern resolves to. A group whose
    /// resolutions are older than it resolves them again.
    pub refresh_version: i64,
    /// The wall-clock time of the heartbeat, in milliseconds since the epoch.
    pub now_ms: i64,
    /// Kafka's `group.consumer.regex.refresh.interval.ms`.
    pub refresh_interval: Duration,
    /// Kafka's `REGEX_BATCH_REFRESH_MIN_INTERVAL_MS`: the regular expressions
    /// of a group are not resolved again within this time of their last
    /// resolution, whatever asks for it.
    pub min_refresh_interval: Duration,
}

impl<'a> RegexResolution<'a> {
    /// The resolution that a heartbeat of `now_ms` gets under `config`, with
    /// `resolver` and the latest `refresh_version` of the coordinator.
    pub(crate) fn of(
        config: &NextGenConfig,
        resolver: &'a dyn TopicRegexResolver,
        refresh_version: i64,
        now_ms: i64,
    ) -> Self {
        Self {
            resolver,
            refresh_version,
            now_ms,
            refresh_interval: config.regex_refresh_interval,
            min_refresh_interval: config.regex_refresh_min_interval,
        }
    }
}

#[cfg(test)]
impl RegexResolution<'static> {
    /// A resolution that finds no topic for any pattern and never refreshes,
    /// for the tests that do not exercise regular expressions.
    pub(crate) fn none() -> Self {
        Self {
            resolver: &crate::coordinator::unified::regex_resolver::NoTopicRegexResolver,
            refresh_version: -1,
            now_ms: 0,
            refresh_interval: Duration::from_mins(10),
            min_refresh_interval: Duration::from_secs(10),
        }
    }
}

#[cfg(test)]
impl<'a> RegexResolution<'a> {
    /// The time that [`Self::with`] puts a heartbeat at.
    pub(crate) const TEST_NOW_MS: i64 = crate::coordinator::unified::regex_resolver::TEST_NOW_MS;

    /// A resolution by `resolver` at [`Self::TEST_NOW_MS`], with no image
    /// change to refresh for and the default refresh interval.
    pub(crate) fn with(resolver: &'a dyn TopicRegexResolver) -> Self {
        Self {
            resolver,
            refresh_version: -1,
            now_ms: Self::TEST_NOW_MS,
            refresh_interval: Duration::from_mins(10),
            min_refresh_interval: Duration::from_secs(10),
        }
    }
}

/// A `ConsumerGroupRegularExpression` record to write: the regular
/// expression, and its resolution or `None` for a tombstone.
pub(crate) type RegexRecord = (String, Option<RegularExpressionValue>);

/// Kafka's `UpdateRegularExpressionStatus`: what a heartbeat did to the
/// group's regular expression subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegexUpdate {
    /// The member's pattern did not change.
    NoChange,
    /// The member changed its pattern to one that is not resolved yet. The
    /// group bumps its epoch when the resolution arrives.
    Updated,
    /// The member changed its pattern to one that the group has resolved, or
    /// dropped its pattern, so the group bumps its epoch at once.
    UpdatedAndResolved,
}

impl RegexUpdate {
    /// Kafka's `UpdateRegularExpressionStatus.regexUpdated`.
    #[must_use]
    pub(crate) fn regex_updated(self) -> bool {
        self != Self::NoChange
    }
}

/// Kafka's `maybeUpdateRegularExpressions`, for a heartbeat of a member whose
/// pattern was `old_regex` and is `new_regex` (`None` for no pattern). The
/// caller has not applied the member's new pattern to `state` yet.
///
/// It tombstones the resolution of the pattern the member drops when it was
/// the last member to use it. It then resolves the group's patterns, the new
/// one included, when one of Kafka's conditions asks for it, and applies the
/// result: each resolution is recorded in `records`, and the group turns
/// dirty when a pattern resolved to other topics than before.
///
/// The conditions, in Kafka's order:
///
/// 1. The group subscribes to a regular expression, or the member just brought
///    a new one.
/// 2. The last resolution is older than
///    [`RegexResolution::min_refresh_interval`].
/// 3. A pattern is not resolved yet, or the metadata image has changed since
///    the last resolution in a way that can change it
///    ([`RegexResolution::refresh_version`]), or the last resolution is older
///    than `group.consumer.regex.refresh.interval.ms`.
pub(crate) fn maybe_update_regular_expressions(
    state: &mut GroupState,
    old_regex: Option<&str>,
    new_regex: Option<&str>,
    regexes: &RegexResolution<'_>,
    records: &mut Vec<RegexRecord>,
) -> RegexUpdate {
    // A member with no pattern before and after asks nothing of the group's
    // resolutions, and this keeps its heartbeat free of a scan of the members.
    // The members that use a pattern heartbeat too, and they refresh a stale
    // resolution.
    if old_regex.is_none() && new_regex.is_none() {
        return RegexUpdate::NoChange;
    }

    let mut require_refresh = false;
    let mut update = RegexUpdate::NoChange;

    // Has the member changed its pattern?
    if old_regex != new_regex {
        update = RegexUpdate::Updated;
        if let Some(old) = old_regex
            && state.num_subscribed_members(old) == 1
            && state.remove_resolved_regex(old)
        {
            // The member was the last one that subscribed to the pattern.
            records.push((old.to_owned(), None));
        }
        if let Some(new) = new_regex {
            if state.num_subscribed_members(new) == 0 {
                // A new pattern: resolve it. The caller has validated it.
                require_refresh = true;
            } else if state.resolved_regex(new).is_some() {
                // Another member's pattern that the group has resolved.
                update = RegexUpdate::UpdatedAndResolved;
            }
        } else if old_regex.is_some() {
            update = RegexUpdate::UpdatedAndResolved;
        }
    }

    // 0. The group subscribes to a regular expression: the member's own
    // change included.
    let mut subscribed = state.subscribed_regexes();
    if let Some(old) = old_regex {
        decrement(&mut subscribed, old);
    }
    if let Some(new) = new_regex {
        *subscribed.entry(new.to_owned()).or_insert(0) += 1;
    }
    if !require_refresh && subscribed.is_empty() {
        return update;
    }

    // 2. The last resolution is older than the minimum interval between two.
    let last_ms = state.last_regex_resolution_ms();
    if regexes.now_ms <= last_ms.saturating_add(duration_millis(regexes.min_refresh_interval)) {
        return update;
    }

    // 3.1 A pattern of the group is not resolved.
    require_refresh |= subscribed.len() != state.num_resolved_regexes();
    // 3.2 The metadata image changed since the last resolution.
    require_refresh |= state.last_regex_resolution_version() < regexes.refresh_version;
    // 3.3 The last resolution is older than the refresh interval.
    require_refresh |=
        regexes.now_ms.saturating_sub(last_ms) > duration_millis(regexes.refresh_interval);

    if require_refresh && !subscribed.is_empty() {
        let patterns: BTreeSet<String> = subscribed.keys().cloned().collect();
        let resolved = regexes.resolver.resolve(&patterns);
        apply_resolutions(state, &subscribed, resolved, records);
    }
    update
}

fn decrement(counts: &mut HashMap<String, usize>, regex: &str) {
    if let Some(count) = counts.get_mut(regex) {
        *count -= 1;
        if *count == 0 {
            counts.remove(regex);
        }
    }
}

/// Kafka's `handleRegularExpressionsResult`: records and stores the
/// resolution of each pattern that a member still uses, and turns the group
/// dirty when a pattern resolved to other topics than before.
fn apply_resolutions(
    state: &mut GroupState,
    subscribed: &HashMap<String, usize>,
    resolved: HashMap<String, ResolvedRegularExpression>,
    records: &mut Vec<RegexRecord>,
) {
    let mut resolved: Vec<(String, ResolvedRegularExpression)> = resolved.into_iter().collect();
    resolved.sort_by(|a, b| a.0.cmp(&b.0));
    for (regex, resolution) in resolved {
        // The group no longer subscribes to this one.
        if !subscribed.contains_key(&regex) {
            continue;
        }
        // Kafka compares with `ResolvedRegularExpression.EMPTY` when the
        // group has no resolution yet.
        let topics_changed = state.resolved_regex(&regex).map_or_else(
            || !resolution.topics.is_empty(),
            |old| old.topics != resolution.topics,
        );
        if topics_changed {
            state.dirty = true;
        }
        records.push((
            regex.clone(),
            Some(RegularExpressionValue::from(&resolution)),
        ));
        state.set_resolved_regex(regex, resolution);
    }
}

/// Kafka's `maybeDeleteResolvedRegularExpressions`: removes the resolutions of
/// the patterns that no member uses any more, after members left, and returns
/// their tombstones.
#[must_use]
pub(crate) fn delete_unsubscribed_regexes(state: &mut GroupState) -> Vec<RegexRecord> {
    state
        .unsubscribed_resolved_regexes()
        .into_iter()
        .map(|regex| {
            state.remove_resolved_regex(&regex);
            (regex, None)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, HashMap},
        sync::Mutex,
    };

    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::consumer_state::test_support::resolved_regex as resolution;

    /// A resolver that answers from a table of what each pattern selects, and
    /// counts its calls.
    #[derive(Debug, Default)]
    struct Table {
        topics: HashMap<&'static str, Vec<&'static str>>,
        calls: Mutex<Vec<BTreeSet<String>>>,
    }

    impl TopicRegexResolver for Table {
        fn resolve(
            &self,
            regexes: &BTreeSet<String>,
        ) -> HashMap<String, ResolvedRegularExpression> {
            self.calls.lock().unwrap().push(regexes.clone());
            regexes
                .iter()
                .map(|regex| {
                    (
                        regex.clone(),
                        ResolvedRegularExpression {
                            topics: self
                                .topics
                                .get(regex.as_str())
                                .into_iter()
                                .flatten()
                                .map(|topic| (*topic).to_owned())
                                .collect(),
                            version: 100,
                            timestamp_ms: NOW_MS,
                        },
                    )
                })
                .collect()
        }
    }

    const NOW_MS: i64 = 1_000_000;
    const REFRESH_INTERVAL: Duration = Duration::from_mins(10);

    fn table(entries: &[(&'static str, &[&'static str])]) -> Table {
        Table {
            topics: entries
                .iter()
                .map(|(regex, topics)| (*regex, topics.to_vec()))
                .collect(),
            ..Table::default()
        }
    }

    fn group(subscriptions: &[(&str, Option<&str>)]) -> GroupState {
        let mut group =
            crate::coordinator::unified::consumer_state::test_support::regex_group(subscriptions);
        group.dirty = false;
        group
    }

    fn run(
        state: &mut GroupState,
        old_regex: Option<&str>,
        new_regex: Option<&str>,
        resolver: &Table,
        now_ms: i64,
        refresh_version: i64,
    ) -> (RegexUpdate, Vec<RegexRecord>) {
        let mut records = Vec::new();
        let update = maybe_update_regular_expressions(
            state,
            old_regex,
            new_regex,
            &RegexResolution {
                resolver,
                refresh_version,
                now_ms,
                refresh_interval: REFRESH_INTERVAL,
                min_refresh_interval: Duration::from_secs(10),
            },
            &mut records,
        );
        (update, records)
    }

    fn written(topics: &[&str], version: i64, timestamp_ms: i64) -> RegularExpressionValue {
        RegularExpressionValue {
            topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
            version,
            timestamp_ms,
        }
    }

    /// A member that brings the first pattern of the group has it resolved at
    /// once, its resolution is written, and the group turns dirty because
    /// the pattern selects topics.
    #[test]
    fn a_new_pattern_is_resolved_and_recorded() {
        let mut state = group(&[]);
        let resolver = table(&[("a.*", &["a1", "a2"])]);

        let (update, records) = run(&mut state, None, Some("a.*"), &resolver, NOW_MS, -1);

        check!(update == RegexUpdate::Updated);
        check!(records == vec![("a.*".to_owned(), Some(written(&["a1", "a2"], 100, NOW_MS)))]);
        check!(state.resolved_regex("a.*") == Some(&resolution(&["a1", "a2"], 100, NOW_MS)));
        check!(state.dirty);
        check!(resolver.calls.lock().unwrap().len() == 1);
    }

    /// A pattern that selects no topic is recorded too, and does not turn a
    /// clean group dirty, since nothing changes for the group's members: Kafka
    /// compares the resolution with an empty one when the group has none.
    #[test]
    fn a_pattern_that_selects_nothing_is_recorded_without_a_rebalance() {
        let mut state = group(&[]);
        let resolver = table(&[]);

        let (_, records) = run(&mut state, None, Some("a.*"), &resolver, NOW_MS, -1);

        check!(records == vec![("a.*".to_owned(), Some(written(&[], 100, NOW_MS)))]);
        check!(state.resolved_regex("a.*").is_some());
        check!(!state.dirty);
    }

    /// Kafka's `maybeUpdateRegularExpressions`, row by row: the state of the
    /// group before the heartbeat, the heartbeat, and whether it resolves and
    /// what it reports. Every row starts from a group whose member `m1`
    /// subscribes to `a.*`, resolved at version 5 at `NOW_MS - since`.
    #[test]
    fn the_conditions_of_a_refresh_follow_kafka() {
        struct Row {
            name: &'static str,
            /// Time since the last resolution, in milliseconds.
            since_ms: i64,
            /// The metadata version of the latest relevant image.
            refresh_version: i64,
            /// The member that heartbeats: its old and new pattern.
            old: Option<&'static str>,
            new: Option<&'static str>,
            /// Whether the resolver is asked.
            resolves: bool,
            update: RegexUpdate,
        }
        let rows = [
            Row {
                name: "nothing changed and the resolution is fresh",
                since_ms: 60_000,
                refresh_version: 5,
                old: Some("a.*"),
                new: Some("a.*"),
                resolves: false,
                update: RegexUpdate::NoChange,
            },
            Row {
                name: "the image changed since the resolution",
                since_ms: 60_000,
                refresh_version: 6,
                old: Some("a.*"),
                new: Some("a.*"),
                resolves: true,
                update: RegexUpdate::NoChange,
            },
            Row {
                name: "the image changed but the last resolution is within ten seconds",
                since_ms: 10_000,
                refresh_version: 6,
                old: Some("a.*"),
                new: Some("a.*"),
                resolves: false,
                update: RegexUpdate::NoChange,
            },
            Row {
                name: "the image changed and the last resolution is just over ten seconds old",
                since_ms: 10_001,
                refresh_version: 6,
                old: Some("a.*"),
                new: Some("a.*"),
                resolves: true,
                update: RegexUpdate::NoChange,
            },
            Row {
                name: "the refresh interval elapsed",
                since_ms: 600_001,
                refresh_version: 5,
                old: Some("a.*"),
                new: Some("a.*"),
                resolves: true,
                update: RegexUpdate::NoChange,
            },
            Row {
                name: "the refresh interval has not elapsed",
                since_ms: 600_000,
                refresh_version: 5,
                old: Some("a.*"),
                new: Some("a.*"),
                resolves: false,
                update: RegexUpdate::NoChange,
            },
            Row {
                name: "a new pattern, within ten seconds of the last resolution, waits",
                since_ms: 5_000,
                refresh_version: 5,
                old: None,
                new: Some("b.*"),
                resolves: false,
                update: RegexUpdate::Updated,
            },
            Row {
                name: "a new pattern is resolved with the others",
                since_ms: 60_000,
                refresh_version: 5,
                old: None,
                new: Some("b.*"),
                resolves: true,
                update: RegexUpdate::Updated,
            },
            Row {
                name: "a pattern that another member uses and the group resolved",
                since_ms: 60_000,
                refresh_version: 5,
                old: None,
                new: Some("a.*"),
                resolves: false,
                update: RegexUpdate::UpdatedAndResolved,
            },
            Row {
                name: "a member without a pattern leaves a stale resolution to the members that use one",
                since_ms: 600_001,
                refresh_version: 6,
                old: None,
                new: None,
                resolves: false,
                update: RegexUpdate::NoChange,
            },
            Row {
                name: "a member drops its pattern",
                since_ms: 60_000,
                refresh_version: 5,
                old: Some("a.*"),
                new: None,
                resolves: false,
                update: RegexUpdate::UpdatedAndResolved,
            },
        ];
        for row in rows {
            let mut state = group(&[("m1", Some("a.*")), ("m2", row.old)]);
            state.set_resolved_regex("a.*".into(), resolution(&["a1"], 5, NOW_MS - row.since_ms));
            let resolver = table(&[("a.*", &["a1"]), ("b.*", &["b1"])]);

            // The heartbeat is m2's, or m1's when m2 has no pattern to change.
            let (update, _) = run(
                &mut state,
                row.old,
                row.new,
                &resolver,
                NOW_MS,
                row.refresh_version,
            );

            check!(update == row.update, "{}", row.name);
            check!(
                (resolver.calls.lock().unwrap().len() == 1) == row.resolves,
                "{}",
                row.name
            );
        }
    }

    /// A refresh that finds the same topics writes the record again, with the
    /// new version and time, and leaves a clean group clean. One that finds
    /// other topics turns the group dirty.
    #[test]
    fn a_refresh_that_changes_the_topics_turns_the_group_dirty() {
        // (what the resolver selects now, dirty afterwards)
        let rows: [(&[&str], bool); 3] = [(&["a1"], false), (&["a1", "a2"], true), (&[], true)];
        for (selects, dirty) in rows {
            let mut state = group(&[("m1", Some("a.*"))]);
            state.set_resolved_regex("a.*".into(), resolution(&["a1"], 5, NOW_MS - 60_000));
            let resolver = table(&[("a.*", selects)]);

            let (update, records) = run(&mut state, Some("a.*"), Some("a.*"), &resolver, NOW_MS, 6);

            check!(update == RegexUpdate::NoChange, "{selects:?}");
            check!(
                records == vec![("a.*".to_owned(), Some(written(selects, 100, NOW_MS)))],
                "{selects:?}"
            );
            check!(state.dirty == dirty, "{selects:?}");
        }
    }

    /// The member that was the last to use a pattern tombstones its
    /// resolution when it changes the pattern, and a member that shares the
    /// pattern does not.
    #[test]
    fn the_last_member_to_leave_a_pattern_tombstones_its_resolution() {
        // (the other member's pattern, tombstones)
        let rows: [(Option<&str>, Vec<RegexRecord>); 2] = [
            (None, vec![("a.*".to_owned(), None)]),
            (Some("a.*"), vec![]),
        ];
        for (other, tombstones) in rows {
            let mut state = group(&[("m1", Some("a.*")), ("m2", other)]);
            state.set_resolved_regex("a.*".into(), resolution(&["a1"], 5, NOW_MS));
            let resolver = table(&[]);

            // m1 changes from `a.*` to no pattern.
            let (update, records) = run(&mut state, Some("a.*"), None, &resolver, NOW_MS, -1);

            check!(update == RegexUpdate::UpdatedAndResolved);
            check!(records == tombstones, "{other:?}");
            check!(state.resolved_regex("a.*").is_some() == tombstones.is_empty());
        }
    }

    /// Kafka's `maybeDeleteResolvedRegularExpressions`: a resolution that no
    /// member uses after members left is removed and tombstoned.
    #[test]
    fn resolutions_that_no_member_uses_are_tombstoned() {
        let mut state = group(&[("m1", Some("a.*")), ("m2", Some("b.*"))]);
        for regex in ["a.*", "b.*"] {
            state.set_resolved_regex(regex.into(), resolution(&[], 1, 1));
        }
        state.remove_member("m1");

        let tombstones = delete_unsubscribed_regexes(&mut state);

        check!(tombstones == vec![("a.*".to_owned(), None)]);
        check!(state.resolved_regex("a.*").is_none());
        check!(state.resolved_regex("b.*").is_some());
        assert!(delete_unsubscribed_regexes(&mut state).is_empty());
    }
}
