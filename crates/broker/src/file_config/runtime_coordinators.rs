//! The `[runtime]` appliers for the group coordinators.
//!
//! `apply_coordinators` covers the classic and consumer group protocol
//! timings, and `apply_share_group` and `apply_streams_group` cover the KIP-932
//! share groups and the KIP-1071 streams groups. All three write the same
//! coordinator layer, and all three validate the durations Kafka bounds.

use {
    super::{
        FileConfigError, RuntimeFileConfig,
        validate::{
            invalid_runtime_value, nonnegative_time, positive_time, positive_usize,
            whole_millis_i32_time,
        },
    },
    crate::{config::BrokerConfig, coordinator::unified::share::config::ShareGroupConfig},
    krabka_units::{Time, convert::TimeExt as _},
    std::{fmt, ops::RangeInclusive, time::Duration},
};

impl RuntimeFileConfig {
    pub(super) fn apply_coordinators(&self, cfg: &mut BrokerConfig) -> Result<(), FileConfigError> {
        let runtime = self;
        krabka_macros::set_runtime! {
            runtime => cfg;
            positive_time: coordinator_session_expiry_tick, coordinator_shutdown_ack_timeout;
            duration: consumer_group_session_timeout => next_gen_consumer_group.session_timeout,
                consumer_group_heartbeat_interval => next_gen_consumer_group.heartbeat_interval,
                consumer_group_min_session_timeout => next_gen_consumer_group.min_session_timeout,
                consumer_group_max_session_timeout => next_gen_consumer_group.max_session_timeout,
                consumer_group_min_heartbeat_interval
                    => next_gen_consumer_group.min_heartbeat_interval,
                consumer_group_max_heartbeat_interval
                    => next_gen_consumer_group.max_heartbeat_interval;
            positive_usize: consumer_group_max_size => next_gen_consumer_group.max_size;
            // Zero is Kafka's own development setting for
            // `group.initial.rebalance.delay.ms`, so this one is not held to
            // `positive_time`.
            nonnegative_time: classic_group_initial_rebalance_delay;
            duration: classic_group_min_session_timeout
                    => next_gen_consumer_group.classic_min_session_timeout,
                classic_group_max_session_timeout
                    => next_gen_consumer_group.classic_max_session_timeout;
            positive_usize: classic_group_max_size => next_gen_consumer_group.classic_max_size;
            positive_time: sync_group_follower_wait;
        }
        Ok(())
    }

    /// Applies the share-group keys with the ranges of Kafka's
    /// `GroupCoordinatorConfig` and `ShareGroupConfig` `ConfigDef`, then
    /// checks the order within each value, minimum and maximum triple over
    /// the resulting config, as their constructors `require` it.
    ///
    /// The order check runs over `cfg` rather than over this table alone, so
    /// a command-line overlay applied after the file is checked against the
    /// file's values.
    pub(super) fn apply_share_group(&self, cfg: &mut BrokerConfig) -> Result<(), FileConfigError> {
        let runtime = self;
        let share = &mut *cfg.share_group;
        krabka_macros::set_runtime! {
            runtime => share;
            int_millis: share_group_session_timeout => session_timeout in AT_LEAST_ONE_MS,
                share_group_heartbeat_interval => heartbeat_interval in AT_LEAST_ONE_MS,
                share_group_min_session_timeout => min_session_timeout in AT_LEAST_ONE_MS,
                share_group_max_session_timeout => max_session_timeout in AT_LEAST_ONE_MS,
                share_group_min_heartbeat_interval => min_heartbeat_interval in AT_LEAST_ONE_MS,
                share_group_max_heartbeat_interval => max_heartbeat_interval in AT_LEAST_ONE_MS,
                share_group_record_lock_duration => record_lock_duration in 1_000..=3_600_000,
                share_group_min_record_lock_duration
                    => min_record_lock_duration in 1_000..=30_000,
                share_group_max_record_lock_duration
                    => max_record_lock_duration in 30_000..=3_600_000;
            in_range: share_group_max_size => max_size in 1..=1_000,
                share_group_delivery_count_limit => max_delivery_attempts in 2..=10,
                share_group_min_delivery_count_limit => min_delivery_count_limit in 2..=5,
                share_group_max_delivery_count_limit => max_delivery_count_limit in 5..=25,
                share_group_partition_max_record_locks => max_inflight_records in 100..=10_000,
                share_group_min_partition_max_record_locks
                    => min_partition_max_record_locks in 100..=2_000,
                share_group_max_partition_max_record_locks
                    => max_partition_max_record_locks in 2_000..=10_000;
        }
        validate_share_group_order(share)?;
        krabka_macros::set_runtime! {
            runtime => share;
            duration: share_group_backlog_poll_interval => backlog_poll_interval;
        }
        Ok(())
    }

    pub(super) fn apply_streams_group(
        &mut self,
        cfg: &mut BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        krabka_macros::set_runtime! {
            runtime => cfg;
            plain: streams_group_enable => streams_group.enable;
            duration: streams_group_session_timeout => streams_group.session_timeout,
                streams_group_heartbeat_interval => streams_group.heartbeat_interval,
                streams_group_min_session_timeout => streams_group.min_session_timeout,
                streams_group_max_session_timeout => streams_group.max_session_timeout,
                streams_group_min_heartbeat_interval => streams_group.min_heartbeat_interval,
                streams_group_max_heartbeat_interval => streams_group.max_heartbeat_interval;
            positive_usize: streams_group_max_size => streams_group.max_size;
        }
        if let Some(value) = runtime.streams_group_num_standby_replicas {
            if value < 0 {
                return Err(invalid_runtime_value(
                    "streams_group_num_standby_replicas",
                    "must be nonnegative",
                ));
            }
            cfg.streams_group.num_standby_replicas = value;
        }
        if let Some(entries) = runtime.streams_group_rack_aware_assignment_tags.take() {
            use crate::coordinator::unified::streams::config::parse_broker_rack_aware_assignment_tags;
            cfg.streams_group.rack_aware_assignment_tags =
                parse_broker_rack_aware_assignment_tags(&entries).map_err(|message| {
                    invalid_runtime_value("streams_group_rack_aware_assignment_tags", message)
                })?;
        }
        if let Some(value) = runtime.streams_group_num_warmup_replicas {
            if value < 0 {
                return Err(invalid_runtime_value(
                    "streams_group_num_warmup_replicas",
                    "must be nonnegative",
                ));
            }
            cfg.streams_group.num_warmup_replicas = value;
        }
        if let Some(value) = runtime.streams_group_acceptable_recovery_lag {
            if value < 0 {
                return Err(invalid_runtime_value(
                    "streams_group_acceptable_recovery_lag",
                    "must be nonnegative",
                ));
            }
            cfg.streams_group.acceptable_recovery_lag = value;
        }
        krabka_macros::set_runtime! {
            runtime => cfg;
            duration: streams_group_task_offset_interval => streams_group.task_offset_interval;
        }
        if let Some(value) = runtime.streams_group_assignor.take() {
            use crate::coordinator::unified::streams::config::StreamsAssignorKind;
            cfg.streams_group.assignor =
                StreamsAssignorKind::from_config_name(&value).ok_or_else(|| {
                    invalid_runtime_value(
                        "streams_group_assignor",
                        "expected `auto`, `sticky`, or `highly-available`",
                    )
                })?;
        }

        if let Some(value) = runtime.inter_broker_server_name.take() {
            cfg.inter_broker_server_name = value;
        }
        Ok(())
    }
}

/// Kafka's `atLeast(1)` over an `INT` millisecond key.
const AT_LEAST_ONE_MS: RangeInclusive<i64> = 1..=2_147_483_647;

/// A whole number of milliseconds within `range`: Kafka's `between` or
/// `atLeast` over an `INT` key.
fn int_millis(
    name: &str,
    value: Time,
    range: RangeInclusive<i64>,
) -> Result<Duration, FileConfigError> {
    let value = whole_millis_i32_time(name, value)?;
    if range.contains(&value.millis_i64()) {
        Ok(value.to_std())
    } else {
        Err(invalid_runtime_value(
            name,
            format!("must be within {}ms..={}ms", range.start(), range.end()),
        ))
    }
}

/// `value` within `range`: Kafka's `between` over an `INT` key.
fn in_range<T: PartialOrd + fmt::Display>(
    name: &str,
    value: T,
    range: RangeInclusive<T>,
) -> Result<T, FileConfigError> {
    if range.contains(&value) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            format!("must be within {}..={}", range.start(), range.end()),
        ))
    }
}

/// The `require` checks of Kafka's `GroupCoordinatorConfig` and
/// `ShareGroupConfig` constructors over the share keys, in Kafka's order and
/// with Kafka's messages, each naming the `[runtime]` field for the Kafka key.
fn validate_share_group_order(share: &ShareGroupConfig) -> Result<(), FileConfigError> {
    const AT_LEAST: &str = "must be greater than or equal to";
    const AT_MOST: &str = "must be less than or equal to";
    // (whether the check holds, the field it names, the relation, the field
    // it compares against)
    let checks = [
        (
            share.max_heartbeat_interval >= share.min_heartbeat_interval,
            "share_group_max_heartbeat_interval",
            AT_LEAST,
            "share_group_min_heartbeat_interval",
        ),
        (
            share.heartbeat_interval >= share.min_heartbeat_interval,
            "share_group_heartbeat_interval",
            AT_LEAST,
            "share_group_min_heartbeat_interval",
        ),
        (
            share.heartbeat_interval <= share.max_heartbeat_interval,
            "share_group_heartbeat_interval",
            AT_MOST,
            "share_group_max_heartbeat_interval",
        ),
        (
            share.max_session_timeout >= share.min_session_timeout,
            "share_group_max_session_timeout",
            AT_LEAST,
            "share_group_min_session_timeout",
        ),
        (
            share.session_timeout >= share.min_session_timeout,
            "share_group_session_timeout",
            AT_LEAST,
            "share_group_min_session_timeout",
        ),
        (
            share.session_timeout <= share.max_session_timeout,
            "share_group_session_timeout",
            AT_MOST,
            "share_group_max_session_timeout",
        ),
        (
            share.heartbeat_interval < share.session_timeout,
            "share_group_heartbeat_interval",
            "must be less than",
            "share_group_session_timeout",
        ),
        (
            share.max_delivery_count_limit >= share.max_delivery_attempts,
            "share_group_max_delivery_count_limit",
            AT_LEAST,
            "share_group_delivery_count_limit",
        ),
        (
            share.max_delivery_attempts >= share.min_delivery_count_limit,
            "share_group_delivery_count_limit",
            AT_LEAST,
            "share_group_min_delivery_count_limit",
        ),
        (
            share.max_partition_max_record_locks >= share.max_inflight_records,
            "share_group_max_partition_max_record_locks",
            AT_LEAST,
            "share_group_partition_max_record_locks",
        ),
        (
            share.max_inflight_records >= share.min_partition_max_record_locks,
            "share_group_partition_max_record_locks",
            AT_LEAST,
            "share_group_min_partition_max_record_locks",
        ),
        (
            share.record_lock_duration >= share.min_record_lock_duration,
            "share_group_record_lock_duration",
            AT_LEAST,
            "share_group_min_record_lock_duration",
        ),
        (
            share.max_record_lock_duration >= share.record_lock_duration,
            "share_group_max_record_lock_duration",
            AT_LEAST,
            "share_group_record_lock_duration",
        ),
    ];
    match checks.into_iter().find(|(holds, ..)| !holds) {
        Some((_, name, relation, other)) => {
            Err(invalid_runtime_value(name, format!("{relation} {other}")))
        }
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use rstest_bdd::datatable::Rows;

    fn applied_runtime(body: &str) -> Result<BrokerConfig, String> {
        let file: FileConfig =
            toml::from_str(&format!("[runtime]\n{body}\n")).expect("parse runtime config");
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).map_err(|error| error.to_string())?;
        Ok(cfg)
    }

    use {
        super::*,
        crate::{config::BrokerConfig, file_config::FileConfig},
        assert2::assert,
    };

    #[derive(Default)]
    struct CoordinatorProperties {
        body: String,
        applied: Option<Result<BrokerConfig, String>>,
    }
    #[derive(rstest_bdd_macros::DataTableRow)]
    struct SharePolicyField {
        field: String,
        value: String,
    }
    #[rstest::fixture]
    fn coordinator_properties() -> CoordinatorProperties {
        // A property set and its complete application outcome belong to this scenario.
        CoordinatorProperties::default()
    }
    #[rstest_bdd_macros::given("the runtime coordinator properties are")]
    fn coordinator_property_source(
        coordinator_properties: &mut CoordinatorProperties,
        docstring: String,
    ) {
        coordinator_properties.body = docstring;
    }
    #[rstest_bdd_macros::when("the broker validates and applies the coordinator properties")]
    fn apply_coordinator_properties(coordinator_properties: &mut CoordinatorProperties) {
        coordinator_properties.applied = Some(applied_runtime(&coordinator_properties.body));
    }
    #[rstest_bdd_macros::then("the entire Share group policy equals Kafka defaults except")]
    fn share_group_policy(
        coordinator_properties: &CoordinatorProperties,
        #[datatable] datatable: Rows<SharePolicyField>,
    ) {
        let config = coordinator_properties
            .applied
            .as_ref()
            .expect("properties applied")
            .as_ref()
            .expect("valid policy");
        let mut expected = ShareGroupConfig::default();
        for row in datatable {
            match row.field.as_str() {
                "max_inflight_records" => {
                    expected.max_inflight_records = row.value.parse().expect("integer bound");
                }
                "max_partition_max_record_locks" => {
                    expected.max_partition_max_record_locks =
                        row.value.parse().expect("integer bound");
                }
                "max_delivery_count_limit" => {
                    expected.max_delivery_count_limit = row.value.parse().expect("integer bound");
                }
                "record_lock_duration" => {
                    expected.record_lock_duration =
                        Duration::from_millis(row.value.parse().expect("millisecond duration"));
                }
                "min_record_lock_duration" => {
                    expected.min_record_lock_duration =
                        Duration::from_millis(row.value.parse().expect("millisecond duration"));
                }
                "min_session_timeout" => {
                    expected.min_session_timeout =
                        Duration::from_millis(row.value.parse().expect("millisecond duration"));
                }
                "max_session_timeout" => {
                    expected.max_session_timeout =
                        Duration::from_millis(row.value.parse().expect("millisecond duration"));
                }
                "session_timeout" => {
                    expected.session_timeout =
                        Duration::from_millis(row.value.parse().expect("millisecond duration"));
                }
                unknown => panic!("unknown Share policy field {unknown}"),
            }
        }
        assert!(config.share_group.as_ref() == &expected);
    }
    #[rstest_bdd_macros::then("coordinator properties are refused with")]
    fn coordinator_property_error(
        coordinator_properties: &CoordinatorProperties,
        docstring: String,
    ) {
        let actual = coordinator_properties
            .applied
            .as_ref()
            .expect("properties applied")
            .as_ref()
            .expect_err("invalid policy refused");
        assert!(actual == docstring.trim());
    }
    #[rstest_bdd_macros::then("the Streams rack tags are exactly {tags}")]
    fn streams_rack_tags(coordinator_properties: &CoordinatorProperties, tags: String) {
        let config = coordinator_properties
            .applied
            .as_ref()
            .expect("properties applied")
            .as_ref()
            .expect("valid rack tags");
        let expected: Vec<String> = serde_json::from_str(&tags).expect("JSON array of rack tags");
        assert!(config.streams_group.rack_aware_assignment_tags == expected);
    }
    rstest_bdd_macros::scenarios!("tests/features/migrated/src/file_config/runtime_coordinators.feature", fixtures = [coordinator_properties: CoordinatorProperties]);

    /// Kafka's `group.streams.min.session.timeout.ms` and its three siblings
    /// move the bounds, and the session timeout and the heartbeat interval
    /// must stay inside them, as `GroupCoordinatorConfig` requires.
    #[test]
    fn streams_group_bounds_move_and_hold_the_timings() {
        /// The session timeout and heartbeat interval of the streams group, each
        /// with its minimum and maximum.
        #[derive(Debug, PartialEq, Eq)]
        struct Timings {
            session_timeout: Duration,
            min_session_timeout: Duration,
            max_session_timeout: Duration,
            heartbeat_interval: Duration,
            min_heartbeat_interval: Duration,
            max_heartbeat_interval: Duration,
        }
        struct Row {
            label: &'static str,
            body: &'static str,
            want: Result<Timings, &'static str>,
        }
        let secs = Duration::from_secs;
        let rows: [Row; 4] = [
            Row {
                label: "the defaults",
                body: "",
                want: Ok(Timings {
                    session_timeout: secs(45),
                    min_session_timeout: secs(45),
                    max_session_timeout: secs(60),
                    heartbeat_interval: secs(5),
                    min_heartbeat_interval: secs(5),
                    max_heartbeat_interval: secs(15),
                }),
            },
            Row {
                label: "session timeout lowered with its minimum",
                body: "streams_group_min_session_timeout = \"10s\"\n\
                 streams_group_session_timeout = \"10s\"",
                want: Ok(Timings {
                    session_timeout: secs(10),
                    min_session_timeout: secs(10),
                    max_session_timeout: secs(60),
                    heartbeat_interval: secs(5),
                    min_heartbeat_interval: secs(5),
                    max_heartbeat_interval: secs(15),
                }),
            },
            Row {
                label: "heartbeat bounds widened",
                body: "streams_group_min_heartbeat_interval = \"1s\"\n\
                 streams_group_max_heartbeat_interval = \"30s\"\n\
                 streams_group_heartbeat_interval = \"20s\"",
                want: Ok(Timings {
                    session_timeout: secs(45),
                    min_session_timeout: secs(45),
                    max_session_timeout: secs(60),
                    heartbeat_interval: secs(20),
                    min_heartbeat_interval: secs(1),
                    max_heartbeat_interval: secs(30),
                }),
            },
            Row {
                label: "session timeout lowered alone",
                body: "streams_group_session_timeout = \"10s\"",
                want: Err(
                    "invalid config: invalid runtime configuration: streams group session \
                     timeout is outside its bounds",
                ),
            },
        ];
        for Row { label, body, want } in rows {
            let applied = applied_runtime(body).map(|cfg| {
                let streams = &cfg.streams_group;
                Timings {
                    session_timeout: streams.session_timeout,
                    min_session_timeout: streams.min_session_timeout,
                    max_session_timeout: streams.max_session_timeout,
                    heartbeat_interval: streams.heartbeat_interval,
                    min_heartbeat_interval: streams.min_heartbeat_interval,
                    max_heartbeat_interval: streams.max_heartbeat_interval,
                }
            });
            assert!(applied == want.map_err(str::to_owned), "{label}");
        }
    }
}
