//! KIP-1071 Streams rebalance-protocol configuration.
use std::{collections::BTreeMap, time::Duration};

pub const KEY_SESSION_TIMEOUT_MS: &str = "streams.session.timeout.ms";
pub const KEY_HEARTBEAT_INTERVAL_MS: &str = "streams.heartbeat.interval.ms";
pub const KEY_ACCEPTABLE_RECOVERY_LAG: &str = "streams.acceptable.recovery.lag";
pub const KEY_NUM_WARMUP_REPLICAS: &str = "streams.num.warmup.replicas";
pub const KEY_NUM_STANDBY_REPLICAS: &str = "streams.num.standby.replicas";
pub const KEY_TASK_OFFSET_INTERVAL_MS: &str = "streams.task.offset.interval.ms";
pub const KEY_ASSIGNOR_NAME: &str = "streams.assignor.name";
pub const KEY_SHARE_AUTO_OFFSET_RESET: &str = "share.auto.offset.reset";

pub const GROUP_CONFIG_KEYS: [&str; 8] = [
    KEY_SESSION_TIMEOUT_MS,
    KEY_HEARTBEAT_INTERVAL_MS,
    KEY_ACCEPTABLE_RECOVERY_LAG,
    KEY_NUM_WARMUP_REPLICAS,
    KEY_NUM_STANDBY_REPLICAS,
    KEY_TASK_OFFSET_INTERVAL_MS,
    KEY_ASSIGNOR_NAME,
    KEY_SHARE_AUTO_OFFSET_RESET,
];

/// Server-side task-assignor selection for a streams group. `Auto` (the Kafka
/// default) picks `HighlyAvailable` when the topology has any stateful
/// subtopology (a state-changelog topic) and `Sticky` otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamsAssignorKind {
    #[default]
    Auto,
    /// Minimise task movement; active-only, no standby/warmup.
    Sticky,
    /// Place standby replicas + warm up state migrations for fault tolerance.
    HighlyAvailable,
}

impl StreamsAssignorKind {
    #[must_use]
    pub fn config_name(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Sticky => "sticky",
            Self::HighlyAvailable => "highly_available",
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "auto" => Ok(Self::Auto),
            "sticky" => Ok(Self::Sticky),
            "highly_available" | "highly-available" => Ok(Self::HighlyAvailable),
            _ => Err(format!(
                "{KEY_ASSIGNOR_NAME} must be `auto`, `sticky`, or `highly_available`"
            )),
        }
    }
}

/// KIP-932 `share.auto.offset.reset`: where a share partition of a group
/// starts when the share coordinator holds no state for it.
///
/// Kafka's `ShareGroupAutoOffsetResetStrategy` accepts `latest`, `earliest`,
/// and `by_duration:<ISO-8601 duration>`, and defaults to `latest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShareAutoOffsetReset {
    /// Start at the partition's high watermark: records produced before the
    /// group's first fetch are not delivered.
    #[default]
    Latest,
    /// Start at the partition's log start offset.
    Earliest,
    /// Start at the first record whose timestamp is at or after
    /// `now - duration`, and at the high watermark when no record qualifies.
    ByDuration(Duration),
}

impl ShareAutoOffsetReset {
    /// The value `DescribeConfigs` reports for this strategy.
    ///
    /// The duration renders the way `java.time.Duration::toString` does, so a
    /// round trip through [`Self::parse`] is lossless: days fold into hours,
    /// zero components drop out, and a zero duration renders as `PT0S`.
    #[must_use]
    pub fn config_value(self) -> String {
        match self {
            Self::Latest => "latest".to_owned(),
            Self::Earliest => "earliest".to_owned(),
            Self::ByDuration(duration) => format!("by_duration:{}", iso8601(duration)),
        }
    }

    /// Parses one `share.auto.offset.reset` value.
    ///
    /// # Errors
    /// Returns a message suitable for `INVALID_CONFIG` when the value names no
    /// strategy, when the `by_duration:` prefix carries no duration, or when
    /// the duration is not ISO-8601. As in Kafka, a negative duration is
    /// rejected and a zero duration (`by_duration:PT0S`) is accepted.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "latest" => return Ok(Self::Latest),
            "earliest" => return Ok(Self::Earliest),
            _ => {}
        }
        let duration = value
            .strip_prefix("by_duration:")
            .ok_or_else(invalid_share_auto_offset_reset)
            .and_then(|iso| parse_iso8601(iso).ok_or_else(invalid_share_auto_offset_reset))?;
        Ok(Self::ByDuration(duration))
    }

    /// The strategy a group runs with, given its persisted override map.
    ///
    /// A group with no override, or with one this broker cannot parse, runs
    /// the Kafka default. `IncrementalAlterConfigs` rejects an unparseable
    /// value before it reaches the metadata log, so the fallback covers only a
    /// value written by some other path.
    #[must_use]
    pub fn from_group_overrides(overrides: &BTreeMap<String, String>) -> Self {
        overrides
            .get(KEY_SHARE_AUTO_OFFSET_RESET)
            .and_then(|value| Self::parse(value).ok())
            .unwrap_or_default()
    }
}

fn invalid_share_auto_offset_reset() -> String {
    format!(
        "{KEY_SHARE_AUTO_OFFSET_RESET} must be `latest`, `earliest`, or \
         `by_duration:<PnDTnHnMn.nS>` with a non-negative duration"
    )
}

/// Renders `duration` the way `java.time.Duration::toString` does.
fn iso8601(duration: Duration) -> String {
    let secs = duration.as_secs();
    let nanos = duration.subsec_nanos();
    let (hours, minutes, seconds) = (secs / 3_600, (secs % 3_600) / 60, secs % 60);
    let hours_part = if hours > 0 {
        format!("{hours}H")
    } else {
        String::new()
    };
    let minutes_part = if minutes > 0 {
        format!("{minutes}M")
    } else {
        String::new()
    };
    // A zero duration still renders its seconds, so `PT` never stands alone.
    let seconds_part = match (seconds, nanos, hours, minutes) {
        (0, 0, hours, minutes) if hours > 0 || minutes > 0 => String::new(),
        (seconds, 0, _, _) => format!("{seconds}S"),
        (seconds, nanos, _, _) => {
            let fraction = format!("{nanos:09}");
            format!("{seconds}.{}S", fraction.trim_end_matches('0'))
        }
    };
    format!("PT{hours_part}{minutes_part}{seconds_part}")
}

/// Parses the ISO-8601 duration forms `java.time.Duration::parse` accepts:
/// `PnDTnHnMn.nS`, case-insensitive, each component optionally signed, at
/// least one component present, and a `T` section that is non-empty when it is
/// present. Weeks, months, and years are not duration components, exactly as
/// in Java. Returns `None` for a malformed or negative duration.
fn parse_iso8601(text: &str) -> Option<Duration> {
    let lowered = text.to_ascii_lowercase();
    let (negate, body) = match lowered.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, lowered.strip_prefix('+').unwrap_or(&lowered)),
    };
    let body = body.strip_prefix('p')?;
    let (days_part, time_part) = match body.split_once('t') {
        Some((days, time)) => (days, Some(time)),
        None => (body, None),
    };

    let mut total_nanos: i128 = 0;
    let mut components = 0_usize;
    let mut rest = days_part;
    if !rest.is_empty() {
        let (value, tail) = take_signed_number(rest, 'd')?;
        total_nanos = total_nanos.checked_add(value.checked_mul(86_400_000_000_000)?)?;
        components += 1;
        rest = tail;
        if !rest.is_empty() {
            return None;
        }
    }
    if let Some(time) = time_part {
        let mut rest = time;
        let mut time_components = 0_usize;
        for (unit, unit_nanos) in [('h', 3_600_000_000_000_i128), ('m', 60_000_000_000)] {
            if rest.is_empty() || !rest.contains(unit) {
                continue;
            }
            let (value, tail) = take_signed_number(rest, unit)?;
            total_nanos = total_nanos.checked_add(value.checked_mul(unit_nanos)?)?;
            time_components += 1;
            rest = tail;
        }
        if !rest.is_empty() {
            total_nanos = total_nanos.checked_add(take_seconds(rest)?)?;
            time_components += 1;
        }
        if time_components == 0 {
            return None;
        }
        components += time_components;
    }
    if components == 0 {
        return None;
    }
    if negate {
        total_nanos = -total_nanos;
    }
    let total_nanos = u128::try_from(total_nanos).ok()?;
    let secs = u64::try_from(total_nanos / 1_000_000_000).ok()?;
    let subsec = u32::try_from(total_nanos % 1_000_000_000).ok()?;
    Some(Duration::new(secs, subsec))
}

/// Splits `text` at `unit`, returning the signed integer before it and the
/// remainder after it.
fn take_signed_number(text: &str, unit: char) -> Option<(i128, &str)> {
    let (number, tail) = text.split_once(unit)?;
    Some((parse_signed(number)?, tail))
}

/// Parses the seconds component, `n` or `n.n`, into nanoseconds.
fn take_seconds(text: &str) -> Option<i128> {
    let seconds = text.strip_suffix('s')?;
    let (whole, fraction) = match seconds.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (seconds, ""),
    };
    // A bare sign, an empty seconds field, or an over-long fraction is not a
    // duration Java would parse.
    if fraction.len() > 9 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let whole_nanos = parse_signed(whole)?.checked_mul(1_000_000_000)?;
    if fraction.is_empty() {
        return Some(whole_nanos);
    }
    let scaled = format!("{fraction:0<9}").parse::<i128>().ok()?;
    // The fraction carries the sign of the whole part, as in Java.
    let signed = if whole.starts_with('-') {
        -scaled
    } else {
        scaled
    };
    whole_nanos.checked_add(signed)
}

/// Parses an optionally signed decimal integer, rejecting an empty digit run.
fn parse_signed(text: &str) -> Option<i128> {
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let magnitude = digits.parse::<i128>().ok()?;
    Some(if text.starts_with('-') {
        -magnitude
    } else {
        magnitude
    })
}

/// KIP-1071 streams-group membership and assignment configuration. Static
/// broker values provide defaults; GROUP resources can override the supported
/// `streams.*` keys for one group through `IncrementalAlterConfigs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupConfig {
    /// Config-level kill switch. The real gate is the `streams.version`
    /// feature (KIP-1071 early access, default-disabled). This switch lets an
    /// operator turn the protocol off even where the feature is finalized.
    pub enable: bool,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    /// Default replication factor when an internal-topic spec leaves it unset.
    pub internal_topic_replication_factor: i16,
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    pub min_heartbeat_interval: Duration,
    pub max_heartbeat_interval: Duration,
    /// Max members per group.
    pub max_size: usize,
    /// `num.standby.replicas`: standby copies per stateful task.
    pub num_standby_replicas: i32,
    /// `max.warmup.replicas`: cap on concurrent warmup tasks. A warmup task
    /// migrates state.
    pub num_warmup_replicas: i32,
    /// `acceptable.recovery.lag`: the maximum changelog lag in records at which
    /// a warmup task is caught up. The assignor can then promote the task to
    /// active or standby.
    pub acceptable_recovery_lag: i64,
    /// How often a member reports task offsets, so the assignor can evaluate
    /// warmup catch-up. This is `task_offset_interval_ms` in the heartbeat
    /// response.
    pub task_offset_interval: Duration,
    /// Server-side assignor selection.
    pub assignor: StreamsAssignorKind,
    /// KIP-932 share-partition start strategy for a group with no persisted
    /// share state.
    pub share_auto_offset_reset: ShareAutoOffsetReset,
    pub actor_mailbox_capacity: usize,
}

impl Default for StreamsGroupConfig {
    fn default() -> Self {
        Self {
            enable: true,
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(5),
            internal_topic_replication_factor: 3,
            min_session_timeout: Duration::from_secs(45),
            max_session_timeout: Duration::from_mins(1),
            min_heartbeat_interval: Duration::from_secs(5),
            max_heartbeat_interval: Duration::from_secs(15),
            max_size: 200,
            // Kafka GA defaults: no standby copies, up to 2 warmups,
            // acceptable lag 10k records.
            num_standby_replicas: 0,
            num_warmup_replicas: 2,
            acceptable_recovery_lag: 10_000,
            task_offset_interval: Duration::from_secs(30),
            assignor: StreamsAssignorKind::Auto,
            share_auto_offset_reset: ShareAutoOffsetReset::Latest,
            actor_mailbox_capacity: 64,
        }
    }
}

impl StreamsGroupConfig {
    /// Apply a persisted GROUP resource override map to these broker defaults.
    ///
    /// # Errors
    /// Returns a message suitable for `INVALID_CONFIG` when a key is unknown,
    /// a value cannot be parsed, or a timeout falls outside broker bounds.
    pub fn with_group_overrides(
        &self,
        overrides: &BTreeMap<String, String>,
    ) -> Result<Self, String> {
        let mut out = self.clone();
        for (key, value) in overrides {
            match key.as_str() {
                KEY_SESSION_TIMEOUT_MS => {
                    out.session_timeout = parse_positive_millis(key, value)?;
                }
                KEY_HEARTBEAT_INTERVAL_MS => {
                    out.heartbeat_interval = parse_positive_millis(key, value)?;
                }
                KEY_ACCEPTABLE_RECOVERY_LAG => {
                    out.acceptable_recovery_lag = parse_nonnegative(key, value)?;
                }
                KEY_NUM_WARMUP_REPLICAS => {
                    out.num_warmup_replicas = parse_nonnegative(key, value)?;
                }
                KEY_NUM_STANDBY_REPLICAS => {
                    out.num_standby_replicas = parse_nonnegative(key, value)?;
                }
                KEY_TASK_OFFSET_INTERVAL_MS => {
                    out.task_offset_interval = parse_positive_millis(key, value)?;
                }
                KEY_ASSIGNOR_NAME => out.assignor = StreamsAssignorKind::parse(value)?,
                KEY_SHARE_AUTO_OFFSET_RESET => {
                    out.share_auto_offset_reset = ShareAutoOffsetReset::parse(value)?;
                }
                _ => return Err(format!("unknown group config `{key}`")),
            }
        }
        if !(out.min_session_timeout..=out.max_session_timeout).contains(&out.session_timeout) {
            return Err(format!(
                "{KEY_SESSION_TIMEOUT_MS} must be between {} and {} ms",
                out.min_session_timeout.as_millis(),
                out.max_session_timeout.as_millis()
            ));
        }
        if !(out.min_heartbeat_interval..=out.max_heartbeat_interval)
            .contains(&out.heartbeat_interval)
        {
            return Err(format!(
                "{KEY_HEARTBEAT_INTERVAL_MS} must be between {} and {} ms",
                out.min_heartbeat_interval.as_millis(),
                out.max_heartbeat_interval.as_millis()
            ));
        }
        Ok(out)
    }

    /// Effective values exposed by `DescribeConfigs` for a GROUP resource.
    #[must_use]
    pub fn group_config_values(&self) -> BTreeMap<String, String> {
        maplit::btreemap! {
        KEY_SESSION_TIMEOUT_MS.into() => self.session_timeout.as_millis().to_string(),
        KEY_HEARTBEAT_INTERVAL_MS.into() => self.heartbeat_interval.as_millis().to_string(),
        KEY_ACCEPTABLE_RECOVERY_LAG.into() => self.acceptable_recovery_lag.to_string(),
        KEY_NUM_WARMUP_REPLICAS.into() => self.num_warmup_replicas.to_string(),
        KEY_NUM_STANDBY_REPLICAS.into() => self.num_standby_replicas.to_string(),
        KEY_TASK_OFFSET_INTERVAL_MS.into() => self.task_offset_interval.as_millis().to_string(),
        KEY_ASSIGNOR_NAME.into() => self.assignor.config_name().into(),
        KEY_SHARE_AUTO_OFFSET_RESET.into() => self.share_auto_offset_reset.config_value()}
    }
}

fn parse_positive_millis(key: &str, value: &str) -> Result<Duration, String> {
    let millis = value
        .parse::<u64>()
        .map_err(|_| format!("{key} must be a positive integer"))?;
    if millis == 0 {
        return Err(format!("{key} must be positive"));
    }
    Ok(Duration::from_millis(millis))
}

fn parse_nonnegative<T>(key: &str, value: &str) -> Result<T, String>
where
    T: std::str::FromStr + PartialOrd + Default,
{
    let parsed = value
        .parse::<T>()
        .map_err(|_| format!("{key} must be a nonnegative integer"))?;
    if parsed < T::default() {
        return Err(format!("{key} must be nonnegative"));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn defaults_are_kafka_ga() {
        assert!(
            StreamsGroupConfig::default()
                == StreamsGroupConfig {
                    enable: true,
                    session_timeout: Duration::from_secs(45),
                    heartbeat_interval: Duration::from_secs(5),
                    internal_topic_replication_factor: 3,
                    min_session_timeout: Duration::from_secs(45),
                    max_session_timeout: Duration::from_mins(1),
                    min_heartbeat_interval: Duration::from_secs(5),
                    max_heartbeat_interval: Duration::from_secs(15),
                    max_size: 200,
                    num_standby_replicas: 0,
                    num_warmup_replicas: 2,
                    acceptable_recovery_lag: 10_000,
                    task_offset_interval: Duration::from_secs(30),
                    assignor: StreamsAssignorKind::Auto,
                    share_auto_offset_reset: ShareAutoOffsetReset::Latest,
                    actor_mailbox_capacity: 64,
                }
        );
    }

    #[test]
    fn group_overrides_are_validated_and_applied() {
        let overrides = maplit::btreemap! {
        KEY_SESSION_TIMEOUT_MS.into() => "50000".into(),
        KEY_HEARTBEAT_INTERVAL_MS.into() => "6000".into(),
        KEY_NUM_STANDBY_REPLICAS.into() => "1".into(),
        KEY_ASSIGNOR_NAME.into() => "highly_available".into()};
        let got = StreamsGroupConfig::default()
            .with_group_overrides(&overrides)
            .expect("valid overrides");
        assert!(got.session_timeout == Duration::from_secs(50));
        assert!(got.heartbeat_interval == Duration::from_secs(6));
        assert!(got.num_standby_replicas == 1);
        assert!(got.assignor == StreamsAssignorKind::HighlyAvailable);
    }

    #[test]
    fn share_auto_offset_reset_parses_the_three_kafka_forms() {
        // Kafka 4.3.1 types this key STRING with
        // `[latest, earliest, by_duration:PnDTnHnMn.nS]`, defaults it to
        // `latest`, and validates it with
        // `ShareGroupAutoOffsetResetStrategy`, which delegates the duration to
        // `java.time.Duration::parse` and rejects only a negative one.
        for (value, want) in [
            ("latest", Some(ShareAutoOffsetReset::Latest)),
            ("earliest", Some(ShareAutoOffsetReset::Earliest)),
            (
                "by_duration:PT1H",
                Some(ShareAutoOffsetReset::ByDuration(Duration::from_secs(3_600))),
            ),
            (
                "by_duration:P1DT2H3M4.5S",
                Some(ShareAutoOffsetReset::ByDuration(Duration::new(
                    93_784,
                    500_000_000,
                ))),
            ),
            (
                "by_duration:PT0S",
                Some(ShareAutoOffsetReset::ByDuration(Duration::ZERO)),
            ),
            ("by_duration:-PT1H", None),
            ("by_duration:PT-1H", None),
            ("by_duration:PT", None),
            ("by_duration:P", None),
            ("by_duration:", None),
            ("by_duration", None),
            ("by_duration:1H", None),
            ("by_duration:P1H", None),
            ("by_duration:PT1X", None),
            ("by_duration:P1W", None),
            ("by_duration:PT1M1H", None),
            ("Earliest", None),
            ("none", None),
            ("", None),
        ] {
            let overrides =
                maplit::btreemap! {KEY_SHARE_AUTO_OFFSET_RESET.into() => value.to_owned()};
            let got = StreamsGroupConfig::default()
                .with_group_overrides(&overrides)
                .ok()
                .map(|config| config.share_auto_offset_reset);
            assert!(got == want, "{KEY_SHARE_AUTO_OFFSET_RESET}={value}");
        }
    }

    #[test]
    fn share_auto_offset_reset_is_echoed_by_describe_configs() {
        // The default the key reports, and the round trip a configured value
        // takes: `DescribeConfigs` renders the duration the way
        // `java.time.Duration::toString` does, so the value it reports parses
        // back to the same strategy.
        assert!(
            StreamsGroupConfig::default().group_config_values()[KEY_SHARE_AUTO_OFFSET_RESET]
                == "latest"
        );
        for (value, want) in [
            ("earliest", "earliest"),
            ("latest", "latest"),
            ("by_duration:PT1H", "by_duration:PT1H"),
            ("by_duration:P1DT2H3M4.5S", "by_duration:PT26H3M4.5S"),
            ("by_duration:PT0S", "by_duration:PT0S"),
            ("by_duration:pt90m", "by_duration:PT1H30M"),
        ] {
            let overrides =
                maplit::btreemap! {KEY_SHARE_AUTO_OFFSET_RESET.into() => value.to_owned()};
            let reported = StreamsGroupConfig::default()
                .with_group_overrides(&overrides)
                .expect("valid strategy")
                .group_config_values()[KEY_SHARE_AUTO_OFFSET_RESET]
                .clone();
            assert!(reported == want, "{KEY_SHARE_AUTO_OFFSET_RESET}={value}");
            assert!(ShareAutoOffsetReset::parse(&reported) == ShareAutoOffsetReset::parse(value));
        }
    }

    #[test]
    fn share_auto_offset_reset_from_group_overrides_defaults_to_latest() {
        assert!(
            ShareAutoOffsetReset::from_group_overrides(&BTreeMap::new())
                == ShareAutoOffsetReset::Latest
        );
        let configured =
            maplit::btreemap! {KEY_SHARE_AUTO_OFFSET_RESET.into() => "earliest".to_owned()};
        assert!(
            ShareAutoOffsetReset::from_group_overrides(&configured)
                == ShareAutoOffsetReset::Earliest
        );
        // A value this broker cannot parse cannot reach the metadata log
        // through `IncrementalAlterConfigs`; the strategy falls back to the
        // Kafka default rather than failing a fetch.
        let bogus = maplit::btreemap! {KEY_SHARE_AUTO_OFFSET_RESET.into() => "bogus".to_owned()};
        assert!(ShareAutoOffsetReset::from_group_overrides(&bogus) == ShareAutoOffsetReset::Latest);
    }

    #[test]
    fn group_overrides_reject_unknown_and_out_of_bounds_values() {
        let unknown = maplit::btreemap! {"streams.unknown".into() => "1".into()};
        assert!(
            StreamsGroupConfig::default()
                .with_group_overrides(&unknown)
                .is_err()
        );
        let too_short = maplit::btreemap! {KEY_SESSION_TIMEOUT_MS.into() => "1000".into()};
        assert!(
            StreamsGroupConfig::default()
                .with_group_overrides(&too_short)
                .is_err()
        );
    }
}
