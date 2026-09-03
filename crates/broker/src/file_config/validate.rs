//! Scalar range, unit, and whole-number validators for file-config values.
//!
//! Each function takes the TOML key it guards so a rejection names the field
//! the operator wrote, and returns [`FileConfigError::InvalidConfig`] with that
//! name when the value is out of range. They live in one module because the
//! `[runtime]` appliers, the `set_runtime_*` macros, and the section appliers
//! all draw on the same set.

use krabka_units::{
    ByteSize, Ratio, Time,
    convert::{ByteSizeExt as _, RatioExt as _, TimeExt as _},
};

use super::FileConfigError;

pub(super) fn invalid_runtime_value(name: &str, error: impl std::fmt::Display) -> FileConfigError {
    FileConfigError::InvalidConfig(format!("{name}: {error}"))
}

pub(super) fn positive_u64(name: &str, value: u64) -> Result<u64, FileConfigError> {
    refined_type::rule::GreaterU64::<0>::new(value)
        .map(refined_type::Refined::into_value)
        .map_err(|error| invalid_runtime_value(name, error))
}

pub(super) fn positive_i32(name: &str, value: i32) -> Result<i32, FileConfigError> {
    crate::config_value::PositiveI32::new(value)
        .map(crate::config_value::PositiveI32::into_value)
        .map_err(|error| invalid_runtime_value(name, error))
}

pub(super) fn positive_i64(name: &str, value: i64) -> Result<i64, FileConfigError> {
    crate::config_value::PositiveI64::new(value)
        .map(crate::config_value::PositiveI64::into_value)
        .map_err(|error| invalid_runtime_value(name, error))
}

pub(super) fn positive_usize(name: &str, value: usize) -> Result<usize, FileConfigError> {
    crate::config_value::PositiveCount::new(value)
        .map(crate::config_value::PositiveCount::into_value)
        .map_err(|error| invalid_runtime_value(name, error))
}

pub(super) fn positive_u32(name: &str, value: u32) -> Result<u32, FileConfigError> {
    let count = usize::try_from(value).map_err(|error| invalid_runtime_value(name, error))?;
    positive_usize(name, count)?;
    Ok(value)
}

pub(super) fn whole_bytes_u64(name: &str, value: ByteSize) -> Result<ByteSize, FileConfigError> {
    let bytes = value.bytes_u64();
    if value.bytes_f64().is_finite()
        && value > ByteSize::from_bytes(0)
        && value.bytes_f64() < 18_446_744_073_709_551_616.0
        && ByteSize::from_bytes(bytes) == value
    {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be a positive whole number of bytes within the u64 range",
        ))
    }
}

pub(super) fn metadata_snapshot_fetch_max(
    name: &str,
    value: ByteSize,
) -> Result<ByteSize, FileConfigError> {
    let value = whole_bytes_u64(name, value)?;
    krabka_kraft_core::snapshot_fetch::MetadataSnapshotFetchMax::new(value)
        .map(|_| value)
        .map_err(|error| invalid_runtime_value(name, error))
}

/// A byte count in the domain Kafka gives an `INT` config with `atLeast(0)`.
///
/// `apache/kafka:4.3.1` starts on `message.max.bytes=0`, refuses `-1` with
/// "Invalid value -1 for configuration message.max.bytes: Value must be at
/// least 0", and refuses `2147483648` with "Not a number of type INT". The
/// topic-level `max.message.bytes` is the same `INT` and the broker-wide key
/// is the default behind it, so one domain covers the TOML file, the command
/// line, and `kafka-configs --alter` alike.
///
/// This is [`whole_bytes_i32`] with zero allowed, which is why it does not go
/// through `whole_bytes_u64`: that helper rejects zero.
pub(super) fn kafka_int_bytes(name: &str, value: ByteSize) -> Result<ByteSize, FileConfigError> {
    let bytes = value.bytes_u64();
    if value.bytes_f64().is_finite()
        && value >= ByteSize::from_bytes(0)
        && ByteSize::from_bytes(bytes) == value
        && bytes <= u64::try_from(i32::MAX).expect("i32::MAX fits u64")
    {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be a whole number of bytes from 0 to 2147483647",
        ))
    }
}

pub(super) fn whole_bytes_i32(name: &str, value: ByteSize) -> Result<ByteSize, FileConfigError> {
    let value = whole_bytes_u64(name, value)?;
    if value.bytes_u64() <= u64::try_from(i32::MAX).expect("i32::MAX fits u64") {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be at most 2147483647 bytes",
        ))
    }
}

pub(super) fn whole_bytes_u32(name: &str, value: ByteSize) -> Result<ByteSize, FileConfigError> {
    let value = whole_bytes_u64(name, value)?;
    if u32::try_from(value.bytes_u64()).is_ok() {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be at most 4294967295 bytes",
        ))
    }
}

pub(super) fn whole_bytes_usize(name: &str, value: ByteSize) -> Result<ByteSize, FileConfigError> {
    let value = whole_bytes_u64(name, value)?;
    usize::try_from(value.bytes_u64())
        .map(|_| value)
        .map_err(|error| invalid_runtime_value(name, error))
}

pub(super) fn positive_ratio(name: &str, value: Ratio) -> Result<Ratio, FileConfigError> {
    if value.as_f64().is_finite() && value > krabka_units::fraction(0.0) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(name, "must be finite and positive"))
    }
}

pub(super) fn unit_interval_ratio(name: &str, value: Ratio) -> Result<Ratio, FileConfigError> {
    if value.as_f64().is_finite()
        && value >= krabka_units::fraction(0.0)
        && value <= krabka_units::fraction(1.0)
    {
        Ok(value)
    } else {
        Err(invalid_runtime_value(name, "must be between 0% and 100%"))
    }
}

pub(super) fn positive_time(name: &str, value: Time) -> Result<Time, FileConfigError> {
    if value.secs_f64().is_finite() && value > Time::from_secs(0) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(name, "must be finite and positive"))
    }
}

pub(super) fn nonnegative_time(name: &str, value: Time) -> Result<Time, FileConfigError> {
    if value.secs_f64().is_finite() && value >= Time::from_secs(0) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be finite and nonnegative",
        ))
    }
}

pub(super) fn voter_request_time(name: &str, value: Time) -> Result<Time, FileConfigError> {
    whole_millis_i32_time(name, value)
}

pub(super) fn whole_millis_i32_time(name: &str, value: Time) -> Result<Time, FileConfigError> {
    let value = whole_millis_i64_time(name, value)?;
    let millis = value.millis_i64();
    if (1..=i64::from(i32::MAX)).contains(&millis) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be within 1ms..=2147483647ms",
        ))
    }
}

/// A whole number of milliseconds Kafka's `ConfigDef.Type::INT` can hold,
/// where zero is a meaningful value rather than a floor violation.
///
/// [`whole_millis_i32_time`] floors at 1ms, which is what Kafka's `atLeast(1)`
/// states for a knob that must always run. A krabka cadence that zero disables
/// needs the same 32-bit ceiling with 0 accepted, so that the value the broker
/// runs on is one `DescribeConfigs` can report as an `INT`.
pub(super) fn disableable_millis_i32_time(
    name: &str,
    value: Time,
) -> Result<Time, FileConfigError> {
    let value = nonnegative_time(name, value)?;
    let millis = value.millis_i64();
    if Time::from_millis(millis) != value {
        return Err(invalid_runtime_value(
            name,
            "must be a whole number of milliseconds",
        ));
    }
    if (0..=i64::from(i32::MAX)).contains(&millis) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be within 0ms..=2147483647ms",
        ))
    }
}

pub(super) fn whole_millis_i64_time(name: &str, value: Time) -> Result<Time, FileConfigError> {
    let value = positive_time(name, value)?;
    let millis = value.millis_i64();
    if Time::from_millis(millis) == value {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be a whole number of milliseconds",
        ))
    }
}

pub(super) fn positive_i16(name: &str, value: i16) -> Result<i16, FileConfigError> {
    positive_i32(name, i32::from(value))?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use crate::file_config::FileConfig;

    #[test]
    fn runtime_file_config_rejects_zero_and_names_field() {
        let file: FileConfig = toml::from_str("[runtime]\ncleaner_interval = \"0ms\"\n")
            .expect("parse runtime config");
        let mut cfg = crate::config::BrokerConfig::default();

        let error = file
            .apply_to(&mut cfg)
            .expect_err("zero cleaner interval must fail");

        assert!(error.to_string().contains("cleaner_interval"));
    }

    #[test]
    fn runtime_file_config_rejects_voter_timeout_above_wire_limit() {
        let file: FileConfig =
            toml::from_str("[runtime]\nauto_join_voter_request_timeout = \"2147483648ms\"\n")
                .expect("parse runtime config");
        let mut cfg = crate::config::BrokerConfig::default();

        let error = file
            .apply_to(&mut cfg)
            .expect_err("timeout above i32 wire limit must fail");

        assert!(
            error
                .to_string()
                .contains("auto_join_voter_request_timeout")
        );
    }

    #[test]
    fn runtime_file_config_rejects_fractional_protocol_milliseconds() {
        for (field, source) in [
            (
                "client_metrics_default_interval",
                "[runtime]\nclient_metrics_default_interval = \"1.5ms\"\n",
            ),
            (
                "producer_id_expiration",
                "[runtime]\nproducer_id_expiration = \"1.5ms\"\n",
            ),
        ] {
            let file: FileConfig = toml::from_str(source).expect("parse runtime config");
            let mut cfg = crate::config::BrokerConfig::default();
            let error = file
                .apply_to(&mut cfg)
                .expect_err("fractional protocol milliseconds must fail");
            assert!(error.to_string().contains(field));
        }
    }
    /// Both KIP-98 expiry knobs are Kafka `ConfigDef.Type::INT`, so a value
    /// wider than an `i32` of milliseconds, or a fractional one, is refused
    /// with the field named. The expiry itself must run, matching Kafka's
    /// `atLeast(1)`; zero on the sweep cadence disables the sweep and is
    /// accepted.
    #[test]
    fn runtime_file_config_bounds_the_transactional_id_expiry_knobs_to_an_i32() {
        for (label, source, field) in [
            (
                "an expiry above the i32 millisecond ceiling",
                "[runtime]\ntxn_id_expiration = \"2147483648ms\"\n",
                Some("txn_id_expiration"),
            ),
            (
                "a zero expiry, which Kafka's atLeast(1) refuses",
                "[runtime]\ntxn_id_expiration = \"0ms\"\n",
                Some("txn_id_expiration"),
            ),
            (
                "a fractional expiry",
                "[runtime]\ntxn_id_expiration = \"1.5ms\"\n",
                Some("txn_id_expiration"),
            ),
            (
                "a sweep cadence above the i32 millisecond ceiling",
                "[runtime]\ntxn_id_expiration_cleanup_interval = \"2147483648ms\"\n",
                Some("txn_id_expiration_cleanup_interval"),
            ),
            (
                "a zero sweep cadence, which disables the sweep",
                "[runtime]\ntxn_id_expiration_cleanup_interval = \"0ms\"\n",
                None,
            ),
        ] {
            let file: FileConfig = toml::from_str(source).expect("parse runtime config");
            let mut cfg = crate::config::BrokerConfig::default();
            let result = file.apply_to(&mut cfg);

            if let Some(field) = field {
                let error = result.expect_err(label);
                check!(error.to_string().contains(field), "{label}");
            } else {
                check!(result.is_ok(), "{label}");
                check!(
                    cfg.txn_id_expiration_cleanup_interval
                        == <krabka_units::Time as krabka_units::convert::TimeExt>::ZERO,
                    "{label}"
                );
            }
        }
    }

    /// The loader records *that* the operator supplied each expiry knob, not
    /// only the value, because `DescribeConfigs` reports a config source and a
    /// source is provenance. Supplying Kafka's own default is the case that
    /// separates the two: `apache/kafka:4.3.1` still reports
    /// `STATIC_BROKER_CONFIG` for it, so krabka has to know the key was named.
    #[test]
    fn runtime_file_config_records_which_expiry_knobs_the_operator_supplied() {
        for (label, source, expected) in [
            (
                "nothing supplied",
                "[runtime]\n",
                crate::config::StaticConfigOrigins {
                    txn_id_expiration: false,
                    txn_id_expiration_cleanup_interval: false,
                },
            ),
            (
                "the expiry supplied, at Kafka's own default value",
                "[runtime]\ntxn_id_expiration = \"604800000ms\"\n",
                crate::config::StaticConfigOrigins {
                    txn_id_expiration: true,
                    txn_id_expiration_cleanup_interval: false,
                },
            ),
            (
                "both supplied",
                "[runtime]\ntxn_id_expiration = \"120000ms\"\n\
                 txn_id_expiration_cleanup_interval = \"60000ms\"\n",
                crate::config::StaticConfigOrigins {
                    txn_id_expiration: true,
                    txn_id_expiration_cleanup_interval: true,
                },
            ),
        ] {
            let file: FileConfig = toml::from_str(source).expect("parse runtime config");
            let mut cfg = crate::config::BrokerConfig::default();
            file.apply_to(&mut cfg).expect("apply runtime config");

            check!(cfg.static_config_origins == expected, "{label}");
        }
    }

    #[test]
    fn runtime_file_config_rejects_invalid_dimensioned_sizes_and_ratios() {
        for field in [
            "client_metrics_telemetry_max",
            "replication_fetch_max",
            "replication_fetch_min",
            "observer_fetch_max",
            "audit_tail_read_max",
            "share_recovery_read_max",
            "socket_request_max",
            "sendfile_min",
            "socket_send_buffer",
            "socket_receive_buffer",
            "acl_max_principal",
            "acl_max_resource_name",
            "telemetry_decompressed_output_floor",
            "telemetry_decompressed_output_ceiling",
            "record_decompression_output_floor",
            "record_decompression_output_ceiling",
            // `message_max_bytes` is deliberately absent: Kafka declares it an
            // `INT` with `atLeast(0)` and `apache/kafka:4.3.1` boots on
            // `message.max.bytes=0`, so zero is a value and not an error.
            // `message_max_bytes_takes_kafkas_int_at_least_zero` in
            // `runtime_storage` covers its whole domain.
            "future_log_move_read_chunk",
            "metadata_max_bytes_between_snapshots",
            "metadata_snapshot_fetch_max",
        ] {
            let source = format!("[runtime]\n{field} = \"0B\"\n");
            let file: FileConfig = toml::from_str(&source).expect("parse runtime config");
            let mut cfg = crate::config::BrokerConfig::default();
            let error = file
                .apply_to(&mut cfg)
                .expect_err("zero byte size must fail");
            assert!(error.to_string().contains(field), "{error}");
        }

        for (field, value) in [
            ("telemetry_max_decompression_ratio", "0"),
            ("record_decompression_max_ratio", "0"),
            ("leader_imbalance_per_broker", "101%"),
        ] {
            let source = format!("[runtime]\n{field} = \"{value}\"\n");
            let file: FileConfig = toml::from_str(&source).expect("parse runtime config");
            let mut cfg = crate::config::BrokerConfig::default();
            let error = file
                .apply_to(&mut cfg)
                .expect_err("invalid ratio must fail");
            assert!(error.to_string().contains(field), "{error}");
        }
    }

    #[test]
    fn runtime_file_config_rejects_fractional_and_overflowing_sizes() {
        for (field, value) in [
            ("client_metrics_telemetry_max", "1.5B"),
            ("replication_fetch_max", "2147483648B"),
            ("observer_fetch_max", "4294967296B"),
            ("socket_request_max", "4294967296B"),
            ("audit_tail_read_max", "1.5B"),
            ("record_decompression_output_floor", "1.5B"),
            ("record_decompression_output_ceiling", "1073741825B"),
            (
                "metadata_max_bytes_between_snapshots",
                "18446744073709551616B",
            ),
            ("metadata_snapshot_fetch_max", "1.5B"),
            ("metadata_snapshot_fetch_max", "1073741825B"),
            ("message_max_bytes", "1.5B"),
            ("message_max_bytes", "2147483648B"),
        ] {
            let source = format!("[runtime]\n{field} = \"{value}\"\n");
            let file: FileConfig = toml::from_str(&source).expect("parse runtime config");
            let mut cfg = crate::config::BrokerConfig::default();
            let error = file
                .apply_to(&mut cfg)
                .expect_err("fractional or overflowing byte size must fail");
            let expected = if field == "record_decompression_output_ceiling" {
                "record_decompression"
            } else {
                field
            };
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn existing_file_inputs_reject_invalid_refined_values() {
        let cases = [
            ("heartbeat_interval = \"0ms\"\n", "heartbeat_interval"),
            (
                "[runtime]\nstreams_internal_topic_replication_factor = 0\n",
                "streams_internal_topic_replication_factor",
            ),
            (
                "[delegation_token]\nmax_lifetime_ms = 0\n",
                "max_lifetime_ms",
            ),
        ];

        for (source, field) in cases {
            let file: FileConfig = toml::from_str(source).expect("parse config");
            let mut cfg = crate::config::BrokerConfig::default();
            let error = file.apply_to(&mut cfg).expect_err("zero must fail");
            assert!(error.to_string().contains(field));
        }
    }
}
