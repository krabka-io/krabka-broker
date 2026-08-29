//! The profiling policy: the `clap`-flattened `ProfilingConfig`, the validated
//! sampling frequency it carries, and the `ProfilingError` a bad policy raises.
//!
//! The policy lives apart from the routes and the admin server so that a
//! service can parse and validate its profiling settings without building a
//! router or binding a port.

use std::str::FromStr;

use clap::Args;
use krabka_units::{Frequency, Time, convert::FrequencyExt as _, parse, per_sec, secs};
use refined_type::rule::GreaterI32;
use thiserror::Error;

type RefinedPositiveFrequency = GreaterI32<0>;

/// Profiling configuration or admin-server failure.
#[derive(Debug, Error)]
pub enum ProfilingError {
    #[error("invalid profiling configuration: {0}")]
    Config(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// A positive, finite, whole-Hz sampling frequency accepted by `pprof`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ProfilingSampleFrequency {
    frequency: Frequency,
    hertz: i32,
}

impl ProfilingSampleFrequency {
    /// Validate a profiling sampling frequency.
    ///
    /// # Errors
    /// Returns an error unless the frequency is positive, finite, whole Hz,
    /// and representable by `pprof`'s signed frequency input.
    pub fn new(frequency: Frequency) -> Result<Self, String> {
        let hertz = frequency.per_sec_f64();
        if !hertz.is_finite() || hertz.fract() != 0.0 || hertz > f64::from(i32::MAX) {
            return Err("profiling sample frequency must be finite whole Hz".to_string());
        }
        let hertz = i32::try_from(frequency.per_sec_u64())
            .map_err(|_| "profiling sample frequency exceeds i32".to_string())?;
        RefinedPositiveFrequency::new(hertz)
            .map_err(|error| format!("profiling sample frequency: {error}"))?;
        Ok(Self { frequency, hertz })
    }

    #[cfg(unix)]
    pub(super) fn hertz(self) -> i32 {
        self.hertz
    }

    /// Return the dimensioned sampling frequency.
    #[must_use]
    pub fn frequency(self) -> Frequency {
        self.frequency
    }
}

impl FromStr for ProfilingSampleFrequency {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(parse::frequency(value).map_err(|error| error.to_string())?)
    }
}

/// Process-local CPU and heap profiling policy.
#[derive(Args, Clone, Debug, PartialEq)]
pub struct ProfilingConfig {
    #[arg(long, env = "KRABKA_PROFILING_CPU_DEFAULT_DURATION", default_value = "30s", value_parser = parse::positive_time)]
    pub profiling_cpu_default_duration: Time,
    #[arg(long, env = "KRABKA_PROFILING_CPU_MAX_DURATION", default_value = "60s", value_parser = parse::positive_time)]
    pub profiling_cpu_max_duration: Time,
    #[arg(
        long,
        env = "KRABKA_PROFILING_CPU_SAMPLE_FREQUENCY",
        default_value = "99Hz"
    )]
    pub profiling_cpu_sample_frequency: ProfilingSampleFrequency,
    #[arg(long, env = "KRABKA_PROFILING_HEAP_DEFAULT_DURATION", default_value = "5s", value_parser = parse::positive_time)]
    pub profiling_heap_default_duration: Time,
    #[arg(long, env = "KRABKA_PROFILING_HEAP_MAX_DURATION", default_value = "30s", value_parser = parse::positive_time)]
    pub profiling_heap_max_duration: Time,
    #[arg(
        long,
        env = "KRABKA_PROFILING_NATIVE_FRAME_BLOCKLIST",
        default_value = "libc,libgcc,pthread,vdso",
        value_delimiter = ','
    )]
    pub profiling_native_frame_blocklist: Vec<String>,
}

impl ProfilingConfig {
    /// Validate related profiling bounds.
    ///
    /// # Errors
    /// Returns an error when a default exceeds its maximum. Returns an error
    /// when a maximum is below the compatible one-second request floor.
    pub fn validate(&self) -> Result<(), String> {
        if self.profiling_cpu_default_duration > self.profiling_cpu_max_duration {
            return Err("profiling CPU default duration exceeds maximum".to_string());
        }
        if self.profiling_heap_default_duration > self.profiling_heap_max_duration {
            return Err("profiling heap default duration exceeds maximum".to_string());
        }
        if self.profiling_cpu_max_duration < secs(1) || self.profiling_heap_max_duration < secs(1) {
            return Err("profiling maximum duration must be at least 1s".to_string());
        }
        Ok(())
    }
}

impl Default for ProfilingConfig {
    fn default() -> Self {
        Self {
            profiling_cpu_default_duration: secs(30),
            profiling_cpu_max_duration: secs(60),
            profiling_cpu_sample_frequency: ProfilingSampleFrequency {
                frequency: per_sec(99),
                hertz: 99,
            },
            profiling_heap_default_duration: secs(5),
            profiling_heap_max_duration: secs(30),
            profiling_native_frame_blocklist: vec![
                "libc".to_string(),
                "libgcc".to_string(),
                "pthread".to_string(),
                "vdso".to_string(),
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use krabka_units::convert::FrequencyExt as _;

    use super::*;

    #[derive(Debug, Parser)]
    struct TestCli {
        #[command(flatten)]
        profiling: ProfilingConfig,
    }

    #[test]
    fn profiling_config_defaults_and_overrides() {
        let defaults = TestCli::parse_from(["test"]).profiling;
        assert_eq!(defaults, ProfilingConfig::default());

        let configured = TestCli::try_parse_from([
            "test",
            "--profiling-cpu-default-duration=2s",
            "--profiling-cpu-max-duration=3s",
            "--profiling-cpu-sample-frequency=101Hz",
            "--profiling-heap-default-duration=4s",
            "--profiling-heap-max-duration=5s",
            "--profiling-native-frame-blocklist=libc,custom",
        ])
        .expect("valid profiling policy")
        .profiling;
        assert_eq!(configured.profiling_cpu_default_duration, secs(2));
        assert_eq!(configured.profiling_cpu_max_duration, secs(3));
        assert_eq!(
            configured.profiling_cpu_sample_frequency.frequency(),
            Frequency::from_per_sec(101.0)
        );
        assert_eq!(configured.profiling_heap_default_duration, secs(4));
        assert_eq!(configured.profiling_heap_max_duration, secs(5));
        assert_eq!(
            configured.profiling_native_frame_blocklist,
            ["libc", "custom"]
        );
        assert!(configured.validate().is_ok());
    }

    #[test]
    fn profiling_config_rejects_invalid_values_and_bounds() {
        for argument in [
            "--profiling-cpu-default-duration=0s",
            "--profiling-cpu-max-duration=-1s",
            "--profiling-cpu-sample-frequency=0Hz",
            "--profiling-cpu-sample-frequency=1.5Hz",
            "--profiling-heap-default-duration=0s",
            "--profiling-heap-max-duration=-1s",
        ] {
            assert!(TestCli::try_parse_from(["test", argument]).is_err());
        }

        let cpu_bounds = TestCli::parse_from([
            "test",
            "--profiling-cpu-default-duration=2s",
            "--profiling-cpu-max-duration=1s",
        ]);
        assert!(cpu_bounds.profiling.validate().is_err());

        let heap_bounds = TestCli::parse_from([
            "test",
            "--profiling-heap-default-duration=2s",
            "--profiling-heap-max-duration=1s",
        ]);
        assert!(heap_bounds.profiling.validate().is_err());
    }
}
