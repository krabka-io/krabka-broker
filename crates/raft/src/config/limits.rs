//! The validated scalar policies a controller's runtime is spelled in.
//!
//! `ControllerFetchMissLimit`, `MetadataRaftCommandQueueCapacity`, and
//! `MetadataRaftFetchMax` each wrap one number that has to stay in range, so a
//! value the broker's configuration names is refused where it is parsed rather
//! than where it is used. They live apart from `ControllerConfig` because the
//! validation, the `FromStr` the configuration file goes through, and the
//! `Display` that renders the value back are a concern of their own. The
//! defaults they fall back to stay with the rest of the configuration
//! defaults, in the parent module.

use std::{fmt, str::FromStr};

use krabka_units::{
    fmt::Human as _,
    prelude::{ByteSize, ByteSizeExt as _},
};
use refined_type::rule::{GreaterI32, GreaterU32, GreaterUsize};

use super::{
    DEFAULT_CONTROLLER_FETCH_MISS_LIMIT, DEFAULT_METADATA_RAFT_COMMAND_QUEUE_CAPACITY,
    DEFAULT_METADATA_RAFT_FETCH_MAX,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControllerFetchMissLimit(u32);

impl ControllerFetchMissLimit {
    /// Validate the consecutive fetch-miss limit.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: u32) -> Result<Self, String> {
        GreaterU32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("controller fetch miss limit: {error}"))
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl Default for ControllerFetchMissLimit {
    fn default() -> Self {
        Self::new(DEFAULT_CONTROLLER_FETCH_MISS_LIMIT)
            .expect("default controller fetch miss limit is positive")
    }
}

impl FromStr for ControllerFetchMissLimit {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

impl fmt::Display for ControllerFetchMissLimit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataRaftCommandQueueCapacity(usize);

impl MetadataRaftCommandQueueCapacity {
    /// Validate the metadata Raft command queue capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: usize) -> Result<Self, String> {
        GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("metadata raft command queue capacity: {error}"))
    }

    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

impl Default for MetadataRaftCommandQueueCapacity {
    fn default() -> Self {
        Self::new(DEFAULT_METADATA_RAFT_COMMAND_QUEUE_CAPACITY)
            .expect("default metadata raft command queue capacity is positive")
    }
}

impl FromStr for MetadataRaftCommandQueueCapacity {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

impl fmt::Display for MetadataRaftCommandQueueCapacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataRaftFetchMax(i32);

impl MetadataRaftFetchMax {
    /// Validate the protocol byte count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or negative.
    pub fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("metadata raft fetch max: {error}"))
    }

    #[must_use]
    pub const fn bytes(self) -> i32 {
        self.0
    }

    #[must_use]
    pub fn size(self) -> ByteSize {
        ByteSize::from_bytes_i64(i64::from(self.0))
    }
}

impl TryFrom<ByteSize> for MetadataRaftFetchMax {
    type Error = String;

    fn try_from(value: ByteSize) -> Result<Self, Self::Error> {
        let bytes = value.bytes_f64();
        if !bytes.is_finite()
            || bytes.fract() != 0.0
            || !(1.0..=f64::from(i32::MAX)).contains(&bytes)
        {
            return Err(
                "metadata raft fetch max must be a positive whole-byte value that fits i32"
                    .to_owned(),
            );
        }
        Self::new(value.bytes_i32())
    }
}

impl Default for MetadataRaftFetchMax {
    fn default() -> Self {
        Self::try_from(DEFAULT_METADATA_RAFT_FETCH_MAX)
            .expect("default metadata raft fetch max is protocol-safe")
    }
}

impl FromStr for MetadataRaftFetchMax {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        krabka_units::parse::byte_size(value)
            .map_err(|error| error.to_string())?
            .try_into()
    }
}

impl fmt::Display for MetadataRaftFetchMax {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.size().human().fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::{ByteSizeExt as _, mebibytes};

    use super::*;

    #[test]
    fn raft_runtime_policy_defaults_and_validation() {
        check!(ControllerFetchMissLimit::default().get() == 3);
        check!(ControllerFetchMissLimit::new(0).is_err());
        check!(
            "7".parse::<ControllerFetchMissLimit>()
                .expect("positive miss limit")
                .get()
                == 7
        );

        check!(MetadataRaftCommandQueueCapacity::default().get() == 256);
        check!(MetadataRaftCommandQueueCapacity::new(0).is_err());
        check!(
            "512"
                .parse::<MetadataRaftCommandQueueCapacity>()
                .expect("positive command queue capacity")
                .get()
                == 512
        );

        check!(MetadataRaftFetchMax::default().size() == mebibytes(8));
        check!(MetadataRaftFetchMax::try_from(ByteSize::from_bytes_i64(0)).is_err());
        check!(
            "4MiB"
                .parse::<MetadataRaftFetchMax>()
                .expect("positive whole-byte fetch maximum")
                .bytes()
                == 4 * 1024 * 1024
        );
        check!(MetadataRaftFetchMax::try_from(ByteSize::from_bytes_f64(1.5)).is_err());
        check!(
            MetadataRaftFetchMax::try_from(ByteSize::from_bytes_i64(i64::from(i32::MAX) + 1))
                .is_err()
        );
    }
}
