//! Shared parse-and-apply setup for file configuration tests.

use super::{FileConfig, FileConfigError};
use crate::config::BrokerConfig;

pub(super) fn configured(
    source: &str,
    parse_context: &str,
) -> Result<BrokerConfig, FileConfigError> {
    apply(toml::from_str(source).expect(parse_context))
}

/// The setup for tests that originally used `unwrap` on the TOML parse.
pub(super) fn configured_unwrap_parse(source: &str) -> Result<BrokerConfig, FileConfigError> {
    apply(toml::from_str(source).unwrap())
}

fn apply(file: FileConfig) -> Result<BrokerConfig, FileConfigError> {
    let mut config = BrokerConfig::default();
    file.apply_to(&mut config)?;
    Ok(config)
}
