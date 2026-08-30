//! The `[audit]` TOML shapes for the `FedRAMP` 20x MLA audit subsystem.
//!
//! [`FileAuditConfig`] and its three subtables — signing, checkpoint cadence,
//! and the durable spool for the AU-5 degraded path — all default to the
//! secure setting, so a broker with no `[audit]` block still audits to the
//! standard internal topic.

use std::num::NonZeroU64;

use krabka_units::convert::{ByteSizeExt as _, TimeExt as _};
use schemars::JsonSchema;
use serde::Deserialize;

/// `[audit]` section of `broker.toml` (`FedRAMP` 20x MLA).
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuditConfig {
    /// Whether the audit subsystem is active.
    #[serde(default = "default_audit_enabled")]
    pub enabled: bool,
    /// Whether privileged operations continue when audit processing fails.
    #[serde(default = "default_audit_failure_mode")]
    #[schemars(with = "String")]
    pub failure_mode: krabka_audit::AuditMode,
    /// Internal topic name for audit records.
    #[serde(default = "default_audit_topic")]
    pub topic: String,
    /// Ed25519 checkpoint signing key. `None` → chaining only, no checkpoints.
    pub signing: Option<FileAuditSigningConfig>,
    /// Checkpoint emission cadence. `None` → use defaults.
    pub checkpoint: Option<FileAuditCheckpointConfig>,
    /// Durable spool for the AU-5 degraded path. `None` → use defaults.
    pub spool: Option<FileAuditSpoolConfig>,
}

impl Default for FileAuditConfig {
    fn default() -> Self {
        Self {
            enabled: default_audit_enabled(),
            failure_mode: krabka_audit::AuditMode::FailOpen,
            topic: default_audit_topic(),
            signing: None,
            checkpoint: None,
            spool: None,
        }
    }
}

/// `[audit.spool]` — durable spool for the AU-5 degraded path.
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuditSpoolConfig {
    #[serde(default = "default_spool_dir")]
    pub dir: String,
    #[serde(default = "default_spool_max_bytes")]
    pub max_bytes: u64,
    /// Number of appended records between durable file syncs.
    #[serde(default = "default_spool_sync_every_n")]
    pub sync_every_n: NonZeroU64,
}

impl Default for FileAuditSpoolConfig {
    fn default() -> Self {
        Self {
            dir: default_spool_dir(),
            max_bytes: default_spool_max_bytes(),
            sync_every_n: default_spool_sync_every_n(),
        }
    }
}

fn default_spool_dir() -> String {
    crate::config::DEFAULT_AUDIT_SPOOL_DIR.to_string()
}

fn default_spool_max_bytes() -> u64 {
    crate::config::DEFAULT_AUDIT_SPOOL_MAX.bytes_u64()
}

fn default_spool_sync_every_n() -> NonZeroU64 {
    crate::config::DEFAULT_AUDIT_SPOOL_SYNC_EVERY_N
}

/// `[audit.signing]` — Ed25519 checkpoint signing key.
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuditSigningConfig {
    pub key_path: String,
    pub key_id: String,
}

/// `[audit.checkpoint]` — checkpoint cadence.
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuditCheckpointConfig {
    #[serde(default = "default_checkpoint_every_n")]
    pub every_n: u64,
    #[serde(default = "default_checkpoint_every_secs")]
    pub every_secs: u64,
}

impl Default for FileAuditCheckpointConfig {
    fn default() -> Self {
        Self {
            every_n: default_checkpoint_every_n(),
            every_secs: default_checkpoint_every_secs(),
        }
    }
}

fn default_checkpoint_every_n() -> u64 {
    crate::config::DEFAULT_AUDIT_CHECKPOINT_EVERY_N
}

fn default_checkpoint_every_secs() -> u64 {
    crate::config::DEFAULT_AUDIT_CHECKPOINT_EVERY
        .secs_i64()
        .cast_unsigned()
}

fn default_audit_enabled() -> bool {
    true
}

fn default_audit_failure_mode() -> krabka_audit::AuditMode {
    krabka_audit::AuditMode::FailOpen
}

fn default_audit_topic() -> String {
    crate::config::DEFAULT_AUDIT_TOPIC.to_string()
}

#[cfg(test)]
mod tests {
    use krabka_units::secs;

    use crate::file_config::FileConfig;

    #[test]
    fn audit_section_parses_and_applies() {
        let toml = r#"
            [audit]
            enabled = true
            topic = "__krabka_audit"
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse audit section");
        let audit = fc.audit.clone().expect("audit present");
        assert2::check!(audit.enabled);
        assert2::check!(audit.topic == "__krabka_audit");

        let mut cfg = crate::config::BrokerConfig::for_tests(std::path::PathBuf::from("/tmp/x"));
        fc.apply_to(&mut cfg).expect("apply");
        assert2::check!(cfg.audit_enabled);
        assert2::check!(cfg.audit_failure_mode == krabka_audit::AuditMode::FailOpen);
        assert2::check!(cfg.audit_topic == "__krabka_audit");
    }

    #[test]
    fn audit_defaults_to_enabled_with_internal_topic() {
        // Absent [audit] section → secure default (enabled, standard topic name).
        let fc: FileConfig = toml::from_str("").expect("parse empty");
        let mut cfg = crate::config::BrokerConfig::for_tests(std::path::PathBuf::from("/tmp/x"));
        fc.apply_to(&mut cfg).expect("apply");
        assert2::check!(cfg.audit_enabled);
        assert2::check!(cfg.audit_failure_mode == krabka_audit::AuditMode::FailOpen);
        assert2::check!(cfg.audit_topic == "__krabka_audit");
    }

    #[test]
    fn audit_signing_and_checkpoint_parse_and_apply() {
        let toml = r#"
            [audit]
            enabled = true

            [audit.signing]
            key_path = "/etc/krabka/audit.pk8"
            key_id = "audit-2026"

            [audit.checkpoint]
            every_n = 500
            every_secs = 30
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::for_tests(std::path::PathBuf::from("/tmp/x"));
        fc.apply_to(&mut cfg).expect("apply");
        assert2::check!(
            cfg.audit_signing_key_path == Some(std::path::PathBuf::from("/etc/krabka/audit.pk8"))
        );
        assert2::check!(cfg.audit_signing_key_id.as_deref() == Some("audit-2026"));
        assert2::check!(cfg.audit_checkpoint_every_n == 500);
        assert2::check!(cfg.audit_checkpoint_every == secs(30));
    }

    #[test]
    fn audit_checkpoint_has_sane_defaults_when_absent() {
        let fc: FileConfig = toml::from_str("[audit]\nenabled = true\n").expect("parse");
        let mut cfg = crate::config::BrokerConfig::for_tests(std::path::PathBuf::from("/tmp/x"));
        fc.apply_to(&mut cfg).expect("apply");
        assert2::check!(cfg.audit_signing_key_path == None);
        assert2::check!(cfg.audit_signing_key_id == None);
        assert2::check!(cfg.audit_checkpoint_every_n == 1000);
        assert2::check!(cfg.audit_checkpoint_every == secs(60));
    }

    #[test]
    fn audit_spool_parses_and_defaults() {
        let toml = r#"
            [audit]
            enabled = true
            failure_mode = "fail-closed"
            [audit.spool]
            dir = "/var/lib/krabka/audit-spool"
            max_bytes = 2048
            sync_every_n = 7
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::for_tests(std::path::PathBuf::from("/tmp/x"));
        fc.apply_to(&mut cfg).expect("apply");
        assert2::check!(
            cfg.audit_spool_dir == std::path::PathBuf::from("/var/lib/krabka/audit-spool")
        );
        assert2::check!(cfg.audit_spool_max == krabka_units::kibibytes(2));
        assert2::check!(cfg.audit_failure_mode == krabka_audit::AuditMode::FailClosed);
        assert2::check!(cfg.audit_spool_sync_every_n.get() == 7);

        let fc2: FileConfig = toml::from_str("[audit]\nenabled = true\n").expect("parse");
        let mut cfg2 = crate::config::BrokerConfig::for_tests(std::path::PathBuf::from("/tmp/x"));
        fc2.apply_to(&mut cfg2).expect("apply");
        assert2::check!(cfg2.audit_spool_dir == std::path::PathBuf::from("audit-spool"));
        assert2::check!(cfg2.audit_spool_max == krabka_units::gibibytes(1));
        assert2::check!(cfg2.audit_spool_sync_every_n.get() == 1);
    }

    #[test]
    fn audit_spool_rejects_zero_sync_cadence() {
        assert2::check!(toml::from_str::<FileConfig>("[audit.spool]\nsync_every_n = 0\n").is_err());
    }
}
