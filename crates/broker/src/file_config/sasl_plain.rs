//! The `[sasl_plain]` TOML shape and how it reaches the broker config.
//!
//! Kafka reads PLAIN server credentials from the JAAS `PlainLoginModule`
//! entry, one `user_<name>="<password>"` option per user, and has no dynamic
//! path for them. This section is the file-config equivalent: a path to a
//! credential file, kept out of the TOML so the secret can be a mounted
//! `Secret` rather than a config-map value.

use schemars::JsonSchema;
use serde::Deserialize;

use super::FileConfigError;

/// TOML shape of `[sasl_plain]`. Maps to
/// [`crate::BrokerConfig::plain_credentials`].
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileSaslPlainConfig {
    /// Filesystem path to the SASL/PLAIN credential file: one
    /// `username=password` per line, `#` comment lines and blank lines
    /// ignored, the trailing newline stripped. File-based (not literal) so no
    /// password sits in the TOML; the operator mounts a `Secret` and writes
    /// the mount path here. A password may contain `=`; only the first `=` on
    /// a line separates the two.
    pub credentials_path: Option<std::path::PathBuf>,
}

/// Parse the credential file body into username → password pairs.
fn parse_credentials(
    path: &std::path::Path,
    body: &str,
) -> Result<crate::config::PlainCredentials, FileConfigError> {
    body.lines()
        .enumerate()
        .filter(|(_, line)| {
            let trimmed = line.trim();
            !trimmed.is_empty() && !trimmed.starts_with('#')
        })
        .map(|(index, line)| {
            let (user, password) = line.trim().split_once('=').ok_or_else(|| {
                FileConfigError::InvalidConfig(format!(
                    "[sasl_plain]: {}:{}: expected user=password",
                    path.display(),
                    index + 1
                ))
            })?;
            if user.is_empty() {
                return Err(FileConfigError::InvalidConfig(format!(
                    "[sasl_plain]: {}:{}: empty username",
                    path.display(),
                    index + 1
                )));
            }
            Ok((user.to_owned(), password.to_owned()))
        })
        .collect()
}

/// Load `[sasl_plain] credentials_path` into
/// [`crate::BrokerConfig::plain_credentials`].
///
/// # Errors
///
/// Returns [`FileConfigError::InvalidConfig`] when the file is unreadable or
/// a non-comment line is not `user=password`.
pub(super) fn apply_sasl_plain(
    sasl_plain: Option<&FileSaslPlainConfig>,
    cfg: &mut crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    let Some(path) = sasl_plain.and_then(|section| section.credentials_path.as_deref()) else {
        return Ok(());
    };
    let body = std::fs::read_to_string(path).map_err(|error| {
        FileConfigError::InvalidConfig(format!(
            "[sasl_plain]: failed to read credentials_path {}: {error}",
            path.display()
        ))
    })?;
    cfg.plain_credentials = parse_credentials(path, body.trim_end_matches(['\n', '\r']))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::file_config::FileConfig;

    fn apply_credential_file(body: &str) -> Result<crate::config::BrokerConfig, String> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain-credentials");
        std::fs::write(&path, body).unwrap();
        let toml = format!(
            "[sasl_plain]\ncredentials_path = '{}'\n",
            path.display().to_string().replace('\\', "/")
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg)
            .map(|()| cfg)
            .map_err(|error| error.to_string())
    }

    #[test]
    fn parses_users_comments_and_blank_lines() {
        let cfg = apply_credential_file(
            "# operators\nadmin=admin-secret\n\nalice=pa=ss\n# trailing comment\n",
        )
        .unwrap();

        assert!(
            *cfg.plain_credentials.as_map()
                == maplit::hashmap! {
                    "admin".to_string() => "admin-secret".to_string(),
                    "alice".to_string() => "pa=ss".to_string(),
                }
        );
    }

    #[test]
    fn absent_section_leaves_the_table_empty() {
        let file: FileConfig = toml::from_str("").unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        assert!(cfg.plain_credentials.is_empty());
    }

    #[test]
    fn rejects_a_malformed_line() {
        let message = apply_credential_file("admin=secret\nnonsense\n")
            .expect_err("a line without `=` is rejected");
        assert!(message.contains("expected user=password"), "{message}");
    }

    #[test]
    fn rejects_an_empty_username() {
        let message =
            apply_credential_file("=secret\n").expect_err("an empty username is rejected");
        assert!(message.contains("empty username"), "{message}");
    }

    #[test]
    fn rejects_an_unreadable_path() {
        let file: FileConfig =
            toml::from_str("[sasl_plain]\ncredentials_path = '/no/such/krabka/plain'\n").unwrap();
        let mut cfg = crate::config::BrokerConfig::default();

        let error = file
            .apply_to(&mut cfg)
            .expect_err("a missing credential file is rejected");
        assert!(
            error
                .to_string()
                .contains("failed to read credentials_path")
        );
    }

    #[test]
    fn debug_prints_usernames_but_no_passwords() {
        let mut cfg = apply_credential_file("admin=admin-secret\n").unwrap();
        cfg.inter_broker_credentials = Some(crate::config::InterBrokerCredentials::Plain {
            username: "broker".to_string(),
            password: "broker-secret".to_string(),
        });

        let rendered = format!("{cfg:?}");

        assert!(rendered.contains("admin"));
        assert!(!rendered.contains("admin-secret"));
        assert!(rendered.contains("broker"));
        assert!(!rendered.contains("broker-secret"));
        assert!(rendered.contains("<redacted>"));
    }
}
