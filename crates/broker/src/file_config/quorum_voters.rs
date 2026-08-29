//! Parsing the KIP-595 controller quorum and KIP-853 bootstrap endpoints.
//!
//! Both entry forms are validated for shape only. The host is carried verbatim
//! rather than DNS-resolved, because the inter-broker dialer re-resolves it on
//! every reconnect and freezing a peer's boot-time address would strand a
//! `StatefulSet` peer that restarts on a new pod IP.

use super::{FileConfig, FileConfigError};

impl FileConfig {
    /// Parse a single `controller_quorum_voters` entry of the form
    /// `<node_id>@<host>:<port>` into `(NodeId, "<host>:<port>")`. The host is
    /// **not** DNS-resolved — it is carried verbatim so the dialer can
    /// re-resolve it on every (re)connect. Freezing a peer's boot-time IP here
    /// would strand a `StatefulSet` peer that restarts on a new pod IP (its
    /// stable DNS name still resolves, but to a different address). Only the
    /// shape is validated: a numeric node id and a `<host>:<port>` with a
    /// non-empty host and a numeric port.
    ///
    /// # Errors
    ///
    /// [`FileConfigError::InvalidQuorumVoter`] when the entry has no `@`, a
    /// non-numeric node id, or a malformed `<host>:<port>` (missing port,
    /// empty host, or non-numeric port).
    pub(super) fn parse_quorum_voter(
        entry: &str,
    ) -> Result<(krabka_raft::NodeId, String), FileConfigError> {
        let (id_str, host_port) = entry.split_once('@').ok_or_else(|| {
            FileConfigError::InvalidQuorumVoter(format!(
                "{entry:?}: expected `<node_id>@<host>:<port>` (missing `@`)"
            ))
        })?;
        let node_id = krabka_raft::NodeId(id_str.parse::<u64>().map_err(|e| {
            FileConfigError::InvalidQuorumVoter(format!(
                "{entry:?}: invalid node id {id_str:?}: {e}"
            ))
        })?);
        // Validate the `<host>:<port>` shape without resolving. Split on the
        // LAST ':' so the port is taken from the end (the dialer splits the
        // same way), then carry `<host>:<port>` verbatim for per-dial lookup.
        let (host, port_str) = host_port.rsplit_once(':').ok_or_else(|| {
            FileConfigError::InvalidQuorumVoter(format!(
                "{entry:?}: expected `<host>:<port>` after `@` (missing `:port`)"
            ))
        })?;
        if host.is_empty() {
            return Err(FileConfigError::InvalidQuorumVoter(format!(
                "{entry:?}: empty host"
            )));
        }
        port_str.parse::<u16>().map_err(|e| {
            FileConfigError::InvalidQuorumVoter(format!(
                "{entry:?}: invalid port {port_str:?}: {e}"
            ))
        })?;
        Ok((node_id, host_port.to_string()))
    }

    pub(super) fn parse_bootstrap_server(entry: &str) -> Result<String, FileConfigError> {
        Self::parse_quorum_voter(&format!("0@{entry}")).map(|(_, endpoint)| endpoint)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn apply_to_parses_multi_voter_quorum_in_order() {
        use crate::config::BrokerConfig;

        let src = r#"
controller_quorum_voters = ["0@127.0.0.1:9093", "1@127.0.0.2:9093", "2@127.0.0.3:9093"]
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        // Host:port carried verbatim (parsed, NOT DNS-resolved) so the dialer
        // re-resolves each peer per connect.
        let expected: Vec<(krabka_raft::NodeId, String)> = vec![
            (krabka_audit::NodeId(0), "127.0.0.1:9093".to_string()),
            (krabka_audit::NodeId(1), "127.0.0.2:9093".to_string()),
            (krabka_audit::NodeId(2), "127.0.0.3:9093".to_string()),
        ];
        assert!(cfg.controller_quorum_voters == expected);
    }
    #[test]
    fn apply_to_keeps_unresolvable_hostname_without_dns() {
        use crate::config::BrokerConfig;

        // A peer FQDN that does not resolve right now (a `StatefulSet` peer
        // whose A record isn't published yet, or simply offline) MUST be
        // accepted and carried verbatim — the old resolve-at-startup path
        // would have failed the whole broker boot here. The dialer resolves it
        // later, per connect, so a peer coming up on a new pod IP is reachable.
        let src = r#"
controller_quorum_voters = ["0@demo-broker-0-0.demo-broker-headless.default.svc.cluster.local:9093"]
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        let expected: Vec<(krabka_raft::NodeId, String)> = vec![(
            krabka_audit::NodeId(0),
            "demo-broker-0-0.demo-broker-headless.default.svc.cluster.local:9093".to_string(),
        )];
        assert!(cfg.controller_quorum_voters == expected);
    }
    #[test]
    fn apply_to_rejects_malformed_quorum_voters() {
        use crate::config::BrokerConfig;

        let cases = [
            ("0@just-a-host", "missing port"),
            ("0@host:nine-thousand", "non-numeric port"),
            ("127.0.0.1:9093", "missing @"),
            ("foo@127.0.0.1:9093", "non-numeric id"),
        ];
        for (voter, label) in cases {
            let src = format!("controller_quorum_voters = [\"{voter}\"]\n");
            let file: FileConfig = toml::from_str(&src).unwrap();
            let mut cfg = BrokerConfig::default();
            let err = file.apply_to(&mut cfg).unwrap_err();
            assert!(
                matches!(err, FileConfigError::InvalidQuorumVoter(_)),
                "voter {voter:?} ({label}) must be rejected as InvalidQuorumVoter; got {err:?}"
            );
        }
    }
    #[test]
    fn apply_to_empty_quorum_voters_leaves_existing_unchanged() {
        use crate::config::BrokerConfig;

        // No `controller_quorum_voters` key at all → empty default.
        let file: FileConfig = toml::from_str("broker_id = 0").unwrap();
        assert!(file.controller_quorum_voters.is_empty());

        // Seed a pre-existing single self-voter as the binary would.
        let seeded: Vec<(krabka_raft::NodeId, String)> =
            vec![(krabka_audit::NodeId(7), "127.0.0.1:9093".to_string())];
        let mut cfg = BrokerConfig {
            controller_quorum_voters: seeded.clone(),
            ..BrokerConfig::default()
        };

        file.apply_to(&mut cfg).unwrap();

        // Empty list must NOT clear the seeded voter set.
        assert!(cfg.controller_quorum_voters == seeded);
    }
}
