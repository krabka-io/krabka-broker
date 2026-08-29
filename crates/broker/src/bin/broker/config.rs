//! The `BrokerConfig` the broker starts from, built out of the parsed command
//! line.

use std::net::SocketAddr;

use krabka_broker::{BootstrapMode, BrokerConfig};
use krabka_log::LogConfig;

use crate::cli::Args;

/// Parse `--process-roles` string values into `NodeRole`s.
pub fn parse_roles_arg(roles: &[String]) -> Result<Vec<krabka_broker::config::NodeRole>, String> {
    use krabka_broker::config::NodeRole;
    roles
        .iter()
        .map(|r| match r.to_ascii_lowercase().as_str() {
            "controller" => Ok(NodeRole::Controller),
            "broker" => Ok(NodeRole::Broker),
            "witness" => Ok(NodeRole::Witness),
            other => Err(format!(
                "unknown --process-roles value `{other}` \
                 (expected `controller`, `broker`, or `witness`)"
            )),
        })
        .collect()
}

/// Map the `--metrics-listen-addr` CLI value onto an `Option<SocketAddr>`.
/// An empty string or `none`, in any case, disables the endpoint. Every other
/// value must parse as a `SocketAddr`.
pub fn parse_metrics_addr(s: &str) -> Result<Option<SocketAddr>, Box<dyn std::error::Error>> {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    Ok(Some(trimmed.parse()?))
}

impl Args {
    pub fn base_broker_config(
        &mut self,
        advertised_listener: String,
        controller_listen_addr: SocketAddr,
        node_id: u64,
        metrics_listen_addr: Option<SocketAddr>,
        client_metrics_otlp_endpoint: Option<String>,
        client_metrics_otlp_protocol: krabka_broker::telemetry::OtlpProtocol,
    ) -> BrokerConfig {
        BrokerConfig {
            broker_id: self.broker_id,
            listen_addr: self.listen_addr,
            advertised_listener,
            log_dir: std::mem::take(&mut self.log_dir),
            extra_log_dirs: std::mem::take(&mut self.extra_log_dirs),
            log_config: LogConfig::default(),
            node_id: krabka_broker::NodeId(node_id),
            controller_listen_addr,
            controller_quorum_voters: vec![(
                krabka_broker::NodeId(node_id),
                controller_listen_addr.to_string(),
            )],
            bootstrap_servers: std::mem::take(&mut self.controller_bootstrap_servers)
                .into_iter()
                .map(|endpoint| endpoint.to_string())
                .collect(),
            directory_id: uuid::Uuid::nil(),
            auto_join: self.controller_auto_join,
            bootstrap_mode: BootstrapMode::Bootstrap,
            cluster_id: self.cluster_id.take(),
            metrics_listen_addr,
            profiling: self.profiling.clone(),
            client_metrics_otlp_endpoint,
            client_metrics_otlp_protocol,
            delegation_token_secret_key: self
                .delegation_token_secret_key
                .take()
                .map(|key| krabka_security::SecretBytes::new(key.into_bytes())),
            ..BrokerConfig::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn parse_roles_arg_maps_strings() {
        assert!(
            parse_roles_arg(&["controller".to_string(), "broker".to_string()]).unwrap()
                == vec![
                    krabka_broker::config::NodeRole::Controller,
                    krabka_broker::config::NodeRole::Broker
                ]
        );
    }

    #[test]
    fn parse_roles_arg_rejects_unknown() {
        assert!(parse_roles_arg(&["nope".to_string()]).is_err());
    }

    #[test]
    fn parse_roles_arg_accepts_witness_case_insensitively() {
        assert!(
            parse_roles_arg(&[
                "BROKER".to_string(),
                "Controller".to_string(),
                "WiTnEsS".to_string(),
            ])
            .unwrap()
                == vec![
                    krabka_broker::config::NodeRole::Broker,
                    krabka_broker::config::NodeRole::Controller,
                    krabka_broker::config::NodeRole::Witness
                ]
        );
    }
}
