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

/// Map an optional-endpoint CLI value, such as `--metrics-listen-addr` or
/// `--health-listen-addr`, onto an `Option<SocketAddr>`. An empty string or
/// `none`, in any case, disables the endpoint. Every other value must parse
/// as a `SocketAddr`.
pub fn parse_optional_listen_addr(
    s: &str,
) -> Result<Option<SocketAddr>, Box<dyn std::error::Error>> {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    Ok(Some(trimmed.parse()?))
}

/// The controller port when `--controller-listen-addr` is not given.
const DEFAULT_CONTROLLER_PORT: u16 = 9093;

impl Args {
    /// Where the controller listener binds: `--controller-listen-addr` when it
    /// is given, and otherwise the client listener's host on port 9093.
    ///
    /// Under `--config-file` (operator/StatefulSet mode), `--listen-addr`
    /// conflicts with the config file, so `listen_addr` keeps its
    /// `127.0.0.1:9092` default. Peers dial this broker's controller via its
    /// pod FQDN, so binding the controller listener to loopback would make it
    /// unreachable across pods. It binds all interfaces (`0.0.0.0`) instead.
    pub fn resolved_controller_listen_addr(&self) -> SocketAddr {
        self.controller_listen_addr.unwrap_or_else(|| {
            let mut addr = self.listen_addr;
            addr.set_port(DEFAULT_CONTROLLER_PORT);
            if self.config_file.is_some() {
                addr.set_ip(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
            }
            addr
        })
    }

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
            controller_quorum_voters: if self.controller_quorum_voters.is_empty() {
                vec![(
                    krabka_broker::NodeId(node_id),
                    controller_listen_addr.to_string(),
                )]
            } else {
                std::mem::take(&mut self.controller_quorum_voters)
            },
            bootstrap_servers: std::mem::take(&mut self.controller_bootstrap_servers),
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
    use clap::Parser as _;

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

    #[test]
    fn base_config_preserves_explicit_controller_discovery() {
        let mut args = Args::try_parse_from([
            "krabka-broker",
            "--controller-quorum-voters",
            "7@controller.example:9093",
            "--controller-bootstrap-servers",
            "bootstrap.example:9093",
        ])
        .unwrap();
        let config = args.base_broker_config(
            "broker.example:9092".into(),
            "127.0.0.1:9093".parse().unwrap(),
            7,
            None,
            None,
            krabka_broker::telemetry::OtlpProtocol::Grpc,
        );

        assert!(
            config.controller_quorum_voters
                == vec![(krabka_broker::NodeId(7), "controller.example:9093".into())]
        );
        assert!(config.bootstrap_servers == vec!["bootstrap.example:9093"]);
    }

    #[test]
    fn the_controller_listener_defaults_to_port_9093_and_yields_to_its_flag() {
        let _guard = crate::test_support::env_guard();
        let cases: [(&str, &[&str], &str); 5] = [
            ("default", &[], "127.0.0.1:9093"),
            (
                "the client host with the controller port",
                &["--listen-addr=10.1.2.3:19092"],
                "10.1.2.3:9093",
            ),
            (
                "a config file binds every interface",
                &["--config-file=broker.toml"],
                "0.0.0.0:9093",
            ),
            (
                "the flag wins over the client host",
                &[
                    "--listen-addr=10.1.2.3:19092",
                    "--controller-listen-addr=127.0.0.1:19093",
                ],
                "127.0.0.1:19093",
            ),
            (
                "the flag wins over the config file",
                &[
                    "--config-file=broker.toml",
                    "--controller-listen-addr=10.0.0.5:29093",
                ],
                "10.0.0.5:29093",
            ),
        ];
        for (name, flags, expected) in cases {
            let args =
                Args::try_parse_from(std::iter::once("krabka-broker").chain(flags.iter().copied()))
                    .unwrap_or_else(|error| panic!("{name}: {error}"));
            let expected: SocketAddr = expected.parse().unwrap();
            assert!(args.resolved_controller_listen_addr() == expected, "{name}");
        }
    }

    #[test]
    fn base_config_defaults_to_the_local_controller() {
        let mut args = Args::try_parse_from(["krabka-broker"]).unwrap();
        let controller = "127.0.0.1:9093".parse().unwrap();
        let config = args.base_broker_config(
            "127.0.0.1:9092".into(),
            controller,
            3,
            None,
            None,
            krabka_broker::telemetry::OtlpProtocol::Grpc,
        );

        assert!(
            config.controller_quorum_voters
                == vec![(krabka_broker::NodeId(3), controller.to_string())]
        );
    }
}
