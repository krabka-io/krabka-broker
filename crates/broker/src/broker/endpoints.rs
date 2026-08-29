//! Parsing of advertised `host:port` strings and construction of the KIP-595
//! static controller voter set. Both turn configured endpoint text into the
//! domain values the registration and metadata paths need, and neither depends
//! on any other part of the broker.

/// Split a `host:port` advertised string. Mirrors the helpers in
/// `handlers::find_coordinator` / `handlers::metadata` but returns
/// `(String, u16)` for direct `BrokerEndpoint` use. Splits on the LAST
/// `:` so IPv6 literals do not break on inner colons (we still expect
/// IPv6 callers to wrap in `[...]`).
pub(super) fn parse_advertised_host_port(addr: &str) -> (String, u16) {
    if let Some(host_port) = crate::host_port::parse_host_port(addr) {
        return host_port;
    }
    tracing::warn!(
        addr,
        "advertised not host:port; falling back to localhost:9092"
    );
    (
        crate::host_port::DEFAULT_KAFKA_HOST.into(),
        crate::host_port::DEFAULT_KAFKA_PORT,
    )
}

/// Build the KIP-595 static controller [`VoterSet`](krabka_metadata::VoterSet)
/// from the configured `controller_quorum_voters` (`(id, "<host>:<port>")`).
///
/// Peer endpoint hosts stay as their configured **DNS names**. They are NOT
/// pre-resolved to IPs, so the inter-broker dialer re-resolves them on every
/// (re)connect, because `TcpStream::connect((host, port))` does a fresh lookup.
/// A `StatefulSet` peer that restarts on a new pod IP keeps its stable DNS
/// name, so re-resolution reaches it again. A frozen boot-time IP would
/// permanently strand a rejoining voter. Its peers would dial the dead old IP
/// forever, the leader's `BeginQuorumEpoch` heartbeats would never arrive, and
/// the rejoining node would never learn the leader. It would then never open
/// its data listener.
///
/// `directory_id` is only load-bearing for self: the engine keys vote/peer
/// logic on `NodeId` and uses `Uuid::nil()` for vote keys, so peers get a nil
/// placeholder (verified against `kraft/network.rs::controller_addr` and
/// `kraft/core.rs`).
pub(super) fn static_controller_voter_set(
    quorum_voters: &[(krabka_raft::NodeId, String)],
    self_node_id: krabka_raft::NodeId,
    self_directory_id: uuid::Uuid,
    _self_controller_listen: std::net::SocketAddr,
) -> krabka_metadata::VoterSet {
    // Split a configured "<host>:<port>" into (host, port), keeping the host
    // verbatim (a DNS name resolved later, per dial). `file_config`
    // (`parse_quorum_voter`) validates the shape, so a parse miss here is not
    // expected; fall back to port 0 rather than panicking.
    fn split_host_port(host_port: &str) -> (String, u16) {
        match host_port.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse().unwrap_or(0)),
            None => (host_port.to_string(), 0),
        }
    }
    fn voter(
        id: krabka_raft::NodeId,
        directory_id: uuid::Uuid,
        host: String,
        port: u16,
    ) -> krabka_metadata::Voter {
        krabka_metadata::Voter {
            id,
            directory_id,
            endpoints: vec![krabka_metadata::VoterEndpoint {
                name: "CONTROLLER".to_string(),
                host,
                port,
            }],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        }
    }

    let voters: Vec<krabka_metadata::Voter> = quorum_voters
        .iter()
        .map(|(node_id, host_port)| {
            let (configured_host, configured_port) = split_host_port(host_port);
            let (host, port, directory_id) = if *node_id == self_node_id {
                (configured_host, configured_port, self_directory_id)
            } else {
                (configured_host, configured_port, uuid::Uuid::nil())
            };
            voter(*node_id, directory_id, host, port)
        })
        .collect();
    krabka_metadata::VoterSet::from_voters(voters)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn static_voter_set_keeps_peer_hostnames_for_per_dial_resolution() {
        // Peer endpoint hosts MUST be the configured DNS names, NOT resolved to
        // IPs: the inter-broker dialer re-resolves the host on every connect, so
        // a peer that restarts on a new pod IP (stable DNS name, fresh A record)
        // is reached again. Regression — pre-resolving froze the peer's
        // boot-time IP, so after a `StatefulSet` pod restart every peer dialed
        // the dead old IP forever, the rejoining voter never received
        // `BeginQuorumEpoch`, never learned the leader, and never opened :9092.
        let quorum = vec![
            (
                krabka_raft::NodeId(0),
                "demo-broker-0-0.demo-broker-headless.default.svc.cluster.local:9093".to_string(),
            ),
            (
                krabka_raft::NodeId(1),
                "demo-broker-1-0.demo-broker-headless.default.svc.cluster.local:9093".to_string(),
            ),
        ];
        let self_dir = uuid::Uuid::from_u128(7);
        let set = static_controller_voter_set(
            &quorum,
            krabka_audit::NodeId(0),
            self_dir,
            "0.0.0.0:9093".parse().unwrap(),
        );

        let v0 = set.get(krabka_audit::NodeId(0)).expect("voter 0 present");
        let ep0 = v0
            .endpoints
            .iter()
            .find(|e| e.name == "CONTROLLER")
            .expect("controller endpoint");
        // Self keeps its real directory id; peers get the nil placeholder.
        check!(
            ep0.host.as_str() == "demo-broker-0-0.demo-broker-headless.default.svc.cluster.local"
        );
        check!(ep0.port == 9093);
        check!(v0.directory_id == self_dir);

        let v1 = set.get(krabka_audit::NodeId(1)).expect("voter 1 present");
        let ep1 = v1
            .endpoints
            .iter()
            .find(|e| e.name == "CONTROLLER")
            .expect("controller endpoint");
        assert!(ep1.host == "demo-broker-1-0.demo-broker-headless.default.svc.cluster.local");
        assert!(v1.directory_id == uuid::Uuid::nil());
    }

    #[test]
    fn static_voter_set_single_self_voter_uses_configured_addr() {
        // The configured endpoint is the advertised address. It can differ
        // from the bind address when the controller runs behind DNS or NAT.
        let quorum = vec![(krabka_raft::NodeId(3), "127.0.0.1:9093".to_string())];
        let self_dir = uuid::Uuid::from_u128(3);
        let set = static_controller_voter_set(
            &quorum,
            krabka_audit::NodeId(3),
            self_dir,
            "192.168.1.5:9099".parse().unwrap(),
        );
        assert!(set.len() == 1);
        let v = set
            .get(krabka_audit::NodeId(3))
            .expect("self voter present");
        let ep = v.endpoints.iter().find(|e| e.name == "CONTROLLER").unwrap();
        assert!(ep.host == "127.0.0.1");
        assert!(ep.port == 9093);
    }

    #[test]
    fn advertised_listener_parser_preserves_valid_host_ports_and_uses_fallback() {
        let cases = [
            ("broker-1.example:19092", ("broker-1.example", 19092)),
            ("[2001:db8::7]:9094", ("[2001:db8::7]", 9094)),
            ("missing-port", ("localhost", 9092)),
            ("broker:not-a-port", ("localhost", 9092)),
        ];
        for (input, (host, port)) in cases {
            assert!(
                parse_advertised_host_port(input) == (host.to_string(), port),
                "input {input:?}"
            );
        }
    }
}
