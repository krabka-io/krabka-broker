//! Construction of the joiner's own identity on the wire: the `CONTROLLER`
//! listener it advertises, the bootstrap server it dials next, and the
//! `AddRaftVoterRequest` body itself.
//!
//! These are pure functions with no I/O, which is why they are their own file:
//! both loops in this module build the same identity, and the unit tests can
//! check it without a socket.

use krabka_protocol::owned::add_raft_voter_request::{AddRaftVoterRequest, Listener};

pub(super) fn controller_listener(bound: std::net::SocketAddr) -> Listener {
    let host = if bound.ip().is_unspecified() {
        std::env::var("HOSTNAME").unwrap_or_else(|_| "127.0.0.1".to_string())
    } else {
        bound.ip().to_string()
    };
    Listener {
        name: "CONTROLLER".to_string(),
        host,
        port: bound.port(),
        ..Default::default()
    }
}

/// The controller endpoint a voter RPC should publish for this node.
///
/// A controller bound to a concrete address publishes that address: it is what
/// the socket actually answers on, and it is what the operator chose.
///
/// A controller bound to `0.0.0.0` has no address of its own, and
/// [`controller_listener`] falls back to a guess -- `HOSTNAME`, or
/// `127.0.0.1`. Publishing that guess is worse than publishing nothing: it
/// replaces a committed endpoint every other node can reach with one that
/// resolves, for whoever reads it, back to the reader. `advertised` -- this
/// node's own `controller.quorum.voters` entry, which is how the rest of the
/// cluster is configured to reach it -- is the address to publish instead, and
/// only that case takes it.
pub(super) fn advertised_controller_listener(
    advertised: Option<&str>,
    bound: std::net::SocketAddr,
) -> Listener {
    if !bound.ip().is_unspecified() {
        return controller_listener(bound);
    }
    let Some((host, port)) = advertised.and_then(split_host_port) else {
        return controller_listener(bound);
    };
    Listener {
        name: "CONTROLLER".to_string(),
        host,
        port,
        ..Default::default()
    }
}

/// Splits a `host:port` endpoint, taking the port after the last colon so an
/// unbracketed IPv6 literal does not split in the middle of an address.
fn split_host_port(endpoint: &str) -> Option<(String, u16)> {
    let (host, port) = endpoint.rsplit_once(':')?;
    if host.is_empty() {
        return None;
    }
    Some((host.to_string(), port.parse().ok()?))
}

pub(super) fn select_bootstrap_server(bootstrap_servers: &[String], attempt: usize) -> &str {
    &bootstrap_servers[attempt % bootstrap_servers.len()]
}

pub(super) fn build_add_raft_voter_request(
    cluster_id: Option<uuid::Uuid>,
    voter_id: i32,
    directory_id: krabka_protocol::primitives::uuid::Uuid,
    listener: Listener,
    timeout_ms: i32,
) -> AddRaftVoterRequest {
    AddRaftVoterRequest {
        cluster_id: cluster_id.map(|u| u.to_string()),
        timeout_ms,
        voter_id,
        voter_directory_id: directory_id,
        listeners: vec![listener],
        ack_when_committed: false,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::BytesMut;
    use krabka_protocol::{Decode, Encode, owned::add_raft_voter_request};

    use super::*;

    #[test]
    fn controller_listener_uses_bound_controller_endpoint() {
        let listener = controller_listener("192.0.2.10:19093".parse().unwrap());
        assert2::assert!((listener.name) == ("CONTROLLER"));
        assert2::assert!((listener.host) == ("192.0.2.10"));
        assert2::assert!((listener.port) == (19093));
    }

    /// A concrete bind is the address the socket answers on, so it is
    /// published even when the node also carries an advertised endpoint.
    #[test]
    fn a_bound_controller_publishes_the_address_it_answers_on() {
        let listener = advertised_controller_listener(
            Some("controller.example:19093"),
            "192.0.2.10:19093".parse().unwrap(),
        );

        assert!((listener.host.as_str(), listener.port) == ("192.0.2.10", 19093));
    }

    /// A wildcard bind has no address of its own, so the configured endpoint
    /// is published rather than the `127.0.0.1` guess.
    #[test]
    fn a_wildcard_bind_publishes_the_configured_endpoint() {
        let listener = advertised_controller_listener(
            Some("controller.example:19093"),
            "0.0.0.0:19093".parse().unwrap(),
        );

        assert!(
            (
                listener.name.as_str(),
                listener.host.as_str(),
                listener.port
            ) == ("CONTROLLER", "controller.example", 19093)
        );
    }

    /// Nothing configured leaves the guess as the only answer there is.
    #[test]
    fn a_wildcard_bind_with_nothing_configured_falls_back_to_the_bound_listener() {
        let bound = "0.0.0.0:19093".parse().unwrap();

        assert!(advertised_controller_listener(None, bound) == controller_listener(bound));
    }

    /// An endpoint the port cannot be read out of is no better than nothing.
    #[test]
    fn an_unparseable_advertised_endpoint_falls_back_to_the_bound_listener() {
        let bound = "0.0.0.0:19093".parse().unwrap();

        for endpoint in ["controller.example", "controller.example:", ":19093"] {
            assert!(
                advertised_controller_listener(Some(endpoint), bound) == controller_listener(bound),
                "{endpoint}"
            );
        }
    }

    #[test]
    fn select_bootstrap_server_wraps_attempts() {
        let servers: Vec<String> = ["127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"]
            .into_iter()
            .map(str::to_owned)
            .collect();

        assert2::assert!((select_bootstrap_server(&servers, 0)) == (servers[0].as_str()));
        assert2::assert!((select_bootstrap_server(&servers, 2)) == (servers[2].as_str()));
        assert2::assert!((select_bootstrap_server(&servers, 3)) == (servers[0].as_str()));
        assert2::assert!((select_bootstrap_server(&servers, 5)) == (servers[2].as_str()));
    }

    #[test]
    fn build_add_raft_voter_request_carries_joiner_identity() {
        let cluster_id = uuid::Uuid::from_u128(0xCAFE);
        let dir = uuid::Uuid::from_u128(0xD1E);
        let listener = controller_listener("127.0.0.1:19093".parse().unwrap());
        let req = build_add_raft_voter_request(
            Some(cluster_id),
            7,
            krabka_protocol::primitives::uuid::Uuid(*dir.as_bytes()),
            listener,
            1_234,
        );

        let cluster_id_string = cluster_id.to_string();
        assert!(matches!(
            (
                req.cluster_id.as_deref(),
                req.timeout_ms,
                req.voter_id,
                req.voter_directory_id.0,
                req.listeners.len(),
                req.listeners[0].name.as_str(),
                req.listeners[0].host.as_str(),
                req.listeners[0].port,
            ),
            (Some(id), 1_234, 7, directory_id, 1, "CONTROLLER", "127.0.0.1", 19093)
                if id == cluster_id_string && directory_id == *dir.as_bytes()
        ));
        assert!(!req.ack_when_committed);
    }

    #[test]
    fn build_add_raft_voter_request_encodes_ack_when_committed() {
        let listener = controller_listener("127.0.0.1:19093".parse().unwrap());
        let req = build_add_raft_voter_request(
            None,
            7,
            krabka_protocol::primitives::uuid::Uuid(*uuid::Uuid::from_u128(7).as_bytes()),
            listener,
            30_000,
        );
        let version = add_raft_voter_request::MAX_VERSION;
        let mut bytes = BytesMut::new();

        req.encode(&mut bytes, version).expect("encode request");
        let decoded =
            AddRaftVoterRequest::decode(&mut bytes.freeze(), version).expect("decode request");

        assert!(!decoded.ack_when_committed);
    }
}
