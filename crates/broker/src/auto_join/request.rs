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
        assert_eq!(listener.name, "CONTROLLER");
        assert_eq!(listener.host, "192.0.2.10");
        assert_eq!(listener.port, 19093);
    }

    #[test]
    fn select_bootstrap_server_wraps_attempts() {
        let servers: Vec<String> = ["127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"]
            .into_iter()
            .map(str::to_owned)
            .collect();

        assert_eq!(select_bootstrap_server(&servers, 0), servers[0].as_str());
        assert_eq!(select_bootstrap_server(&servers, 2), servers[2].as_str());
        assert_eq!(select_bootstrap_server(&servers, 3), servers[0].as_str());
        assert_eq!(select_bootstrap_server(&servers, 5), servers[2].as_str());
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
