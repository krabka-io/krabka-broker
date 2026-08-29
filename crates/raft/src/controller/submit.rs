//! Record submission: the leader-local `submit_change`, the resolution of a
//! leader's controller-listener address, and the encode → dial → translate path
//! a follower forwards a rejected batch over. The transport seam that dials the
//! leader is isolated here so the encode and translate halves stay testable
//! without a live quorum.

use krabka_metadata::MetadataRecord;

use super::ControllerHandle;
use crate::{
    error::RaftError,
    network::OutboundDialer,
    types::{NodeId, controller_endpoint_addr as endpoint_addr_from_endpoints},
};

impl ControllerHandle {
    /// Submit a batch of metadata records. Returns `Ok(())` once committed AND
    /// applied on the leader. Pre-validation lives in the engine. On a follower
    /// (`NotLeader` with a known leader), forwards directly to the leader's
    /// controller listener via `API_KEY_SUBMIT_CHANGE`.
    ///
    /// # Errors
    /// Returns an error if validation, replication, or forwarding fails.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.self_node_id.0, records = records.len())
    )]
    pub async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<crate::SubmitChangeResult, RaftError> {
        match self.engine.submit_change(records.clone()).await {
            Ok(result) => Ok(result),
            Err(RaftError::NotLeader {
                current_leader: Some(leader),
            }) => {
                if let Some(addr) = self.voter_addr(leader) {
                    self.forward_submit_to(leader, &addr, &records).await
                } else {
                    Err(RaftError::NotLeader {
                        current_leader: Some(leader),
                    })
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Resolve a voter's controller listener `<host>:<port>` from the static
    /// voter set's CONTROLLER endpoint. See [`controller_endpoint_addr`].
    fn voter_addr(&self, node_id: NodeId) -> Option<String> {
        let voters = self.engine.quorum_snapshot().voters;
        controller_endpoint_addr(&voters, node_id)
            .or_else(|| controller_endpoint_addr(&self.voters, node_id))
    }

    /// Open a one-shot authenticated connection to the leader's controller
    /// listener, send a wincode-encoded `Vec<MetadataRecord>` as
    /// `API_KEY_SUBMIT_CHANGE`, and translate the response into a `RaftError`.
    ///
    /// Decomposed into three killable steps: [`encode_submit_change_body`] builds
    /// the exact wire bytes, the [`SubmitChangeTransport`] seam performs the
    /// (un-mockable) dial→`raw_request`→close round trip and hands back the raw
    /// response body, and [`translate_submit_change_response`] decodes that body
    /// and maps the transport `error_code` into a `RaftError`.
    // cargo-mutants: thin wrapper; builds the live DialerSubmitTransport, needs a real dialer
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.self_node_id.0, leader = leader.0, addr, records = records.len()),
        err
    )]
    async fn forward_submit_to(
        &self,
        leader: NodeId,
        addr: &str,
        records: &[krabka_metadata::MetadataRecord],
    ) -> Result<crate::SubmitChangeResult, RaftError> {
        let transport = DialerSubmitTransport {
            dialer: self.dialer.as_ref(),
            client_id: &self.client_id,
            client_dispatch_queue_capacity: self.client_dispatch_queue_capacity,
            client_frame_max: self.client_frame_max,
        };
        forward_submit_via(&transport, leader, addr, records).await
    }
}

/// Resolve a voter's CONTROLLER-listener `<host>:<port>` from the voter set,
/// preferring the endpoint named `CONTROLLER` and falling back to the first.
///
/// The host is returned VERBATIM (a DNS name), never pre-resolved to a
/// `SocketAddr`. The dialer re-resolves it per connect (`TcpStream::connect`),
/// so a peer that restarts on a new pod IP stays reachable. Parsing to a
/// `SocketAddr` here would (a) freeze a restarted peer's boot-time IP and
/// (b) fail outright on a non-literal hostname — which silently disabled
/// leader-forwarding of `submit_change` (e.g. broker self-registration), since
/// `parse()` returned `None` and the forward was skipped.
fn controller_endpoint_addr(voters: &krabka_metadata::VoterSet, node_id: NodeId) -> Option<String> {
    let voter = voters.get(node_id)?;
    endpoint_addr_from_endpoints(&voter.endpoints)
}

/// The single un-mockable step of leader-forwarding a `submit_change`: dial the
/// leader's controller listener, issue one `API_KEY_SUBMIT_CHANGE` request with
/// the already-encoded `body`, and return the raw response body bytes. The
/// concrete [`krabka_client_core::Connection`] is opaque (it cannot be built in
/// a test), so this seam returns plain `Bytes` — every serialize/translate
/// decision around it stays unit-testable against a mock.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
trait SubmitChangeTransport: Send + Sync {
    /// Round-trip the encoded `API_KEY_SUBMIT_CHANGE` `body` to the leader and
    /// return the raw response body.
    async fn send_submit_change(
        &self,
        leader: NodeId,
        addr: &str,
        body: Vec<u8>,
    ) -> Result<bytes::Bytes, krabka_client_core::ClientError>;
}

/// Live [`SubmitChangeTransport`] over the injected [`OutboundDialer`]: dials a
/// one-shot authenticated connection, sends the request at `API_KEY_SUBMIT_CHANGE`
/// version 0, closes the connection, and returns the response body. This is the
/// only part of the forward path that touches a real socket.
struct DialerSubmitTransport<'a> {
    dialer: &'a dyn OutboundDialer,
    client_id: &'a str,
    client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    client_frame_max: krabka_client_core::ClientFrameMax,
}

#[async_trait::async_trait]
impl SubmitChangeTransport for DialerSubmitTransport<'_> {
    // The only un-mockable step: dial + one `API_KEY_SUBMIT_CHANGE` + close, with
    // no offline signal (a `krabka_client_core::Connection` cannot be built in a
    // test). `#[mutants::skip]` rather than an `exclude_re` because cargo-mutants'
    // name-regex exclusions do not reliably match the struct-field-deletion mutant
    // this method's `ConnectionOptions { .. }` literal generates.
    #[cfg_attr(test, mutants::skip)]
    async fn send_submit_change(
        &self,
        leader: NodeId,
        addr: &str,
        body: Vec<u8>,
    ) -> Result<bytes::Bytes, krabka_client_core::ClientError> {
        let opts = krabka_client_core::ConnectionOptions {
            client_id: self.client_id.to_owned(),
            dispatch_queue_capacity: self.client_dispatch_queue_capacity,
            frame_max: self.client_frame_max,
            ..krabka_client_core::ConnectionOptions::default()
        };
        let conn = self.dialer.dial(leader, addr, opts).await?;
        let resp_body = conn
            .raw_request(
                crate::wire::API_KEY_SUBMIT_CHANGE,
                0,
                bytes::Bytes::from(body),
            )
            .await?;
        conn.close();
        Ok(resp_body)
    }
}

/// `forward_submit_to`'s testable core: serialize → send (via the injected
/// [`SubmitChangeTransport`]) → translate. The real path supplies a
/// [`DialerSubmitTransport`]; tests supply a mock so the serialize/translate
/// decisions carry mutation signal without a live quorum.
async fn forward_submit_via(
    transport: &dyn SubmitChangeTransport,
    leader: NodeId,
    addr: &str,
    records: &[krabka_metadata::MetadataRecord],
) -> Result<crate::SubmitChangeResult, RaftError> {
    let body = encode_submit_change_body(records)?;
    let resp_body = transport
        .send_submit_change(leader, addr, body)
        .await
        .map_err(RaftError::Network)?;
    translate_submit_change_response(&resp_body, leader)
}

/// Build the exact `API_KEY_SUBMIT_CHANGE` v0 request body for `records`:
/// wincode-encode the `Vec<MetadataRecord>`, then frame it with the
/// length-prefixed [`crate::wire::KrabkaSubmitChangeRequest`] codec. Kept
/// byte-for-byte identical to the inlined path so the wire stays exact.
fn encode_submit_change_body(
    records: &[krabka_metadata::MetadataRecord],
) -> Result<Vec<u8>, RaftError> {
    let body_bytes = <serde_wincode::SerdeCompat<Vec<krabka_metadata::MetadataRecord>> as wincode::Serialize>::serialize(
        &records.to_vec(),
    )
    .map_err(RaftError::from)?;
    let payload = crate::wire::KrabkaSubmitChangeRequest {
        records: bytes::Bytes::from(body_bytes),
    };
    let mut body = Vec::with_capacity(payload.records.len() + 4);
    payload.encode_v0(&mut body)?;
    Ok(body)
}

/// Decode a `KrabkaSubmitChangeResponse` from the leader's `resp_body` and map
/// its transport `error_code` into the caller's `Result`:
/// - `0` → applied (`Ok`).
/// - `2` → the leader rejected at apply-time (topic already exists). The wire
///   carries only a code; the topic name is what the caller had in hand.
/// - anything else → collapse to `NotLeader` (`CreateTopics` maps that to the
///   retryable `NOT_CONTROLLER`), preferring the response's `leader_hint` when
///   non-negative and falling back to the dialed `leader`.
fn translate_submit_change_response(
    resp_body: &[u8],
    leader: NodeId,
) -> Result<crate::SubmitChangeResult, RaftError> {
    let mut cur: &[u8] = resp_body;
    let resp = crate::wire::KrabkaSubmitChangeResponse::decode_v0(&mut cur)?;
    match resp.error_code {
        0 => <serde_wincode::SerdeCompat<crate::SubmitChangeResult> as wincode::Deserialize>::deserialize(
            &resp.result,
        )
        .map_err(RaftError::from),
        2 => Err(RaftError::Metadata(
            krabka_metadata::MetadataError::TopicExists(String::new()),
        )),
        _ => Err(RaftError::NotLeader {
            current_leader: (resp.leader_hint >= 0)
                .then(|| NodeId(u64::try_from(resp.leader_hint).unwrap_or(leader.0))),
        }),
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::controller::test_support::{submit_change_response_bytes, topic_record};

    #[test]
    fn controller_endpoint_addr_keeps_dns_hostname_not_parsed_socketaddr() {
        // Regression: a voter endpoint host is a per-pod DNS FQDN, NOT a
        // pre-resolved IP. The resolver must return "<host>:<port>" verbatim so
        // the dialer re-resolves it per connect. Parsing to a `SocketAddr`
        // returns None for a hostname, which silently disabled leader-forwarding
        // of `submit_change` — broker self-registration then failed with "not
        // leader" and RF=3 topics could not be placed.
        let host = "demo-broker-2-0.demo-broker-headless.default.svc.cluster.local";
        let voters = krabka_metadata::VoterSet::from_voters([krabka_metadata::Voter {
            id: NodeId(2),
            directory_id: Uuid::nil(),
            endpoints: vec![krabka_metadata::VoterEndpoint {
                name: "CONTROLLER".to_string(),
                host: host.to_string(),
                port: 9093,
            }],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        }]);
        for (_name, node_id, expected) in [
            ("registered voter", NodeId(2), Some(format!("{host}:9093"))),
            ("unknown voter", NodeId(99), None),
        ] {
            assert2::assert!(controller_endpoint_addr(&voters, node_id) == expected);
        }
    }

    #[test]
    fn controller_endpoint_addr_prefers_controller_endpoint_over_others() {
        // A voter advertises several listeners. The resolver must pick the one
        // named CONTROLLER even when it is not first in the list — submit_change
        // must be forwarded to the controller listener, not (e.g.) the
        // inter-broker REPLICATION listener on a different port. The non-
        // CONTROLLER endpoint is placed FIRST so a flipped `name == "CONTROLLER"`
        // predicate (matching the first NON-controller endpoint instead) returns
        // the wrong address.
        let voters = krabka_metadata::VoterSet::from_voters([krabka_metadata::Voter {
            id: NodeId(7),
            directory_id: Uuid::nil(),
            endpoints: vec![
                krabka_metadata::VoterEndpoint {
                    name: "REPLICATION".to_string(),
                    host: "replication-host".to_string(),
                    port: 9092,
                },
                krabka_metadata::VoterEndpoint {
                    name: "CONTROLLER".to_string(),
                    host: "controller-host".to_string(),
                    port: 9093,
                },
            ],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        }]);
        assert2::assert!(
            controller_endpoint_addr(&voters, NodeId(7))
                == Some("controller-host:9093".to_string())
        );
    }

    #[test]
    fn encode_submit_change_body_frames_wincode_records_with_i32_length_prefix() {
        // The forward path must produce the exact `KrabkaSubmitChangeRequest` v0
        // wire bytes: a 4-byte big-endian length prefix followed by the
        // wincode-encoded `Vec<MetadataRecord>`. Decoding the framed body back
        // and re-deserializing must round-trip to the original records, proving
        // the prefix length matches the payload length (a mutated length or a
        // dropped wincode step fails to decode or yields different records).
        let records = vec![topic_record("alpha"), topic_record("beta")];
        let body = encode_submit_change_body(&records).expect("encode");

        let expected_wincode = <serde_wincode::SerdeCompat<
            Vec<krabka_metadata::MetadataRecord>,
        > as wincode::Serialize>::serialize(&records)
        .expect("wincode");
        assert2::assert!(body.len() == expected_wincode.len() + 4);

        let mut cur: &[u8] = &body;
        let req =
            crate::wire::KrabkaSubmitChangeRequest::decode_v0(&mut cur).expect("decode frame");
        assert2::assert!(req.records.as_ref() == expected_wincode.as_slice());
        // The framed payload IS the wincode encoding of the original records, so
        // it deserializes back to them — proving no double-framing / corruption.
        let decoded = <serde_wincode::SerdeCompat<
            Vec<krabka_metadata::MetadataRecord>,
        > as wincode::Deserialize>::deserialize(&req.records)
        .expect("wincode decode");
        assert2::assert!(decoded == records);
    }

    #[test]
    fn translate_submit_change_response_maps_each_error_code() {
        // 0 => applied.
        assert2::assert!(
            translate_submit_change_response(&submit_change_response_bytes(0, -1), NodeId(5))
                .is_ok()
        );

        // 2 => leader rejected at apply-time: a TopicExists metadata error.
        let err = translate_submit_change_response(&submit_change_response_bytes(2, -1), NodeId(5))
            .expect_err("code 2 is an error");
        assert2::assert!(matches!(
            err,
            RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))
        ));

        // Any other code collapses to NotLeader, taking the response's
        // leader_hint when non-negative.
        let err = translate_submit_change_response(&submit_change_response_bytes(1, 9), NodeId(5))
            .expect_err("code 1 is an error");
        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: Some(NodeId(9))
            }
        ));

        // A negative leader_hint falls back to None (unknown), NOT to the dialed
        // leader id — distinguishing the `>= 0` guard.
        let err = translate_submit_change_response(&submit_change_response_bytes(3, -1), NodeId(5))
            .expect_err("code 3 is an error");
        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: None
            }
        ));
    }

    #[test]
    fn translate_submit_change_response_propagates_decode_error() {
        // A truncated body (fewer than the fixed 10 response bytes) must surface
        // as a protocol error rather than being silently treated as success.
        let err = translate_submit_change_response(&[0u8; 3], NodeId(5))
            .expect_err("truncated decodes err");
        assert2::assert!(matches!(err, RaftError::Protocol(_)));
    }

    #[tokio::test]
    async fn forward_submit_via_sends_encoded_body_and_returns_ok_on_applied() {
        // End-to-end of the testable core: the transport must receive the exact
        // framed body for `records` (the wincode + length-prefix encoding) at the
        // dialed leader/addr, and an `error_code = 0` response yields Ok.
        let records = vec![topic_record("gamma")];
        let expected_body = encode_submit_change_body(&records).expect("encode");

        let mut transport = MockSubmitChangeTransport::new();
        transport
            .expect_send_submit_change()
            .withf(move |leader, addr, body| {
                *leader == 7 && addr == "leader-host:9093" && body == &expected_body
            })
            .times(1)
            .returning(|_, _, _| Ok(submit_change_response_bytes(0, -1)));

        forward_submit_via(&transport, NodeId(7), "leader-host:9093", &records)
            .await
            .expect("applied");
    }

    #[tokio::test]
    async fn forward_submit_via_translates_not_leader_hint() {
        // The transport's response leader_hint must flow through translation to
        // the caller's NotLeader error.
        let mut transport = MockSubmitChangeTransport::new();
        transport
            .expect_send_submit_change()
            .returning(|_, _, _| Ok(submit_change_response_bytes(1, 4)));

        let err = forward_submit_via(
            &transport,
            NodeId(7),
            "leader-host:9093",
            &[topic_record("z")],
        )
        .await
        .expect_err("not leader");
        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: Some(NodeId(4))
            }
        ));
    }

    #[tokio::test]
    async fn forward_submit_via_maps_transport_error_to_network() {
        // A dial/send failure surfaces as RaftError::Network (so CreateTopics
        // retries), not a panic or a swallowed success.
        let mut transport = MockSubmitChangeTransport::new();
        transport.expect_send_submit_change().returning(|_, _, _| {
            Err(krabka_client_core::ClientError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "refused",
            )))
        });

        let err = forward_submit_via(
            &transport,
            NodeId(7),
            "leader-host:9093",
            &[topic_record("z")],
        )
        .await
        .expect_err("network error");
        assert2::assert!(matches!(err, RaftError::Network(_)));
    }
}
