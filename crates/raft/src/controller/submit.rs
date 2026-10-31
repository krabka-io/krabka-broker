//! Record submission: the leader-local `submit_change`, the resolution of a
//! leader's controller-listener address, and the encode → dial → translate path
//! a follower forwards a rejected batch over. The transport seam that dials the
//! leader is isolated here so the encode and translate halves stay testable
//! without a live quorum.

use {
    super::{ControllerHandle, LeaderRoundTrip},
    crate::{
        DelegationTokenMutation, SubmitChangeResult,
        error::RaftError,
        types::{NodeId, controller_endpoint_addr as endpoint_addr_from_endpoints},
        wire::{
            API_KEY_DELEGATION_TOKEN_MUTATION, API_KEY_SUBMIT_CHANGE, KrabkaSubmitChangeRequest,
            KrabkaSubmitChangeResponse, PRIVATE_CLUSTER_AUTHORIZATION_FAILED,
            PRIVATE_UNSUPPORTED_VERSION, SUBMIT_CHANGE_UNCOMMITTED_TAIL, private_request_version,
        },
    },
    krabka_metadata::{MetadataImage, MetadataRecord},
    std::sync::Arc,
};

impl ControllerHandle {
    /// Submit a mutation admitted against this controller's published image.
    /// The engine checks that image before appending; this method never forwards
    /// a mutation whose admission happened on a different controller.
    ///
    /// # Errors
    /// Returns a leadership, stale-image, uncommitted-tail, or submission error.
    pub async fn submit_change_if_image(
        &self,
        records: Vec<MetadataRecord>,
        expected_image: Arc<MetadataImage>,
    ) -> Result<SubmitChangeResult, RaftError> {
        self.engine
            .submit_change_if_image(records, expected_image)
            .await
    }

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
    ) -> Result<SubmitChangeResult, RaftError> {
        let submission = self.engine.submit_change(records.clone()).await;
        match submission {
            Err(RaftError::NotLeader {
                current_leader: Some(leader),
            }) => {
                let addr = self.leader_addr_or_not_leader(leader)?;
                self.forward_submit_to(leader, &addr, &records).await
            }
            other => other,
        }
    }

    /// Submit token mutations whose expected records are checked by the leader
    /// in the same engine turn as the append.
    ///
    /// # Errors
    ///
    /// Returns a leadership, transport, protocol, or mutation-rejection error.
    pub async fn submit_delegation_token_mutations(
        &self,
        mutations: Vec<DelegationTokenMutation>,
    ) -> Result<SubmitChangeResult, RaftError> {
        match self
            .engine
            .submit_delegation_token_mutations(mutations.clone())
            .await
        {
            Err(RaftError::NotLeader {
                current_leader: Some(leader),
            }) => {
                let addr = self.leader_addr_or_not_leader(leader)?;
                self.forward_delegation_token_mutations_to(leader, &addr, &mutations)
                    .await
            }
            other => other,
        }
    }

    // cargo-mutants: thin wrapper; builds the live DialerRoundTrip, needs a real
    // dialer. Its encode → send → translate core is
    // `forward_delegation_token_mutations_via`, which is mock-tested.
    #[cfg_attr(test, mutants::skip)]
    async fn forward_delegation_token_mutations_to(
        &self,
        leader: NodeId,
        addr: &str,
        mutations: &[DelegationTokenMutation],
    ) -> Result<SubmitChangeResult, RaftError> {
        let version = private_request_version(
            krabka_metadata::PrivateRpc::DelegationTokenMutation,
            Some(&self.current_image()),
        );
        forward_delegation_token_mutations_via(
            &self.round_trip_transport(),
            leader,
            addr,
            version,
            mutations,
        )
        .await
    }

    /// Forward a raw Kafka-wire request for `api_key`/`version` to the
    /// current quorum leader's controller listener and return its raw
    /// response bytes, unmodified.
    ///
    /// Used by the broker's `DescribeQuorum` handler: Kafka's `KafkaApis`
    /// forwards `DescribeQuorum` from the broker listener to the active
    /// controller unconditionally (`forwardToController`), so a combined or
    /// controller node that is not itself the leader must reach the leader
    /// the same way a broker-only observer already does.
    ///
    /// # Errors
    /// `RaftError::NotLeader` when no leader is known or the leader's
    /// address cannot be resolved, `RaftError::Network` when the dial or
    /// round trip fails.
    pub async fn forward_raw(
        &self,
        api_key: i16,
        version: i16,
        body: bytes::Bytes,
    ) -> Result<bytes::Bytes, RaftError> {
        let Some(leader) = self.engine.quorum_snapshot().leader_id else {
            return Err(RaftError::NotLeader {
                current_leader: None,
            });
        };
        let addr = self.leader_addr_or_not_leader(leader)?;
        forward_raw_via(
            &self.round_trip_transport(),
            leader,
            &addr,
            api_key,
            version,
            body,
        )
        .await
    }

    /// Resolve `leader`'s controller listener `<host>:<port>` to forward to,
    /// or reject with `NotLeader` naming `leader` when it has no known
    /// address.
    fn leader_addr_or_not_leader(&self, leader: NodeId) -> Result<String, RaftError> {
        self.voter_addr(leader).ok_or(RaftError::NotLeader {
            current_leader: Some(leader),
        })
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
    /// the exact wire bytes, the [`LeaderRoundTrip`] seam performs the
    /// (un-mockable) dial→`raw_request`→close round trip and hands back the raw
    /// response body, and [`translate_submit_change_response`] decodes that body
    /// and maps the transport `error_code` into a `RaftError`.
    // cargo-mutants: thin wrapper; builds the live DialerRoundTrip, needs a real dialer
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
    ) -> Result<SubmitChangeResult, RaftError> {
        let version = private_request_version(
            krabka_metadata::PrivateRpc::SubmitChange,
            Some(&self.current_image()),
        );
        forward_submit_via(&self.round_trip_transport(), leader, addr, version, records).await
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

/// `forward_submit_to`'s testable core: serialize → send (via the injected
/// [`LeaderRoundTrip`]) → translate. The real path supplies a
/// [`DialerRoundTrip`]; tests supply a mock so the serialize/translate
/// decisions carry mutation signal without a live quorum.
async fn forward_submit_via(
    transport: &dyn LeaderRoundTrip,
    leader: NodeId,
    addr: &str,
    version: i16,
    records: &[krabka_metadata::MetadataRecord],
) -> Result<SubmitChangeResult, RaftError> {
    let body = encode_submit_change_body(records)?;
    forward_encoded_via(
        transport,
        leader,
        addr,
        API_KEY_SUBMIT_CHANGE,
        version,
        body,
    )
    .await
}

/// `forward_delegation_token_mutations_to`'s testable core: serialize → send
/// (via the injected [`LeaderRoundTrip`]) → translate.
async fn forward_delegation_token_mutations_via(
    transport: &dyn LeaderRoundTrip,
    leader: NodeId,
    addr: &str,
    version: i16,
    mutations: &[DelegationTokenMutation],
) -> Result<SubmitChangeResult, RaftError> {
    let body = encode_delegation_token_mutation_body(mutations)?;
    forward_encoded_via(
        transport,
        leader,
        addr,
        API_KEY_DELEGATION_TOKEN_MUTATION,
        version,
        body,
    )
    .await
}

/// Send an already-encoded `SubmitChange`-shaped `body` as `api_key` and
/// translate the leader's reply with [`translate_submit_change_response`].
async fn forward_encoded_via(
    transport: &dyn LeaderRoundTrip,
    leader: NodeId,
    addr: &str,
    api_key: i16,
    version: i16,
    body: Vec<u8>,
) -> Result<SubmitChangeResult, RaftError> {
    let resp_body = forward_raw_via(
        transport,
        leader,
        addr,
        api_key,
        version,
        bytes::Bytes::from(body),
    )
    .await?;
    translate_submit_change_response(&resp_body, leader, api_key, version)
}

/// One round trip over `transport`, returning the response body unmodified
/// and surfacing a dial or request failure as [`RaftError::Network`].
async fn forward_raw_via(
    transport: &dyn LeaderRoundTrip,
    leader: NodeId,
    addr: &str,
    api_key: i16,
    version: i16,
    body: bytes::Bytes,
) -> Result<bytes::Bytes, RaftError> {
    transport
        .round_trip(leader, addr, api_key, version, body)
        .await
        .map_err(RaftError::Network)
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
    submit_change_frame(body_bytes)
}

fn encode_delegation_token_mutation_body(
    mutations: &[DelegationTokenMutation],
) -> Result<Vec<u8>, RaftError> {
    let payload = <serde_wincode::SerdeCompat<Vec<DelegationTokenMutation>> as wincode::Serialize>::serialize(
        &mutations.to_vec(),
    )
    .map_err(RaftError::from)?;
    submit_change_frame(payload)
}

fn submit_change_frame(payload: Vec<u8>) -> Result<Vec<u8>, RaftError> {
    let request = KrabkaSubmitChangeRequest {
        records: bytes::Bytes::from(payload),
    };
    let mut body = Vec::with_capacity(request.records.len() + 4);
    request.encode_v0(&mut body)?;
    Ok(body)
}

/// Decode a `KrabkaSubmitChangeResponse` from the leader's `resp_body` and map
/// its transport `error_code` into the caller's `Result`:
/// - `0` → applied (`Ok`).
/// - `2` → the leader rejected at apply-time (topic already exists). The wire
///   carries only a code; the topic name is what the caller had in hand.
/// - [`crate::wire::SUBMIT_CHANGE_UNCOMMITTED_TAIL`] → the leader refused a
///   compare-and-set until its tail commits ([`RaftError::UncommittedTail`]).
/// - [`crate::wire::PRIVATE_CLUSTER_AUTHORIZATION_FAILED`] → the leader denied
///   `ClusterAction` to this node's principal
///   ([`RaftError::ClusterAuthorizationFailed`]).
/// - [`crate::wire::PRIVATE_UNSUPPORTED_VERSION`] → the leader does not
///   implement `version`, the version of `api_key` this node sent
///   ([`RaftError::UnsupportedPrivateVersion`]). `krabka.version` negotiation
///   keeps a sender at a version every registered node serves, so only a peer
///   outside that agreement answers it.
/// - anything else → collapse to `NotLeader` (`CreateTopics` maps that to the
///   retryable `NOT_CONTROLLER`), preferring the response's `leader_hint` when
///   non-negative and falling back to the dialed `leader`.
fn translate_submit_change_response(
    resp_body: &[u8],
    leader: NodeId,
    api_key: i16,
    version: i16,
) -> Result<SubmitChangeResult, RaftError> {
    let mut cur: &[u8] = resp_body;
    let resp = KrabkaSubmitChangeResponse::decode_v0(&mut cur)?;
    match resp.error_code {
        0 => <serde_wincode::SerdeCompat<SubmitChangeResult> as wincode::Deserialize>::deserialize(
            &resp.result,
        )
        .map_err(RaftError::from),
        2 => Err(RaftError::Metadata(
            krabka_metadata::MetadataError::TopicExists(String::new()),
        )),
        SUBMIT_CHANGE_UNCOMMITTED_TAIL => Err(RaftError::UncommittedTail),
        PRIVATE_CLUSTER_AUTHORIZATION_FAILED => Err(RaftError::ClusterAuthorizationFailed),
        PRIVATE_UNSUPPORTED_VERSION => {
            Err(RaftError::UnsupportedPrivateVersion { api_key, version })
        }
        _ => Err(RaftError::NotLeader {
            current_leader: (resp.leader_hint >= 0)
                .then(|| NodeId(u64::try_from(resp.leader_hint).unwrap_or(leader.0))),
        }),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            DelegationTokenMutation, SubmitChangeResult,
            controller::{
                MockLeaderRoundTrip,
                test_support::{
                    bootstrap_controller, joining_controller, submit_change_response_bytes,
                    topic_record,
                },
            },
            wire::{
                API_KEY_DELEGATION_TOKEN_MUTATION, API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE,
                KrabkaSubmitChangeRequest, PRIVATE_CLUSTER_AUTHORIZATION_FAILED,
                PRIVATE_UNSUPPORTED_VERSION, SUBMIT_CHANGE_UNCOMMITTED_TAIL,
            },
        },
        assert2::check,
        std::io::{Error, ErrorKind},
        uuid::Uuid,
    };

    #[derive(Clone, Copy)]
    enum SubmitResponseExpectation {
        Applied,
        TopicExists,
        UncommittedTail,
        ClusterAuthorizationFailed,
        UnsupportedVersion,
        NotLeaderWithHint,
        NotLeaderWithoutHint,
    }

    #[yare::parameterized(
        registered_voter = {
            NodeId(2),
            Some("demo-broker-2-0.demo-broker-headless.default.svc.cluster.local:9093".to_owned()),
        },
        unknown_voter = { NodeId(99), None },
    )]
    fn controller_endpoint_addr_keeps_dns_hostname_not_parsed_socketaddr(
        node_id: NodeId,
        expected: Option<String>,
    ) {
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
                name: "CONTROLLER".to_owned(),
                host: host.to_owned(),
                port: 9093,
            }],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        }]);
        assert2::assert!(controller_endpoint_addr(&voters, node_id) == expected);
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
                    name: "REPLICATION".to_owned(),
                    host: "replication-host".to_owned(),
                    port: 9092,
                },
                krabka_metadata::VoterEndpoint {
                    name: "CONTROLLER".to_owned(),
                    host: "controller-host".to_owned(),
                    port: 9093,
                },
            ],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        }]);
        assert2::assert!(
            controller_endpoint_addr(&voters, NodeId(7)) == Some("controller-host:9093".to_owned())
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
        let req = KrabkaSubmitChangeRequest::decode_v0(&mut cur).expect("decode frame");
        assert2::assert!(req.records.as_ref() == expected_wincode.as_slice());
        // The framed payload IS the wincode encoding of the original records, so
        // it deserializes back to them — proving no double-framing / corruption.
        let decoded = <serde_wincode::SerdeCompat<
            Vec<krabka_metadata::MetadataRecord>,
        > as wincode::Deserialize>::deserialize(&req.records)
        .expect("wincode decode");
        assert2::assert!(decoded == records);
    }

    /// A `SubmitChange` v0 request body that carries one `TopicRecord`.
    const SUBMIT_CHANGE_V0_GOLDEN: &[u8] = &[
        0, 0, 0, 51, // frame length
        1, 0, 0, 0, 0, 0, 0, 0, // one record
        0, 0, 0, 0, // V1Topic
        1, 0, 0, 0, 0, 0, 0, 0, b't', // name
        16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, // topic id
        1, 0, 0, 0, // partitions
        1, 0, // replication factor
    ];

    /// A `DelegationTokenMutation` v0 request body that carries one `Delete`.
    fn delegation_token_mutation_v0_golden() -> Vec<u8> {
        // Captured wincode principal, shared by owner, requester and renewer.
        // These chunks are literal oracle bytes, independent of serialization.
        const USER_ALICE: &[u8] = &[
            4, 0, 0, 0, 0, 0, 0, 0, b'U', b's', b'e', b'r', 5, 0, 0, 0, 0, 0, 0, 0, b'a', b'l',
            b'i', b'c', b'e',
        ];
        let prefix: &[u8] = &[
            0, 0, 0, 130, // frame length
            1, 0, 0, 0, 0, 0, 0, 0, // one mutation
            2, 0, 0, 0, // Delete
            3, 0, 0, 0, 0, 0, 0, 0, b't', b'o', b'k', // token id
        ];
        let timestamps_and_renewer_count: &[u8] = &[
            1, 0, 0, 0, 0, 0, 0, 0, // issue timestamp
            2, 0, 0, 0, 0, 0, 0, 0, // expiry timestamp
            3, 0, 0, 0, 0, 0, 0, 0, // max timestamp
            1, 0, 0, 0, 0, 0, 0, 0, // one renewer
        ];
        [
            prefix,
            USER_ALICE,
            USER_ALICE,
            timestamps_and_renewer_count,
            USER_ALICE,
        ]
        .concat()
    }

    /// The wincode `SubmitChangeResult` of a `SubmitChange` v0 response with
    /// one offset reservation.
    const SUBMIT_CHANGE_RESULT_V0_GOLDEN: &[u8] = &[
        1, 0, 0, 0, 0, 0, 0, 0, // one reservation
        1, 0, 0, 0, 0, 0, 0, 0, b't', // topic
        0, 0, 0, 0, // partition
        5, 0, 0, 0, 0, 0, 0, 0, // base offset
        2, 0, 0, 0, 0, 0, 0, 0, // count
        3, 0, 0, 0, 0, 0, 0, 0, // leader epoch
    ];

    /// One delegation token, the representative value of the
    /// `DelegationTokenMutation` golden body.
    fn golden_token() -> krabka_metadata::DelegationTokenRecord {
        let alice = krabka_security::KafkaPrincipal {
            principal_type: "User".into(),
            name: "alice".into(),
        };
        krabka_metadata::DelegationTokenRecord {
            token_id: "tok".into(),
            owner: alice.clone(),
            requester: alice.clone(),
            issue_timestamp_ms: 1,
            expiry_timestamp_ms: 2,
            max_timestamp_ms: 3,
            renewers: vec![alice],
        }
    }

    /// The bodies of `SubmitChange` and `DelegationTokenMutation` v0 requests and
    /// of a `SubmitChange` v0 result, byte for byte: the length-prefixed frame
    /// around the wincode payload. Nodes of two 1.x builds exchange these
    /// during a rolling upgrade, so a change to one of them is a change to the
    /// 1.x contract and takes a new api version.
    #[test]
    fn v0_payloads_match_their_golden_bytes() {
        let delegation_golden = delegation_token_mutation_v0_golden();
        let records = vec![krabka_metadata::MetadataRecord::V1Topic(
            krabka_metadata::TopicRecord {
                name: "t".into(),
                topic_id: Uuid::from_u128(1),
                partitions: 1,
                replication_factor: 1,
            },
        )];
        let mutations = vec![crate::DelegationTokenMutation::Delete {
            expected: golden_token(),
        }];
        let result = SubmitChangeResult {
            offset_reservations: vec![crate::OffsetReservation {
                topic: "t".into(),
                partition: 0,
                base_offset: 5,
                count: 2,
                leader_epoch: 3,
            }],
        };
        let result_bytes =
            <serde_wincode::SerdeCompat<SubmitChangeResult> as wincode::Serialize>::serialize(
                &result,
            )
            .expect("wincode");

        check!(encode_submit_change_body(&records).unwrap() == SUBMIT_CHANGE_V0_GOLDEN);
        check!(encode_delegation_token_mutation_body(&mutations).unwrap() == delegation_golden);
        check!(result_bytes == SUBMIT_CHANGE_RESULT_V0_GOLDEN);

        // Each golden body decodes back to the value it was made from.
        let mut cur: &[u8] = SUBMIT_CHANGE_V0_GOLDEN;
        let frame = KrabkaSubmitChangeRequest::decode_v0(&mut cur).unwrap();
        check!(
            <serde_wincode::SerdeCompat<Vec<krabka_metadata::MetadataRecord>> as wincode::Deserialize>::deserialize(&frame.records).unwrap()
                == records
        );
        let mut cur: &[u8] = &delegation_golden;
        let frame = KrabkaSubmitChangeRequest::decode_v0(&mut cur).unwrap();
        check!(
            <serde_wincode::SerdeCompat<Vec<crate::DelegationTokenMutation>> as wincode::Deserialize>::deserialize(&frame.records).unwrap()
                == mutations
        );
        check!(
            <serde_wincode::SerdeCompat<crate::SubmitChangeResult> as wincode::Deserialize>::deserialize(SUBMIT_CHANGE_RESULT_V0_GOLDEN).unwrap()
                == result
        );
    }

    #[yare::parameterized(
        applied = { 0, -1, API_KEY_SUBMIT_CHANGE, 0, SubmitResponseExpectation::Applied },
        topic_exists = { 2, -1, API_KEY_SUBMIT_CHANGE, 0, SubmitResponseExpectation::TopicExists },
        uncommitted_tail = {
            SUBMIT_CHANGE_UNCOMMITTED_TAIL,
            -1,
            API_KEY_SUBMIT_CHANGE,
            0,
            SubmitResponseExpectation::UncommittedTail,
        },
        cluster_authorization = {
            PRIVATE_CLUSTER_AUTHORIZATION_FAILED,
            -1,
            API_KEY_SUBMIT_CHANGE,
            0,
            SubmitResponseExpectation::ClusterAuthorizationFailed,
        },
        submit_unsupported_version = {
            PRIVATE_UNSUPPORTED_VERSION,
            -1,
            API_KEY_SUBMIT_CHANGE,
            2,
            SubmitResponseExpectation::UnsupportedVersion,
        },
        token_unsupported_version = {
            PRIVATE_UNSUPPORTED_VERSION,
            -1,
            API_KEY_DELEGATION_TOKEN_MUTATION,
            2,
            SubmitResponseExpectation::UnsupportedVersion,
        },
        not_leader_with_hint = {
            1,
            9,
            API_KEY_SUBMIT_CHANGE,
            0,
            SubmitResponseExpectation::NotLeaderWithHint,
        },
        not_leader_without_hint = {
            3,
            -1,
            API_KEY_SUBMIT_CHANGE,
            0,
            SubmitResponseExpectation::NotLeaderWithoutHint,
        },
    )]
    fn translate_submit_change_response_maps_each_error_code(
        code: i16,
        leader_hint: i64,
        api_key: i16,
        version: i16,
        expected: SubmitResponseExpectation,
    ) {
        let result = translate_submit_change_response(
            &submit_change_response_bytes(code, leader_hint),
            NodeId(5),
            api_key,
            version,
        );
        match expected {
            SubmitResponseExpectation::Applied => assert2::assert!(result.is_ok()),
            expected => {
                let error = result.expect_err("response is an error");
                let matches = match expected {
                    SubmitResponseExpectation::Applied => unreachable!(),
                    SubmitResponseExpectation::TopicExists => matches!(
                        error,
                        RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))
                    ),
                    SubmitResponseExpectation::UncommittedTail => {
                        matches!(error, RaftError::UncommittedTail)
                    }
                    SubmitResponseExpectation::ClusterAuthorizationFailed => {
                        matches!(error, RaftError::ClusterAuthorizationFailed)
                    }
                    SubmitResponseExpectation::UnsupportedVersion => matches!(
                        error,
                        RaftError::UnsupportedPrivateVersion {
                            api_key: got,
                            version: got_version
                        } if got == api_key && got_version == version
                    ),
                    SubmitResponseExpectation::NotLeaderWithHint => {
                        matches!(error, RaftError::NotLeader {
                            current_leader: Some(NodeId(9))
                        })
                    }
                    SubmitResponseExpectation::NotLeaderWithoutHint => {
                        matches!(error, RaftError::NotLeader {
                            current_leader: None
                        })
                    }
                };
                assert2::assert!(matches);
            }
        }
    }

    #[test]
    fn translate_submit_change_response_propagates_decode_error() {
        // A truncated body (fewer than the fixed 10 response bytes) must surface
        // as a protocol error rather than being silently treated as success.
        let err = translate_submit_change_response(&[0u8; 3], NodeId(5), API_KEY_SUBMIT_CHANGE, 0)
            .expect_err("truncated decodes err");
        assert2::assert!(matches!(err, RaftError::Protocol(_)));
    }

    const LEADER: NodeId = NodeId(7);
    const LEADER_ADDR: &str = "leader-host:9093";

    /// A transport that expects exactly one round trip of `body` as `api_key`
    /// at `version` to [`LEADER`] on [`LEADER_ADDR`], and answers `response`.
    fn expect_one_round_trip(
        api_key: i16,
        version: i16,
        body: impl Into<bytes::Bytes>,
        response: bytes::Bytes,
    ) -> MockLeaderRoundTrip {
        let body = body.into();
        let mut transport = MockLeaderRoundTrip::new();
        transport
            .expect_round_trip()
            .withf(move |target, addr, sent_key, sent_version, sent| {
                *target == LEADER
                    && addr == LEADER_ADDR
                    && *sent_key == api_key
                    && *sent_version == version
                    && sent == &body
            })
            .times(1)
            .returning(move |_, _, _, _, _| Ok(response.clone()));
        transport
    }

    /// A transport whose every dial is refused.
    fn refused_transport() -> MockLeaderRoundTrip {
        let mut transport = MockLeaderRoundTrip::new();
        transport.expect_round_trip().returning(|_, _, _, _, _| {
            Err(krabka_client_core::ClientError::Io(Error::new(
                ErrorKind::ConnectionRefused,
                "refused",
            )))
        });
        transport
    }

    #[tokio::test]
    async fn forward_submit_via_sends_encoded_body_and_returns_ok_on_applied() {
        // End-to-end of the testable core: the transport must receive the exact
        // framed body for `records` (the wincode + length-prefix encoding) at the
        // dialed leader/addr, and an `error_code = 0` response yields Ok.
        let records = vec![topic_record("gamma")];
        let expected_body = encode_submit_change_body(&records).expect("encode");

        let transport = expect_one_round_trip(
            API_KEY_SUBMIT_CHANGE,
            0,
            expected_body,
            submit_change_response_bytes(0, -1),
        );

        forward_submit_via(&transport, LEADER, LEADER_ADDR, 0, &records)
            .await
            .expect("applied");
    }

    #[tokio::test]
    async fn forward_submit_via_translates_not_leader_hint() {
        // The transport's response leader_hint must flow through translation to
        // the caller's NotLeader error.
        let mut transport = MockLeaderRoundTrip::new();
        transport
            .expect_round_trip()
            .returning(|_, _, _, _, _| Ok(submit_change_response_bytes(1, 4)));

        let err = forward_submit_via(&transport, NodeId(7), "leader-host:9093", 0, &[
            topic_record("z"),
        ])
        .await
        .expect_err("not leader");
        assert2::assert!(matches!(err, RaftError::NotLeader {
            current_leader: Some(NodeId(4))
        }));
    }

    #[tokio::test]
    async fn forward_submit_via_maps_transport_error_to_network() {
        // A dial/send failure surfaces as RaftError::Network (so CreateTopics
        // retries), not a panic or a swallowed success.
        let transport = refused_transport();

        let err = forward_submit_via(&transport, NodeId(7), "leader-host:9093", 0, &[
            topic_record("z"),
        ])
        .await
        .expect_err("network error");
        assert2::assert!(matches!(err, RaftError::Network(_)));
    }

    #[tokio::test]
    async fn forward_delegation_token_mutations_via_sends_encoded_body_and_returns_ok_on_applied() {
        // The transport must receive the exact framed mutations body as
        // `API_KEY_DELEGATION_TOKEN_MUTATION` at the dialed leader/addr and
        // version, and an `error_code = 0` response yields the decoded result.
        let mutations = vec![DelegationTokenMutation::Delete {
            expected: golden_token(),
        }];
        let expected_body = encode_delegation_token_mutation_body(&mutations).expect("encode");

        let transport = expect_one_round_trip(
            API_KEY_DELEGATION_TOKEN_MUTATION,
            1,
            expected_body,
            submit_change_response_bytes(0, -1),
        );

        let result =
            forward_delegation_token_mutations_via(&transport, LEADER, LEADER_ADDR, 1, &mutations)
                .await
                .expect("applied");
        assert2::assert!(result == SubmitChangeResult::default());
    }

    #[tokio::test]
    async fn forward_delegation_token_mutations_via_reports_its_own_api_key_on_unsupported_version()
    {
        // The leader's reply is translated against the delegation-token api
        // key, not the submit-change one.
        let mut transport = MockLeaderRoundTrip::new();
        transport.expect_round_trip().returning(|_, _, _, _, _| {
            Ok(submit_change_response_bytes(
                PRIVATE_UNSUPPORTED_VERSION,
                -1,
            ))
        });

        let err = forward_delegation_token_mutations_via(
            &transport,
            NodeId(7),
            "leader-host:9093",
            2,
            &[],
        )
        .await
        .expect_err("unsupported version");
        assert2::assert!(matches!(err, RaftError::UnsupportedPrivateVersion {
            api_key: API_KEY_DELEGATION_TOKEN_MUTATION,
            version: 2,
        }));
    }

    #[tokio::test]
    async fn forward_delegation_token_mutations_via_maps_transport_error_to_network() {
        let err = forward_delegation_token_mutations_via(
            &refused_transport(),
            LEADER,
            LEADER_ADDR,
            0,
            &[],
        )
        .await
        .expect_err("network error");
        assert2::assert!(matches!(err, RaftError::Network(_)));
    }

    #[tokio::test]
    async fn forward_raw_via_sends_request_verbatim_and_returns_response_unmodified() {
        // `forward_raw` relays a Kafka-wire request it does not interpret: the
        // api key, version and body reach the transport as given, and the
        // response bytes come back untouched.
        let body = bytes::Bytes::from_static(b"describe-quorum-request");
        let transport = expect_one_round_trip(
            55,
            2,
            body.clone(),
            bytes::Bytes::from_static(b"raw-response"),
        );

        let response = forward_raw_via(&transport, LEADER, LEADER_ADDR, 55, 2, body)
            .await
            .expect("round trip");
        assert2::assert!(response == bytes::Bytes::from_static(b"raw-response"));
    }

    #[tokio::test]
    async fn forward_raw_via_maps_transport_error_to_network() {
        let err = forward_raw_via(
            &refused_transport(),
            LEADER,
            LEADER_ADDR,
            55,
            2,
            bytes::Bytes::new(),
        )
        .await
        .expect_err("network error");
        assert2::assert!(matches!(err, RaftError::Network(_)));
    }

    #[test]
    fn encode_delegation_token_mutation_body_frames_mutations() {
        use {krabka_metadata::DelegationTokenRecord, krabka_security::KafkaPrincipal};

        let rec = DelegationTokenRecord {
            token_id: "tok".into(),
            owner: KafkaPrincipal {
                principal_type: "User".into(),
                name: "alice".into(),
            },
            requester: KafkaPrincipal {
                principal_type: "User".into(),
                name: "alice".into(),
            },
            issue_timestamp_ms: 10,
            max_timestamp_ms: 20,
            expiry_timestamp_ms: 15,
            renewers: vec![],
        };
        let mutations = vec![crate::DelegationTokenMutation::Delete { expected: rec }];
        let body = encode_delegation_token_mutation_body(&mutations).expect("encode");
        assert2::assert!(body.len() > 10);
        let mut cur: &[u8] = &body;
        let req = KrabkaSubmitChangeRequest::decode_v0(&mut cur).expect("decode frame");
        let decoded = <serde_wincode::SerdeCompat<
            Vec<DelegationTokenMutation>,
        > as wincode::Deserialize>::deserialize(&req.records)
        .expect("wincode decode");
        assert2::assert!(decoded == mutations);
    }

    #[tokio::test]
    async fn controller_handle_voter_addr_resolves_known_voter() {
        let (_dir, ctrl) = bootstrap_controller("start").await;
        let addr = ctrl.voter_addr(NodeId(1));
        assert2::assert!(addr == Some("127.0.0.1:0".to_owned()));
        assert2::assert!(ctrl.voter_addr(NodeId(999)).is_none());
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn forward_raw_with_no_known_leader_rejects_not_leader() {
        let (_dir, ctrl) = joining_controller("join start").await;

        let err = ctrl
            .forward_raw(API_KEY_METADATA_FETCH, 0, bytes::Bytes::new())
            .await
            .expect_err("no leader is known yet");
        assert2::assert!(matches!(err, RaftError::NotLeader {
            current_leader: None
        }));
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn submit_delegation_token_mutations_on_join_node_rejects_not_leader() {
        let (_dir, ctrl) = joining_controller("join start").await;
        let res = ctrl.submit_delegation_token_mutations(vec![]).await;
        assert2::assert!(matches!(
            res,
            Err(RaftError::NotLeader {
                current_leader: None
            })
        ));
        ctrl.shutdown().await;
    }
}
