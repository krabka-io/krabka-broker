//! `BrokerRegistration` (`api_key=62`). KIP-631/KIP-903 broker registration.

use std::collections::HashSet;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use krabka_metadata::{
    AclOperation, BrokerEndpoint, BrokerRegistrationRecord, MetadataRecord, NodeId, ResourceType,
};
use krabka_protocol::{
    Decode,
    owned::{
        broker_registration_request::{BrokerRegistrationRequest, Listener},
        broker_registration_response::BrokerRegistrationResponse,
    },
};
use krabka_raft::RaftError;
use krabka_security::ListenerProtocol;

use crate::{broker::Broker, codes, error::BrokerError, handlers::RequestContext};

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur = req_bytes;
    let req = BrokerRegistrationRequest::decode(&mut cur, version)?;
    let image = broker.controller.current_image();

    if crate::handlers::acl_denied(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        ResourceType::Cluster,
        crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
        AclOperation::ClusterAction,
    ) {
        return response(version, codes::CLUSTER_AUTHORIZATION_FAILED, -1);
    }
    if broker.controller.watch_leader().borrow().as_ref() != Some(&broker.config.node_id) {
        return response(version, codes::NOT_CONTROLLER, -1);
    }

    let node_id = match u64::try_from(req.broker_id) {
        Ok(id) => NodeId(id),
        Err(_) => return response(version, codes::INVALID_REGISTRATION, -1),
    };
    if !cluster_id_matches(&req.cluster_id, image.cluster_id()) {
        return response(version, codes::INCONSISTENT_CLUSTER_ID, -1);
    }
    if req.is_migrating_zk_broker {
        return response(version, codes::BROKER_ID_NOT_REGISTERED, -1);
    }
    let endpoints = match decode_listeners(&req.listeners) {
        Ok(endpoints) => endpoints,
        Err(code) => return response(version, code, -1),
    };
    if !features_support_finalized(&req, &image) {
        return response(version, codes::UNSUPPORTED_VERSION, -1);
    }

    let incarnation_id = uuid::Uuid::from_bytes(req.incarnation_id.0);
    if let Some(existing) = image.broker(node_id) {
        if existing.incarnation_id == incarnation_id {
            // Retried registration from the same process. Kafka preserves its
            // epoch; returning it makes the operation idempotent.
            return response(version, 0, existing.broker_epoch);
        }
        if broker.liveness.is_alive(node_id.0).await {
            return response(version, codes::DUPLICATE_BROKER_REGISTRATION, -1);
        }
    }
    let clean_restart = clean_shutdown_proven(&req, version, &image, node_id);

    let first = &endpoints[0];
    let features = req
        .features
        .into_iter()
        .map(|feature| {
            (
                feature.name,
                (feature.min_supported_version, feature.max_supported_version),
            )
        })
        .collect();
    let log_dirs = req
        .log_dirs
        .iter()
        .map(|directory| uuid::Uuid::from_bytes(directory.0))
        .collect();
    let record = BrokerRegistrationRecord {
        node_id,
        broker_epoch: 0,
        incarnation_id,
        host: first.host.clone(),
        port: first.port,
        rack: req.rack,
        endpoints,
        log_dirs,
        features,
    };
    // KIP-966: a broker that cannot prove it stopped gracefully may have lost
    // an unflushed log tail, so nothing the cluster still believes about that
    // log holds -- not its ELR membership, and not its ISR seat either.
    // `ClusterControlManager.registerBroker` calls
    // `handleBrokerShutdown(id, isCleanShutdown, records)` before it appends
    // the `RegisterBrokerRecord`, and the branch that boolean picks is the
    // whole difference: `isElrFeatureEnabled() && !isCleanShutdown` runs two
    // `generateLeaderAndIsrUpdates` calls, which is what
    // `compute_unclean_restart_changes` is. A restart that proves itself
    // clean takes none of that.
    let restart = if clean_restart {
        crate::leader_election::FailoverPlan::default()
    } else {
        crate::leader_election::compute_unclean_restart_changes(
            &image,
            node_id,
            &broker.liveness,
            &broker.metrics,
        )
        .await
    };
    for (topic, partition) in &restart.unavailable {
        tracing::warn!(
            %topic, partition, node_id = node_id.0,
            "returning broker led this partition and no live ISR replica can take it; partition unavailable"
        );
    }
    if let Err(error) = broker
        .controller
        .submit_change(registration_records(restart.changes, record))
        .await
    {
        return response(version, raft_error_code(&error), -1);
    }
    // KIP-966: a partition whose topic opted into an offset-aware recovery
    // strategy is handed to the Unclean Recovery Manager, the same way the
    // dead-broker failover hands one over. Fire and forget.
    for (topic, partition, strategy) in restart.recoveries {
        broker
            .unclean_recovery
            .enqueue(crate::unclean_recovery::RecoveryJob {
                topic,
                partition,
                strategy,
                reply: None,
                // Nobody asked for this recovery, so there is no proposal to
                // name and nobody to refuse; `break_glass` decides whether the
                // URM runs it.
                proposal: None,
            })
            .await;
    }

    let epoch = broker
        .controller
        .current_image()
        .broker(node_id)
        .map_or(-1, |registration| registration.broker_epoch);
    response(
        version,
        if epoch < 0 {
            codes::UNKNOWN_SERVER_ERROR
        } else {
            0
        },
        epoch,
    )
}

/// The records one accepted registration writes, in the order they apply.
///
/// `restart` is what a restart that could not prove itself clean costs, empty
/// for one that did: the ELR withdrawals and ISR removals
/// [`compute_unclean_restart_changes`](crate::leader_election::compute_unclean_restart_changes)
/// decided. They go ahead of the registration, so a replay that stops between
/// the two has already stopped trusting the returning log rather than not yet
/// started.
fn registration_records(
    mut restart: Vec<MetadataRecord>,
    record: BrokerRegistrationRecord,
) -> Vec<MetadataRecord> {
    restart.push(MetadataRecord::V1BrokerRegistration(record));
    restart
}

/// Whether this registration proves the broker stopped gracefully last time.
///
/// `previous_broker_epoch` reaches the wire only at v3, and Kafka's
/// `QuorumController` mirrors that by passing
/// `cleanShutdownDetectionEnabled = requestApiVersion >= 3` into
/// `ClusterControlManager.registerBroker`, which forces the comparison to
/// `false` for anything older. A broker that cannot say what epoch it last
/// held is not trusted to have held one.
fn clean_shutdown_proven(
    req: &BrokerRegistrationRequest,
    version: i16,
    image: &krabka_metadata::MetadataImage,
    node_id: NodeId,
) -> bool {
    version >= 3
        && crate::clean_shutdown::restart_was_clean(image, node_id, req.previous_broker_epoch)
}

fn cluster_id_matches(request: &str, cluster_id: uuid::Uuid) -> bool {
    request == cluster_id.to_string() || request == URL_SAFE_NO_PAD.encode(cluster_id.as_bytes())
}

fn decode_listeners(listeners: &[Listener]) -> Result<Vec<BrokerEndpoint>, i16> {
    if listeners.is_empty() {
        return Err(codes::INVALID_REGISTRATION);
    }
    let mut names = HashSet::with_capacity(listeners.len());
    listeners
        .iter()
        .map(|listener| {
            if listener.name.is_empty()
                || listener.host.is_empty()
                || listener.port == 0
                || !names.insert(listener.name.clone())
            {
                return Err(codes::INVALID_REGISTRATION);
            }
            Ok(BrokerEndpoint {
                name: listener.name.clone(),
                host: listener.host.clone(),
                port: listener.port,
                protocol: protocol_from_wire(listener.security_protocol)
                    .ok_or(codes::INVALID_REGISTRATION)?,
            })
        })
        .collect()
}

fn protocol_from_wire(protocol: i16) -> Option<ListenerProtocol> {
    match protocol {
        0 => Some(ListenerProtocol::Plaintext),
        1 => Some(ListenerProtocol::Ssl),
        2 => Some(ListenerProtocol::SaslPlaintext),
        3 => Some(ListenerProtocol::SaslSsl),
        _ => None,
    }
}

fn features_support_finalized(
    req: &BrokerRegistrationRequest,
    image: &krabka_metadata::MetadataImage,
) -> bool {
    image.finalized_features().iter().all(|(name, level)| {
        req.features.iter().any(|feature| {
            feature.name == *name
                && feature.min_supported_version <= *level
                && *level <= feature.max_supported_version
        })
    })
}

fn raft_error_code(error: &RaftError) -> i16 {
    match error {
        RaftError::NotLeader { .. } | RaftError::LeaderUnknown => codes::NOT_CONTROLLER,
        RaftError::Metadata(_) => codes::INVALID_REGISTRATION,
        _ => codes::UNKNOWN_SERVER_ERROR,
    }
}

fn response(version: i16, error_code: i16, broker_epoch: i64) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(
        &BrokerRegistrationResponse {
            error_code,
            broker_epoch,
            ..Default::default()
        },
        version,
    )
}

#[cfg(test)]
mod tests {
    use krabka_protocol::owned::broker_registration_request::Feature;

    use super::*;

    #[test]
    fn accepts_uuid_and_kafka_base64_cluster_ids() {
        let id = uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        assert2::assert!(cluster_id_matches(&id.to_string(), id));
        assert2::assert!(cluster_id_matches(
            &URL_SAFE_NO_PAD.encode(id.as_bytes()),
            id
        ));
        assert2::assert!(!cluster_id_matches("different", id));
    }

    #[test]
    fn listener_validation_rejects_duplicates_and_unknown_protocol() {
        let valid = Listener {
            name: "PLAINTEXT".into(),
            host: "broker".into(),
            port: 9092,
            security_protocol: 0,
            ..Default::default()
        };
        assert2::assert!(decode_listeners(std::slice::from_ref(&valid)).is_ok());
        assert2::assert!(decode_listeners(&[valid.clone(), valid.clone()]).is_err());
        assert2::assert!(
            decode_listeners(&[Listener {
                security_protocol: 99,
                ..valid
            }])
            .is_err()
        );
    }

    #[test]
    fn finalized_features_must_fit_request_ranges() {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: "metadata.version".into(),
                level: 25,
            },
        ));
        let mut req = BrokerRegistrationRequest {
            features: vec![Feature {
                name: "metadata.version".into(),
                min_supported_version: 7,
                max_supported_version: 25,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert2::assert!(features_support_finalized(&req, &image));
        req.features[0].max_supported_version = 24;
        assert2::assert!(!features_support_finalized(&req, &image));
    }
}

/// KIP-966 on the wire: an external broker's registration carries the
/// clean-shutdown proof as `previousBrokerEpoch`, and the controller withdraws
/// its ELR membership when the proof does not hold.
///
/// This is the path a JVM broker takes.
/// [`crate::broker::registration`] holds the same rule for krabka's own
/// self-registration, which is a different route to the same records.
#[cfg(test)]
mod wire_tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_metadata::{
        BrokerEndpoint, BrokerRegistrationRecord, LeaderEpoch, NodeId, PartitionRecord,
        TopicConfigRecord, TopicRecord,
    };
    use krabka_protocol::owned::broker_registration_request::Feature;
    use krabka_security::{AuthMethod, ListenerProtocol, Principal};

    use super::*;
    use crate::{
        config_keys::{ELIGIBLE_LEADER_REPLICAS, MIN_INSYNC_REPLICAS},
        elr::{TopicElr, state::PartitionElr},
        test_support::{
            decode_response, encode_request, request_context, start_broker_with_authorizer,
        },
    };

    const TOPIC: &str = "orders";
    const REGISTERED: NodeId = NodeId(2);
    /// `BrokerRegistration` v3 is where `previousBrokerEpoch` enters the
    /// schema, and `QuorumController` passes
    /// `cleanShutdownDetectionEnabled = requestApiVersion >= 3`.
    const V3: i16 = 3;
    const V2: i16 = 2;

    /// What the restarting broker offers as its clean-shutdown proof.
    #[derive(Debug, Clone, Copy)]
    enum Offer {
        /// The epoch the cluster still holds for it -- what a graceful stop
        /// leaves behind.
        HeldEpoch,
        /// The `-1` a `BrokerRegistrationRequest` defaults to, which is all a
        /// crashed broker has.
        Unproven,
    }

    fn nodes(ids: &[u64]) -> Vec<NodeId> {
        ids.iter().copied().map(NodeId).collect()
    }

    /// Node 2 registered, and one partition of `TOPIC` holding `isr` under
    /// `min_isr`, published with `elr`.
    ///
    /// All three are fixture knobs because the halves of the restart rule read
    /// different columns: the ELR withdrawal fires on a partition node 2 has
    /// already left, and the ISR removal only on one it is still in.
    fn seed_records(isr: &[u64], min_isr: &str, elr: Option<&str>) -> Vec<MetadataRecord> {
        let mut overrides: std::collections::BTreeMap<String, String> =
            [(MIN_INSYNC_REPLICAS.to_string(), min_isr.to_string())]
                .into_iter()
                .collect();
        if let Some(elr) = elr {
            overrides.insert(ELIGIBLE_LEADER_REPLICAS.to_string(), elr.to_string());
        }
        vec![
            // KIP-966 ELR maintenance is gated on the feature, whose release
            // default is 0, so the seed finalizes it the way an operator's
            // `kafka-features upgrade` would.
            MetadataRecord::V1FeatureLevel(krabka_metadata::FeatureLevelRecord {
                name: crate::features::ELR_VERSION.into(),
                level: 1,
            }),
            MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                node_id: REGISTERED,
                // The controller stamps the real epoch on submit; this is the
                // placeholder every self-registration sends.
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::from_u128(0xdead),
                host: "broker-2".into(),
                port: 9092,
                rack: None,
                endpoints: vec![BrokerEndpoint {
                    name: "PLAINTEXT".into(),
                    host: "broker-2".into(),
                    port: 9092,
                    protocol: ListenerProtocol::Plaintext,
                }],
                log_dirs: vec![uuid::Uuid::from_u128(11)],
                features: krabka_metadata::supported_feature_ranges(),
            }),
            MetadataRecord::V1Topic(TopicRecord {
                name: TOPIC.into(),
                topic_id: uuid::Uuid::from_u128(9),
                partitions: 1,
                replication_factor: 3,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: TOPIC.into(),
                partition: 0,
                leader: NodeId(1),
                replicas: nodes(&[1, 2, 3]),
                isr: nodes(isr),
                leader_epoch: LeaderEpoch(7),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![uuid::Uuid::nil(); 3],
                partition_epoch: 4,
            }),
            MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: TOPIC.into(),
                overrides,
            }),
        ]
    }

    /// The partition of `TOPIC` as it stands after a restart: the two columns
    /// the restart rule can move.
    #[derive(Debug, PartialEq, Eq)]
    struct Restarted {
        isr: Vec<NodeId>,
        elr: PartitionElr,
    }

    /// [`restart`] against the seed the ELR tests use: node 2 already out of
    /// the ISR, and published as eligible.
    async fn reregister(version: i16, offer: Offer) -> PartitionElr {
        restart(version, offer, seed_records(&[1], "2", Some("0:2,3:")))
            .await
            .elr
    }

    /// Re-register node 2 from a fresh process -- a new incarnation id, the
    /// way a JVM broker generates one per boot -- against `seed`, offering
    /// `previous_broker_epoch` as its proof, and return what the partition
    /// looks like afterwards.
    async fn restart(version: i16, offer: Offer, seed: Vec<MetadataRecord>) -> Restarted {
        let (broker_handle, _dir) =
            start_broker_with_authorizer(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while broker.controller.watch_leader().borrow().as_ref() != Some(&broker.config.node_id) {
            assert!(
                std::time::Instant::now() <= deadline,
                "broker did not become controller leader"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        broker.controller.submit_change(seed).await.expect("seed");

        let image = broker.controller.current_image();
        let previous_broker_epoch = match offer {
            Offer::HeldEpoch => image
                .broker_epoch(REGISTERED)
                .expect("node 2 is registered"),
            Offer::Unproven => crate::clean_shutdown::UNPROVEN,
        };
        let request = BrokerRegistrationRequest {
            broker_id: 2,
            cluster_id: image.cluster_id().to_string(),
            incarnation_id: krabka_protocol::primitives::uuid::Uuid(
                uuid::Uuid::from_u128(0xbeef).into_bytes(),
            ),
            listeners: vec![Listener {
                name: "PLAINTEXT".into(),
                host: "broker-2".into(),
                port: 9092,
                security_protocol: 0,
                ..Default::default()
            }],
            features: image
                .finalized_features()
                .iter()
                .map(|(name, level)| Feature {
                    name: name.clone(),
                    min_supported_version: 0,
                    max_supported_version: *level,
                    ..Default::default()
                })
                .collect(),
            log_dirs: vec![krabka_protocol::primitives::uuid::Uuid(
                uuid::Uuid::from_u128(11).into_bytes(),
            )],
            previous_broker_epoch,
            ..Default::default()
        };
        let principal = Principal {
            name: "broker".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer = "127.0.0.1:9092".parse().expect("peer address");
        let ctx = request_context(&principal, &peer, "broker-client");
        let bytes = super::handle(
            &broker,
            version,
            1,
            &encode_request(&request, version),
            &ctx,
        )
        .await
        .expect("BrokerRegistration");
        let response: BrokerRegistrationResponse = decode_response(&bytes, version);
        assert!(
            response.error_code == 0,
            "registration was refused: {response:?}"
        );

        let image = broker.controller.current_image();
        let restarted = Restarted {
            isr: image
                .partition(TOPIC, 0)
                .expect("seeded partition")
                .isr
                .clone(),
            elr: TopicElr::of_topic(&image, TOPIC).partition(0),
        };
        drop(image);
        drop(broker);
        broker_handle.shutdown().await;
        restarted
    }

    /// A broker offering the epoch the cluster still holds for it restarted
    /// cleanly and keeps its membership.
    #[tokio::test]
    async fn a_proven_clean_restart_keeps_its_elr_membership() {
        assert!(
            reregister(V3, Offer::HeldEpoch).await
                == PartitionElr {
                    eligible_leader_replicas: vec![2, 3],
                    last_known_elr: vec![],
                }
        );
    }

    /// A broker offering nothing -- the `-1` a `BrokerRegistrationRequest`
    /// defaults to, which is what a crashed broker has to offer -- loses it.
    #[tokio::test]
    async fn an_unproven_restart_loses_its_elr_membership() {
        assert!(
            reregister(V3, Offer::Unproven).await
                == PartitionElr {
                    eligible_leader_replicas: vec![3],
                    last_known_elr: vec![2],
                }
        );
    }

    /// A request older than v3 has no `previousBrokerEpoch` field to carry a
    /// proof, so the controller cannot detect a clean shutdown and assumes
    /// unclean -- Kafka's `cleanShutdownDetectionEnabled = requestApiVersion
    /// >= 3`. The epoch on the struct is ignored because it never reaches the
    /// wire.
    #[tokio::test]
    async fn a_pre_v3_registration_cannot_prove_anything() {
        assert!(
            reregister(V2, Offer::HeldEpoch).await
                == PartitionElr {
                    eligible_leader_replicas: vec![3],
                    last_known_elr: vec![2],
                }
        );
    }

    /// The seed the ISR half needs: node 2 in a healthy ISR that sits exactly
    /// at `min.insync.replicas`, and nothing published about it.
    ///
    /// This is the case an ELR withdrawal on its own cannot see. There is no
    /// membership to withdraw, and yet the next eligibility is derived from
    /// `old_isr`, which still names node 2 -- so whether node 2 stays in that
    /// ISR is the whole of the difference between the two branches.
    fn healthy_isr_seed() -> Vec<MetadataRecord> {
        seed_records(&[1, 2, 3], "3", None)
    }

    /// A restart that cannot prove itself clean loses its ISR seat as well as
    /// its eligibility, and the batch that takes the seat away does not hand
    /// eligibility back on the way out.
    ///
    /// Dropping node 2 leaves the ISR under `min.insync.replicas`, so the
    /// recompute that rides the same batch has an ELR to publish and
    /// `old_isr ∪ eligible_before` still names node 2. Kafka's
    /// `uncleanShutdownReplicas` is what keeps it out of the eligible column
    /// and lands it in the last-known one instead.
    #[tokio::test]
    async fn an_unproven_restart_loses_its_isr_seat_without_regaining_eligibility() {
        assert!(
            restart(V3, Offer::Unproven, healthy_isr_seed()).await
                == Restarted {
                    isr: nodes(&[1, 3]),
                    elr: PartitionElr {
                        eligible_leader_replicas: vec![],
                        last_known_elr: vec![2],
                    },
                }
        );
    }

    /// A restart that proves itself clean keeps the seat, from the same seed.
    ///
    /// Kafka's `handleBrokerShutdown` reaches the two-call unclean path only
    /// under `isElrFeatureEnabled() && !isCleanShutdown`, so the proof is what
    /// decides, and a broker that stopped gracefully still holds the log its
    /// ISR membership claims. Nothing moves: same ISR, still no ELR.
    #[tokio::test]
    async fn a_proven_clean_restart_keeps_its_isr_seat() {
        assert!(
            restart(V3, Offer::HeldEpoch, healthy_isr_seed()).await
                == Restarted {
                    isr: nodes(&[1, 2, 3]),
                    elr: PartitionElr::default(),
                }
        );
    }
}
