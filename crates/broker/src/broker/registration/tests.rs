use assert2::assert;
use tempfile::tempdir;

use super::*;

#[test]
fn file_config_self_registration_uses_advertised_listener_for_legacy_endpoint() {
    let file: crate::file_config::FileConfig = toml::from_str(
        r#"
inter_broker_listener_name = "INTERNAL"

[[listeners]]
name = "EXTERNAL"
bind_addr = "127.0.0.1:19094"
advertised = "external.example:29094"
protocol = "Plaintext"

[[listeners]]
name = "INTERNAL"
bind_addr = "127.0.0.1:19093"
advertised = "internal.example:29093"
protocol = "Plaintext"
"#,
    )
    .expect("parse file config");
    let mut config = BrokerConfig::default();
    assert!(
        config.listen_addr.port() == 9092,
        "preserve CLI default precondition"
    );
    file.apply_to(&mut config).expect("apply file config");

    let registration = self_registration_record(&config);

    assert!(registration.host == "internal.example");
    assert!(registration.port == 29093);
    assert!(
        registration
            .endpoints
            .iter()
            .map(|endpoint| (
                endpoint.name.as_str(),
                endpoint.host.as_str(),
                endpoint.port
            ))
            .collect::<Vec<_>>()
            == vec![
                ("EXTERNAL", "external.example", 29094),
                ("INTERNAL", "internal.example", 29093),
            ]
    );
}

/// A stretch profile with two data sites and one witness site, with
/// leadership pinned to `dc-a`.
fn three_site_profile() -> crate::config::StretchProfile {
    crate::config::StretchProfile {
        sites: vec!["dc-a".to_string(), "dc-b".to_string(), "dc-w".to_string()],
        witness_site: "dc-w".to_string(),
        preferred_leader_site: "dc-a".to_string(),
    }
}

/// A node of node id 4 that carries `roles` and logs into `log_dir`.
///
/// The registration record mints a directory id under `log_dir`, so the
/// caller passes a temporary directory rather than the source tree.
fn node_with_roles(log_dir: &std::path::Path, roles: Vec<crate::config::NodeRole>) -> BrokerConfig {
    BrokerConfig {
        node_id: krabka_metadata::NodeId(4),
        roles,
        ..BrokerConfig::for_tests(log_dir.to_path_buf())
    }
}

fn broker_witness_record(node_id: u64, value: Option<&str>) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1BrokerConfig(krabka_metadata::BrokerConfigRecord {
        node_id: krabka_metadata::NodeId(node_id),
        config_name: "broker.witness".into(),
        config_value: value.map(str::to_string),
    })
}

#[test]
fn a_witness_registration_batch_publishes_the_witness_role() {
    let log_dir = tempdir().expect("temp log dir");
    let config = node_with_roles(
        log_dir.path(),
        vec![
            crate::config::NodeRole::Controller,
            crate::config::NodeRole::Broker,
            crate::config::NodeRole::Witness,
        ],
    );

    assert!(
        broker_registration_batch(&config)
            == vec![
                krabka_metadata::MetadataRecord::V1BrokerRegistration(self_registration_record(
                    &config
                )),
                broker_witness_record(4, Some("true")),
            ]
    );
}

#[test]
fn a_plain_broker_registration_batch_clears_the_witness_role() {
    let log_dir = tempdir().expect("temp log dir");
    let config = node_with_roles(
        log_dir.path(),
        vec![
            crate::config::NodeRole::Controller,
            crate::config::NodeRole::Broker,
        ],
    );

    assert!(
        broker_registration_batch(&config)
            == vec![
                krabka_metadata::MetadataRecord::V1BrokerRegistration(self_registration_record(
                    &config
                )),
                broker_witness_record(4, None),
            ]
    );
}

#[test]
fn a_stretch_profile_publishes_the_preferred_leader_site_as_a_cluster_default() {
    let config = BrokerConfig {
        stretch: Some(three_site_profile()),
        ..BrokerConfig::for_tests(std::path::PathBuf::new())
    };

    assert!(
        stretch_default_records(&config)
            == vec![krabka_metadata::MetadataRecord::V1BrokerConfig(
                krabka_metadata::BrokerConfigRecord {
                    node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                    config_name: "stretch.preferred.leader.site".into(),
                    config_value: Some("dc-a".into()),
                }
            )]
    );
}

#[test]
fn a_node_without_a_stretch_profile_publishes_no_cluster_default() {
    let config = BrokerConfig::for_tests(std::path::PathBuf::new());

    assert!(stretch_default_records(&config) == vec![]);
}

#[test]
fn checkpoint_loaded_still_submits_the_stretch_cluster_default() {
    let config = BrokerConfig {
        stretch: Some(three_site_profile()),
        ..BrokerConfig::for_tests(std::path::PathBuf::new())
    };
    let duplicate =
        krabka_metadata::MetadataRecord::V1FeatureLevel(krabka_metadata::FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 25,
        });

    assert!(
        bootstrap_records_to_submit(&config, vec![duplicate], true)
            == stretch_default_records(&config)
    );
}

#[test]
fn self_controller_registration_uses_quorum_endpoint_and_feature_ranges() {
    let config = BrokerConfig {
        node_id: krabka_metadata::NodeId(7),
        incarnation_id: uuid::Uuid::from_u128(0xCAFE),
        controller_quorum_voters: vec![(
            krabka_metadata::NodeId(7),
            "controller.example:19093".into(),
        )],
        controller_listener_protocol: krabka_security::ListenerProtocol::Ssl,
        ..Default::default()
    };

    let registration = self_controller_registration_record(&config);

    assert!(registration.node_id == krabka_metadata::NodeId(7));
    assert!(registration.incarnation_id == uuid::Uuid::from_u128(0xCAFE));
    assert!(registration.features == krabka_metadata::supported_feature_ranges());
    assert!(
        registration.endpoints
            == vec![krabka_metadata::BrokerEndpoint {
                name: "CONTROLLER".into(),
                host: "controller.example".into(),
                port: 19093,
                protocol: krabka_security::ListenerProtocol::Ssl,
            }]
    );
}

#[test]
fn controller_registration_starts_at_kip_919_floor_and_is_idempotent() {
    let config = BrokerConfig::default();
    let registration = self_controller_registration_record(&config);
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&krabka_metadata::MetadataRecord::V1FeatureLevel(
        krabka_metadata::FeatureLevelRecord {
            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: krabka_metadata::metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL - 1,
        },
    ));

    assert!(controller_registration_update(&image, &registration).is_none());

    image.apply(&krabka_metadata::MetadataRecord::V1FeatureLevel(
        krabka_metadata::FeatureLevelRecord {
            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: krabka_metadata::metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL,
        },
    ));
    let update = controller_registration_update(&image, &registration)
        .expect("crossing the KIP-919 floor registers the controller");
    image.apply(&update);

    assert!(image.controller(config.node_id) == Some(&registration));
    assert!(controller_registration_update(&image, &registration).is_none());
}

/// KIP-966, the crash half: a broker that restarts without proving it stopped
/// gracefully gives up its eligible-leader-replica membership.
///
/// The pair below drives the real self-registration path against a live
/// controller and reads the published ELR back out of the committed metadata.
/// Both restarts reuse the node's incarnation id -- krabka keeps that id in the
/// log dir, so a broker that crashes and comes back on the same disk really
/// does present the same one -- which is the whole point: identity says
/// nothing about whether the log this node brings back is the log its ELR
/// membership claims, and only the clean-shutdown proof does.
mod unclean_restart {
    use std::{sync::Arc, time::Duration};

    use assert2::assert;
    use krabka_metadata::{
        LeaderEpoch, MetadataRecord, NodeId, PartitionRecord, TopicConfigRecord, TopicRecord,
    };
    use krabka_protocol::owned::{
        alter_partition_request::{
            AlterPartitionRequest, PartitionData as ReqPartitionData, TopicData as ReqTopicData,
        },
        alter_partition_response::AlterPartitionResponse,
    };
    use krabka_security::{AuthMethod, Principal};

    use crate::{
        broker::{Broker, registration::register_broker},
        codes,
        config::BrokerConfig,
        config_keys::MIN_INSYNC_REPLICAS,
        elr::{TopicElr, state::PartitionElr},
        test_support::{
            decode_response, encode_request, request_context, start_broker_with_authorizer,
        },
    };

    const TOPIC: &str = "orders";
    const TOPIC_ID_BYTES: [u8; 16] = [9; 16];
    const LEADER_EPOCH: i32 = 7;
    /// `AlterPartition` v2, whose `new_isr` is a plain broker-id list, for the
    /// reason [`crate::elr::tests`] gives: v3 drags the KIP-903 broker-epoch
    /// eligibility check into a fixture that is not about it.
    const ALTER_VERSION: i16 = 2;
    /// The restarting broker. Node 1 is the controller and stays up.
    const RESTARTING: NodeId = NodeId(2);

    fn nodes(ids: &[u64]) -> Vec<NodeId> {
        ids.iter().copied().map(NodeId).collect()
    }

    /// One RF=3 partition with a full ISR, and a `min.insync.replicas` of 2,
    /// which is what gives the partition an ELR to fall below.
    fn seed_records() -> Vec<MetadataRecord> {
        vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: TOPIC.into(),
                topic_id: uuid::Uuid::from_bytes(TOPIC_ID_BYTES),
                partitions: 1,
                replication_factor: 3,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: TOPIC.into(),
                partition: 0,
                leader: NodeId(1),
                replicas: nodes(&[1, 2, 3]),
                isr: nodes(&[1, 2, 3]),
                leader_epoch: LeaderEpoch(LEADER_EPOCH),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![uuid::Uuid::nil(); 3],
                partition_epoch: 4,
            }),
            MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: TOPIC.into(),
                overrides: [(MIN_INSYNC_REPLICAS.to_string(), "2".to_string())]
                    .into_iter()
                    .collect(),
            }),
        ]
    }

    async fn wait_for_leader(broker: &Broker) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if broker
                .controller
                .watch_leader()
                .borrow()
                .is_some_and(|node| node == broker.config.node_id)
            {
                return;
            }
            assert!(
                std::time::Instant::now() <= deadline,
                "broker did not become controller leader"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Shrink the ISR to `new_isr` through the real `AlterPartition` handler,
    /// which is how a real partition's ELR comes to exist at all.
    async fn alter_isr(broker: &Arc<Broker>, new_isr: &[i32]) {
        let principal = Principal {
            name: "replica".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer = "127.0.0.1:9092".parse().expect("peer address");
        let ctx = request_context(&principal, &peer, "broker-client");
        let request = AlterPartitionRequest {
            broker_id: 1,
            broker_epoch: -1,
            topics: vec![ReqTopicData {
                topic_id: krabka_protocol::primitives::uuid::Uuid(TOPIC_ID_BYTES),
                partitions: vec![ReqPartitionData {
                    partition_index: 0,
                    leader_epoch: LEADER_EPOCH,
                    new_isr: new_isr.to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let bytes = crate::handlers::alter_partition::handle(
            broker,
            ALTER_VERSION,
            1,
            &encode_request(&request, ALTER_VERSION),
            &ctx,
        )
        .await
        .expect("AlterPartition");
        let response: AlterPartitionResponse = decode_response(&bytes, ALTER_VERSION);
        assert!(
            response.topics[0].partitions[0].error_code == codes::NONE,
            "AlterPartition refused the proposal: {response:?}"
        );
    }

    /// The published ELR of partition 0.
    fn published_elr(broker: &Arc<Broker>) -> PartitionElr {
        TopicElr::of_topic(&broker.controller.current_image(), TOPIC).partition(0)
    }

    /// Bring the restarting broker up: read whatever clean-shutdown proof its
    /// log dir holds, then register through the real self-registration path.
    ///
    /// This is what `start_metadata_phase` does, in the order it does it.
    async fn boot(broker: &Arc<Broker>, config: &mut BrokerConfig) {
        config.previous_broker_epoch = crate::clean_shutdown::take(&config.log_dir);
        register_broker(config, &*broker.controller)
            .await
            .expect("self-registration");
    }

    /// Stand the fixture up: node 1 leading an RF=3 partition, node 2 booted
    /// once and then dropped out of the ISR, so it holds ELR membership.
    ///
    /// Returns node 2's config, its log dir, and the broker epoch the cluster
    /// now holds for it -- the epoch a graceful stop would leave behind.
    async fn cluster_with_node_two_eligible(
        broker: &Arc<Broker>,
    ) -> (BrokerConfig, tempfile::TempDir, i64) {
        wait_for_leader(broker).await;
        broker
            .controller
            .submit_change(seed_records())
            .await
            .expect("seed orders");

        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.node_id = RESTARTING;
        boot(broker, &mut config).await;

        alter_isr(broker, &[1]).await;
        assert!(
            published_elr(broker)
                == PartitionElr {
                    eligible_leader_replicas: vec![2, 3],
                    last_known_elr: vec![],
                },
            "precondition: the shrink leaves nodes 2 and 3 eligible"
        );

        let epoch = broker
            .controller
            .current_image()
            .broker_epoch(RESTARTING)
            .expect("node 2 is registered");
        (config, dir, epoch)
    }

    /// The defect this closes: a broker that crashed -- no clean-shutdown
    /// proof left behind -- comes back on the same log dir with the same
    /// incarnation id, and must no longer be eligible to be elected. Its log
    /// may be shorter than the records its ELR membership asserts it holds.
    #[tokio::test]
    async fn a_crashed_broker_loses_its_elr_membership_on_restart() {
        let (handle, _dir) =
            start_broker_with_authorizer(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = handle.broker_arc_for_test();
        let (mut config, _victim_dir, _epoch) = cluster_with_node_two_eligible(&broker).await;

        // A crash writes nothing, so the restart can prove nothing.
        boot(&broker, &mut config).await;

        assert!(
            published_elr(&broker)
                == PartitionElr {
                    eligible_leader_replicas: vec![3],
                    last_known_elr: vec![2],
                }
        );

        handle.shutdown().await;
    }

    /// The other end of the round trip: a graceful stop leaves behind exactly
    /// the epoch the cluster holds, which is what its own next start spends.
    /// The two tests above hand that value over by writing it themselves; this
    /// one says the broker really does write it on the way down.
    #[tokio::test]
    async fn a_graceful_stop_leaves_the_proof_its_own_restart_spends() {
        let (handle, dir) =
            start_broker_with_authorizer(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let node_id = broker.config.node_id;
        let log_dir = broker.config.log_dir.clone();
        let epoch = broker
            .controller
            .current_image()
            .broker_epoch(node_id)
            .expect("the broker registered itself at startup");
        drop(broker);

        handle.shutdown().await;

        assert!(crate::clean_shutdown::take(&log_dir) == epoch);
        drop(dir);
    }

    /// The companion, without which the fix could pass by withdrawing
    /// everyone: a broker that stopped gracefully offers back the epoch the
    /// cluster still holds for it, and keeps its membership.
    #[tokio::test]
    async fn a_gracefully_stopped_broker_keeps_its_elr_membership_on_restart() {
        let (handle, _dir) =
            start_broker_with_authorizer(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = handle.broker_arc_for_test();
        let (mut config, victim_dir, epoch) = cluster_with_node_two_eligible(&broker).await;

        // The graceful stop leaves its proof; the restart spends it.
        crate::clean_shutdown::write(victim_dir.path(), epoch);
        boot(&broker, &mut config).await;

        assert!(
            published_elr(&broker)
                == PartitionElr {
                    eligible_leader_replicas: vec![2, 3],
                    last_known_elr: vec![],
                }
        );

        handle.shutdown().await;
    }
}
