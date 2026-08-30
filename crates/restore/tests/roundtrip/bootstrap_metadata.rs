//! The metadata a restore writes beside the partition data: the cluster id in
//! `meta.properties.json`, and the topics and partitions the archive held in
//! `bootstrap.records.bin`.
//!
//! Reading those records back needs `wincode`, which this crate cannot name,
//! so the checks here are the count of records the seeding added and the raw
//! bytes of the archived topic ids -- a long enough argument to keep the
//! metadata claim in a file of its own.

use std::path::Path;

use assert2::{assert, check};
use krabka_broker::{Broker, BrokerConfig};
use krabka_ids::LeaderEpoch;
use krabka_metadata::{
    AclEntry, AclOperation, MetadataImage, MetadataRecord, NodeId, PartitionRecord, PatternType,
    PermissionType, ResourceType, TopicConfigRecord, TopicRecord,
};
use krabka_protocol::owned::{
    describe_acls_request::DescribeAclsRequest,
    describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
};
use krabka_restore::restore;
use uuid::Uuid;

use crate::{
    args::restore_args,
    fixture::{Fixture, build_fixture},
};

/// 3. Read back the restored metadata: `meta.properties.json` names the
/// cluster id `format_target` chose, and `bootstrap.records.bin` carries the
/// archive's topics and partitions -- with the SAME topic id the archive
/// used, not a freshly generated one.
#[tokio::test]
async fn restored_bootstrap_metadata_carries_the_archived_topic_ids_and_partition_counts() {
    let fixture = build_fixture();
    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");
    let cluster_id = Uuid::new_v4();
    let args = restore_args(
        fixture.archive_root.path(),
        &log_dir,
        &["--cluster-id", &cluster_id.to_string()],
    );

    let report = restore(&args).await.expect("restore");
    check!(report.cluster_id == cluster_id);

    let meta: serde_json::Value = serde_json::from_slice(
        &std::fs::read(log_dir.join("meta.properties.json")).expect("meta.properties.json"),
    )
    .expect("meta.properties.json is JSON");
    check!(meta["cluster_id"] == serde_json::json!(cluster_id.to_string()));

    // `bootstrap.records.bin` is a length-prefixed stream of
    // `serde_wincode::SerdeCompat<MetadataRecord>` payloads (see
    // `crates/format/src/format.rs` and
    // `crates/format/tests/seeded_records.rs`), and `wincode`/`serde-wincode`
    // are dependencies of `krabka-format` alone, not of `krabka-restore` --
    // this crate (and so this test, which may only touch this one file) has
    // no `use` path to either crate. Record survival is checked two ways
    // instead: the record *count* the restore's seeded topics and
    // partitions must have added, against a baseline format with the same
    // target flags and no seeding; and the archived topic id's raw bytes,
    // which a `Uuid` serializes to verbatim (as a 16-byte tuple, with no
    // framing of its own, per the `uuid` crate's `Serialize` impl for a
    // non-self-describing format) in any binary serde format, wincode
    // included.
    let baseline = tempfile::tempdir().expect("baseline tempdir");
    let baseline_dir = baseline.path().join("formatted");
    let baseline_code = krabka_format::run_from_args([
        "krabka-format".to_owned(),
        "--log-dir".to_owned(),
        baseline_dir.display().to_string(),
        "--cluster-id".to_owned(),
        Uuid::new_v4().to_string(),
        "--node-id".to_owned(),
        "1".to_owned(),
        "--standalone".to_owned(),
        "--controller-listener".to_owned(),
        "127.0.0.1:9093".to_owned(),
    ])
    .await;
    check!(baseline_code == 0);

    let record_count = |dir: &Path| -> u64 {
        let manifest: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.join("bootstrap.json")).expect("bootstrap.json"),
        )
        .expect("bootstrap.json is JSON");
        manifest["record_count"]
            .as_u64()
            .expect("record_count is a JSON number")
    };
    let extra_records =
        u64::try_from(Fixture::topic_count() + fixture.partitions().len()).expect("small count");
    check!(record_count(&log_dir) == record_count(&baseline_dir) + extra_records);

    let bin = std::fs::read(log_dir.join("bootstrap.records.bin")).expect("bootstrap.records.bin");
    for topic_id in [fixture.orders_id, fixture.payments_id] {
        let topic_id_bytes: [u8; 16] = topic_id.into_bytes();
        check!(
            bin.windows(16).any(|window| window == topic_id_bytes),
            "topic id {topic_id} not found in bootstrap.records.bin"
        );
    }
    for topic in ["orders", "payments"] {
        check!(
            bin.windows(topic.len())
                .any(|window| window == topic.as_bytes()),
            "topic name {topic:?} not found in bootstrap.records.bin"
        );
    }
}

/// Restored metadata must reach the broker's public admin APIs, not merely
/// survive as bytes in the bootstrap file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restored_snapshot_reaches_describe_configs_and_describe_acls() {
    const ORDERS: &str = "orders";
    let fixture = build_fixture();
    let mut image = MetadataImage::new(Uuid::new_v4());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: ORDERS.to_owned(),
        topic_id: fixture.orders_id,
        partitions: 2,
        replication_factor: 1,
    }));
    for partition in 0..2 {
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: ORDERS.to_owned(),
            partition,
            leader: NodeId(1),
            replicas: vec![NodeId(1)],
            isr: vec![NodeId(1)],
            leader_epoch: LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
    }
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: ORDERS.to_owned(),
        overrides: maplit::btreemap! {"cleanup.policy".to_owned() => "compact".to_owned()},
    }));
    image.apply(&MetadataRecord::V1AccessControlEntry(AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: ORDERS.to_owned(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".to_owned(),
        host: "*".to_owned(),
        operation: AclOperation::Read,
        permission_type: PermissionType::Allow,
    }));

    let snapshot_dir = tempfile::tempdir().expect("snapshot tempdir");
    let snapshot_path = snapshot_dir
        .path()
        .join("00000000000000000042-0000000001.checkpoint");
    std::fs::write(
        &snapshot_path,
        krabka_raft::serialize_metadata_snapshot(&image, 1_700_000_000_000)
            .expect("serialize metadata snapshot"),
    )
    .expect("write metadata snapshot");

    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");
    let args = restore_args(
        fixture.archive_root.path(),
        &log_dir,
        &["--metadata-snapshot", &snapshot_path.display().to_string()],
    );
    let report = restore(&args).await.expect("restore");
    check!(report.metadata.topic_configs == 1);
    check!(report.metadata.access_control_entries == 1);

    let broker = Broker::start(BrokerConfig::for_tests(log_dir))
        .await
        .expect("restored broker starts");
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("restore-metadata-test")
        .build()
        .await
        .expect("client");

    let configs = client
        .send(DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: 2,
                resource_name: ORDERS.to_owned(),
                configuration_keys: None,
                ..Default::default()
            }],
            include_synonyms: false,
            include_documentation: false,
            ..Default::default()
        })
        .await
        .expect("DescribeConfigs");
    let result = configs.results.first().expect("one config result");
    assert!(result.error_code == 0, "DescribeConfigs failed: {result:?}");
    check!(result.configs.iter().any(|config| {
        config.name == "cleanup.policy" && config.value.as_deref() == Some("compact")
    }));

    let acls = client
        .send(DescribeAclsRequest {
            resource_type_filter: 2,
            resource_name_filter: Some(ORDERS.to_owned()),
            pattern_type_filter: 3,
            principal_filter: None,
            host_filter: None,
            operation: 1,
            permission_type: 1,
            ..Default::default()
        })
        .await
        .expect("DescribeAcls");
    assert!(acls.error_code == 0, "DescribeAcls failed: {acls:?}");
    check!(acls.resources.iter().any(|resource| {
        resource.resource_name == ORDERS
            && resource
                .acls
                .iter()
                .any(|acl| acl.principal == "User:alice")
    }));

    broker.shutdown().await;
}
