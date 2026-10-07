//! Shared unit-test fixtures for the coordinator modules: coordinator
//! builders, a metadata provider and a metadata source with fixed contents, a
//! share persister, and the record values the replay tests feed in.
//!
//! Several sibling modules assert against the same fixtures, so they live in
//! one module rather than being rebuilt per test file.

use std::{collections::BTreeMap, sync::Arc};

use super::{
    actor::MetadataProvider,
    config::NextGenConfig,
    group_coordinator::GroupCoordinator,
    persistence_next_gen, reconciler,
    share::{self, config::ShareGroupConfig},
    streams::{self, config::StreamsGroupConfig},
};
use crate::test_support::string_pairs;

/// An active-only stable streams seed, with no standby, warmup or revocations.
pub(crate) fn stable_streams_assignment(
    epochs: (i32, i32),
    active: BTreeMap<String, Vec<i32>>,
) -> streams::persistence::StreamsGroupCurrentMemberAssignmentValue {
    streams::persistence::StreamsGroupCurrentMemberAssignmentValue {
        member_epoch: epochs.0,
        previous_member_epoch: epochs.1,
        state: streams::persistence::StreamsMemberWireState::Stable,
        active,
        standby: BTreeMap::new(),
        warmup: BTreeMap::new(),
        active_pending_revocation: BTreeMap::new(),
        standby_pending_revocation: BTreeMap::new(),
        warmup_pending_revocation: BTreeMap::new(),
        active_epochs: BTreeMap::new(),
        active_pending_revocation_epochs: BTreeMap::new(),
    }
}

pub(crate) fn make_coord() -> Arc<GroupCoordinator> {
    make_coord_with_log().0
}

pub(crate) fn make_coord_with_log() -> (
    Arc<GroupCoordinator>,
    Arc<crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog>,
) {
    use crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog;
    let metadata: Arc<dyn MetadataProvider> = Arc::new(ImageMetadatalessProvider);
    let offsets_log = Arc::new(InMemoryOffsetsLog::default());
    let coord = Arc::new(GroupCoordinator::new(
        NextGenConfig::assigning_at_once(),
        ShareGroupConfig::assigning_at_once(),
        metadata,
        offsets_log.clone(),
        StreamsGroupConfig::default(),
    ));
    (coord, offsets_log)
}

#[derive(Debug)]
pub(super) struct ImageMetadatalessProvider;
impl MetadataProvider for ImageMetadatalessProvider {
    fn snapshot(&self) -> reconciler::ReconcileInput {
        reconciler::ReconcileInput::default()
    }
}

/// A metadata provider whose snapshot a test replaces, as the controller
/// replaces the metadata image.
#[derive(Debug, Default)]
pub(crate) struct SwitchableMetadata(std::sync::Mutex<reconciler::ReconcileInput>);

impl SwitchableMetadata {
    pub(crate) fn new(input: reconciler::ReconcileInput) -> Arc<Self> {
        Arc::new(Self(std::sync::Mutex::new(input)))
    }

    pub(crate) fn set(&self, input: reconciler::ReconcileInput) {
        *self.0.lock().expect("metadata lock") = input;
    }
}

impl MetadataProvider for SwitchableMetadata {
    fn snapshot(&self) -> reconciler::ReconcileInput {
        self.0.lock().expect("metadata lock").clone()
    }
}

/// A snapshot of `topics`, each a name, the byte its topic id repeats and a
/// partition count.
pub(crate) fn snapshot_of(topics: &[(&str, u8, i32)]) -> reconciler::ReconcileInput {
    reconciler::ReconcileInput {
        topic_id_by_name: topics
            .iter()
            .map(|(name, id, _)| ((*name).to_string(), proto_uuid(*id)))
            .collect(),
        partitions_per_topic: topics
            .iter()
            .map(|(_, id, partitions)| (proto_uuid(*id), *partitions))
            .collect(),
        ..reconciler::ReconcileInput::default()
    }
}

/// A coordinator whose group actors read `metadata`.
pub(crate) fn make_coord_with_metadata(
    metadata: Arc<dyn MetadataProvider>,
) -> Arc<GroupCoordinator> {
    Arc::new(GroupCoordinator::new(
        NextGenConfig::assigning_at_once(),
        ShareGroupConfig::assigning_at_once(),
        metadata,
        Arc::new(crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog::default()),
        StreamsGroupConfig::default(),
    ))
}

/// A metadata source over `image`, with node 1 reported as the controller
/// leader.
pub(super) fn fixed_source(
    image: krabka_metadata::MetadataImage,
) -> Arc<dyn crate::metadata_source::MetadataSource> {
    Arc::new(
        crate::test_support::FakeMetadataSource::builder()
            .image(image)
            .leader(Some(krabka_raft::NodeId(1)))
            .build(),
    )
}

pub(super) fn make_share_persister(
    source: Arc<dyn crate::metadata_source::MetadataSource>,
) -> Arc<crate::share_coordinator::persister_client::SharePersister> {
    let share_coordinator = Arc::new(
        crate::share_coordinator::coordinator::ShareCoordinator::new(
            krabka_metadata::NodeId(1),
            Arc::new(crate::partition_registry::PartitionRegistry::new()),
            crate::share_coordinator::config::ShareCoordinatorConfig::default(),
        ),
    );
    Arc::new(
        crate::share_coordinator::persister_client::SharePersister::new(
            krabka_metadata::NodeId(1),
            share_coordinator,
            source,
            Arc::default(),
            Arc::new(crate::network::client::InterBrokerClient::new(None, None)),
            krabka_security::ListenerProtocol::Plaintext,
            "PLAINTEXT".into(),
        ),
    )
}

pub(super) fn proto_uuid(byte: u8) -> krabka_protocol::primitives::uuid::Uuid {
    krabka_protocol::primitives::uuid::Uuid([byte; 16])
}

pub(super) fn real_uuid(byte: u8) -> uuid::Uuid {
    uuid::Uuid::from_bytes([byte; 16])
}

pub(super) fn next_member(client_id: &str) -> persistence_next_gen::MemberMetadataValue {
    persistence_next_gen::MemberMetadataValue {
        instance_id: Some(format!("{client_id}-instance")),
        rack_id: Some("rack-a".into()),
        client_id: client_id.into(),
        client_host: "host".into(),
        subscribed_topic_names: vec!["topic-a".into()],
        subscribed_topic_regex: Some("topic-.*".into()),
        server_assignor: Some("range".into()),
        rebalance_timeout_ms: 45_000,
        classic: None,
    }
}

pub(super) fn next_current(epoch: i32) -> persistence_next_gen::CurrentMemberAssignmentValue {
    persistence_next_gen::CurrentMemberAssignmentValue {
        member_epoch: epoch,
        previous_member_epoch: epoch - 1,
        state: persistence_next_gen::MemberAssignmentState::Stable,
        assigned_partitions: vec![persistence_next_gen::CurrentTopicPartitions {
            topic_id: proto_uuid(1),
            partitions: vec![0, 1],
            assignment_epochs: None,
        }],
        partitions_pending_revocation: vec![],
    }
}

pub(super) fn share_member(client_id: &str) -> share::persistence::ShareGroupMemberMetadataValue {
    share::persistence::ShareGroupMemberMetadataValue {
        rack_id: Some("rack-b".into()),
        client_id: client_id.into(),
        client_host: "host".into(),
        subscribed_topic_names: vec!["share-topic".into()],
    }
}

pub(super) fn streams_member(
    client_id: &str,
) -> streams::persistence::StreamsGroupMemberMetadataValue {
    streams::persistence::StreamsGroupMemberMetadataValue {
        instance_id: Some(format!("{client_id}-instance")),
        rack_id: Some("rack-c".into()),
        client_id: client_id.into(),
        client_host: "host".into(),
        process_id: "process".into(),
        user_endpoint: Some(streams::persistence::StreamsEndpoint {
            host: "localhost".into(),
            port: 8080,
        }),
        client_tags: vec![("app".into(), "streams".into())],
        rebalance_timeout_ms: 30_000,
        topology_epoch: 4,
    }
}

/// A metadata image with the requested per-group config records.
pub(crate) fn group_config_image(
    overrides: &[(&str, &[(&str, &str)])],
) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    for (group_id, entries) in overrides {
        image.apply(&krabka_metadata::MetadataRecord::V1GroupConfig(
            krabka_metadata::GroupConfigRecord {
                group_id: (*group_id).to_owned(),
                configs: string_pairs(entries),
            },
        ));
    }
    image
}

/// The fields every heartbeat capacity/timing contract observes.
pub(crate) trait HeartbeatStatus {
    fn status(self) -> (i16, i32);
}

macro_rules! heartbeat_status {
    ($($response:ty),+ $(,)?) => {$(
        impl HeartbeatStatus for $response {
            fn status(self) -> (i16, i32) {
                (self.error_code, self.member_epoch)
            }
        }
    )+};
}
heartbeat_status!(
    krabka_protocol::owned::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    krabka_protocol::owned::share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    krabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
);

/// Exercise one-member capacity through each protocol's heartbeat RPC.
pub(crate) async fn assert_single_member_limit<H, Q, R: HeartbeatStatus>(
    handle: &Arc<H>,
    request: impl Fn(&str, i32) -> Q,
    heartbeat: impl AsyncFn(&H, Q) -> R,
) {
    let status = async |id: &'static str, epoch| {
        let request = request(id, epoch);
        heartbeat(&Arc::clone(handle), request).await.status()
    };

    let (code, epoch) = status("m1", 0).await;
    assert2::check!(code == crate::codes::NONE);
    let (code, _) = status("m2", 0).await;
    assert2::check!(code == crate::codes::GROUP_MAX_SIZE_REACHED);
    let (code, existing_epoch) = status("m1", epoch).await;
    assert2::check!(code == crate::codes::NONE);
    assert2::check!(existing_epoch == epoch);
}

/// Run the shared session-timeout contract after protocol-specific setup.
pub(crate) async fn check_group_session_timeout<H, R: HeartbeatStatus>(
    create: impl Fn(&str) -> Arc<H>,
    heartbeat: impl AsyncFn(&H, &str, i32) -> R,
) {
    use assert2::{assert, check};
    let mut epochs = Vec::new();
    for group_id in ["brief", "plain"] {
        let handle = create(group_id);
        let (code, epoch) = heartbeat(&handle, "m1", 0).await.status();
        check!(code == crate::codes::NONE, "{group_id}");
        epochs.push((group_id, handle, epoch));
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let mut answers = Vec::new();
    for (group_id, handle, epoch) in &epochs {
        answers.push((*group_id, heartbeat(handle, "m1", *epoch).await.status().0));
    }
    assert!(
        answers
            == [
                ("brief", crate::codes::UNKNOWN_MEMBER_ID),
                ("plain", crate::codes::NONE)
            ]
    );
}

/// Join two members in each configured group, then heartbeat after the paced
/// interval. Each protocol checks the first paced response with its own oracle.
pub(crate) async fn check_group_assignment_timing<H, R: HeartbeatStatus>(
    create: impl AsyncFn(&str) -> Arc<H>,
    heartbeat: impl AsyncFn(&H, &str, i32) -> R,
    check_first: impl Fn((i16, i32)),
) {
    use assert2::{assert, check};
    let mut joined = Vec::new();
    let mut handles = Vec::new();
    for group_id in ["slow", "fast", "paced"] {
        let handle = create(group_id).await;
        check!(
            heartbeat(&handle, "m1", 0).await.status().1 == 2,
            "{group_id}"
        );
        joined.push((group_id, heartbeat(&handle, "m2", 0).await.status().1));
        handles.push((group_id, handle));
    }
    assert!(joined == [("slow", 2), ("fast", 3), ("paced", 2)]);
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let (_, paced) = &handles[2];
    check_first(heartbeat(paced, "m1", 2).await.status());
    check!(heartbeat(paced, "m2", 2).await.status().1 == 3);
    // The slow group still waits for the broker's minute.
    let (_, slow) = &handles[0];
    check!(heartbeat(slow, "m2", 2).await.status().1 == 2);
}

/// Split a freshly encoded key into its leading version and body, mirroring
/// the broker's `__consumer_offsets` dispatch.
pub(crate) fn peek_version(mut buf: &[u8]) -> (i16, &[u8]) {
    let version = bytes::Buf::get_i16(&mut buf);
    (version, buf)
}

/// Literal wire fields, decoded independently of the record codecs. Each field
/// must contain complete hexadecimal byte pairs; malformed literals fail the test.
pub(crate) fn wire_bytes(fields: &[&str]) -> Vec<u8> {
    fields
        .iter()
        .flat_map(|field| hex::decode(field).expect("valid literal wire bytes"))
        .collect()
}

/// The three independent failure expectations for a heartbeat's durable write.
pub(crate) fn heartbeat_write_failures()
-> [(&'static str, Option<crate::error::BrokerError>, i16); 3] {
    let uncommitted =
        |code| Some(crate::error::BrokerError::CoordinatorWriteUncommitted { partition: 0, code });
    [
        (
            "the partition writer is gone",
            None,
            crate::codes::COORDINATOR_LOAD_IN_PROGRESS,
        ),
        (
            "the leadership moved before the write committed",
            uncommitted(crate::codes::NOT_COORDINATOR),
            crate::codes::NOT_COORDINATOR,
        ),
        (
            "the write did not commit in time",
            uncommitted(crate::codes::COORDINATOR_NOT_AVAILABLE),
            crate::codes::COORDINATOR_NOT_AVAILABLE,
        ),
    ]
}

/// A successful leave appends exactly one batch containing a tombstone.
pub(crate) async fn assert_next_tombstone_batch(
    log: &crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog,
    pre_leave: usize,
) {
    let batches = log.batches().await;
    assert2::assert!(batches.len() == pre_leave + 1);
    let leave_batch = &batches[batches.len() - 1];
    assert2::assert!(
        leave_batch.records.iter().any(|r| r.value.is_none()),
        "leave batch must contain at least one tombstone"
    );
}
