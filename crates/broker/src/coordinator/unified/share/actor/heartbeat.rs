//! The `ShareGroupHeartbeat` request path: first join, the steady-state
//! subscription and liveness update, and leave. It is the largest single
//! concern of the share-group actor, so it lives in its own file.

#[cfg(test)]
use crate::coordinator::unified::streams::config::StreamsGroupConfig;
use {
    super::{
        assignment::reconcile,
        records::{PendingShareRecords, ShareRecorder, chrono_now_ms, flush_pending},
        response::{build_assignment_resp, error_resp},
        share_state::{cleanup_deleted_topics, prepare_initialize, start_initialize},
    },
    crate::{
        codes,
        coordinator::unified::{
            ClientIdentity, GroupCoordinator,
            actor::MetadataProvider,
            member_helpers::update_present,
            offsets_log::OffsetsLog,
            share::{
                config::ShareGroupConfig,
                state::{ShareGroupState, ShareMemberState},
            },
        },
        error::BrokerError,
        metadata_epoch::next_i32,
    },
    krabka_protocol::owned::{
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    },
    std::{collections::HashSet, time::Instant},
};

pub(super) async fn handle_heartbeat(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    req: &ShareGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
) -> Result<ShareGroupHeartbeatResponse, BrokerError> {
    let now = Instant::now();
    let now_ms = chrono_now_ms();

    // ─── Leave path ──────────────────────────────────────────────
    if req.member_epoch == -1 {
        return handle_leave(
            state,
            config,
            metadata,
            offsets_log,
            coordinator,
            req,
            now_ms,
        )
        .await;
    }

    // ─── Member lookup ───────────────────────────────────────────
    // KIP-932 mirrors KIP-848: the client mints its own member UUID and
    // sends it with `member_epoch == 0`. Epoch 0 from an unknown member is a
    // first join under the client's id, which the handler has checked is set
    // (`KafkaApis.isMemberIdValid`). Epoch 0 from a known member is a rejoin.
    // Kafka's `getOrMaybeCreateMember` creates a new member with its defaults,
    // and the heartbeat then updates it like any other.
    let joining = req.member_epoch == 0 && !state.members.contains_key(&req.member_id);
    if joining {
        if state.members.len() >= config.max_size {
            return Ok(error_resp(codes::GROUP_MAX_SIZE_REACHED, config));
        }
    } else if let Err(error_code) = state.validate_member_epoch(&req.member_id, req.member_epoch) {
        return Ok(error_resp(error_code, config));
    }
    let recorder = ShareRecorder::start(state, &[&req.member_id]);
    let assigned_before = state
        .members
        .get(&req.member_id)
        .map(|m| m.assigned_partitions.clone());
    if joining {
        state.add_or_update_member(ShareMemberState::joining(
            &req.member_id,
            client.id,
            client.host,
            HashSet::new(),
        ));
    }
    let subscription_changed = update_member(state, req, client, now);
    let Ok(target) = reconcile(
        state,
        metadata,
        config.assignment_interval,
        subscription_changed,
    ) else {
        if joining {
            state.remove_member(&req.member_id);
        }
        return Ok(error_resp(codes::INVALID_REQUEST, config));
    };
    state.reconcile_member(&req.member_id, subscription_changed, metadata);
    let mut pending = recorder.finish(state, target.as_deref());
    // Kafka's `maybeCreateInitializeShareGroupStateRequest` writes the
    // partitions it initializes last in the heartbeat's batch.
    let initialize =
        prepare_initialize(state, config, coordinator, now_ms).map(|(value, initialize)| {
            pending.state_partition_metadata = Some(value);
            initialize
        });
    flush_pending(state, pending, offsets_log, coordinator, now_ms).await?;
    if let Some(initialize) = initialize {
        start_initialize(state, coordinator, initialize);
    }
    cleanup_deleted_topics(state, offsets_log, coordinator, now_ms).await;
    // Kafka sends the assignment only on a full request (a rejoin at epoch 0
    // or a request that carries the subscription) or when it changed.
    let assigned_changed = state
        .members
        .get(&req.member_id)
        .map(|m| &m.assigned_partitions)
        != assigned_before.as_ref();
    let with_assignment =
        req.member_epoch == 0 || req.subscribed_topic_names.is_some() || assigned_changed;
    Ok(build_assignment_resp(
        state,
        &req.member_id,
        config,
        with_assignment,
    ))
}

/// Kafka's `ShareGroupMember.Builder` updates of `shareGroupHeartbeat`, and
/// whether the heartbeat changed the member's subscribed topic names
/// (`hasMemberSubscriptionChanged`).
fn update_member(
    state: &mut ShareGroupState,
    req: &ShareGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
) -> bool {
    let Some(m) = state.members.get_mut(&req.member_id) else {
        return false;
    };
    m.last_seen = now;
    client.update_metadata(&mut m.client_id, &mut m.client_host);
    // Kafka's `ShareGroupMember.Builder.maybeUpdateRackId`: a heartbeat that
    // carries a rack id replaces the stored one, a rejoin included.
    update_present(&mut m.rack_id, req.rack_id.as_ref());
    if let Some(ref names) = req.subscribed_topic_names {
        let set: HashSet<String> = names.iter().cloned().collect();
        if set != m.subscribed_topic_names {
            m.subscribed_topic_names = set;
            return true;
        }
    }
    false
}

/// Handle a leave-group heartbeat (`member_epoch == -1`).
///
/// It follows Kafka's `GroupMetadataManager.shareGroupLeave`. An unknown
/// member answers `UNKNOWN_MEMBER_ID` with Kafka's message and writes no
/// record. A known member is fenced (`shareGroupFenceMember`).
async fn handle_leave(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    req: &ShareGroupHeartbeatRequest,
    now_ms: i64,
) -> Result<ShareGroupHeartbeatResponse, BrokerError> {
    if !state.members.contains_key(&req.member_id) {
        return Ok(ShareGroupHeartbeatResponse {
            error_code: codes::UNKNOWN_MEMBER_ID,
            error_message: Some(format!(
                "Member {} is not a member of group {}.",
                req.member_id, state.group_id
            )),
            ..Default::default()
        });
    }
    let Some(pending) = fence_member(state, metadata, &req.member_id) else {
        return Ok(error_resp(codes::INVALID_REQUEST, config));
    };
    flush_pending(state, pending, offsets_log, coordinator, now_ms).await?;
    Ok(leave_resp(&req.member_id, req.member_epoch))
}

/// Kafka's `shareGroupFenceMember`: the member's current assignment, target
/// assignment and subscription tombstones, and the group epoch bumped with
/// the metadata hash of the subscriptions that remain. The target waits for
/// the next heartbeat. `None` when the epoch is exhausted; the group is then
/// unchanged.
pub(super) fn fence_member(
    state: &mut ShareGroupState,
    metadata: &dyn MetadataProvider,
    member_id: &str,
) -> Option<PendingShareRecords> {
    next_i32(state.group_epoch)?;
    let recorder = ShareRecorder::start(state, &[member_id]);
    state.remove_member(member_id);
    state.bump_epoch();
    state.metadata_hash = super::assignment::metadata_hash(state, &metadata.snapshot());
    Some(recorder.finish(state, None))
}

/// The response to a successful leave: Kafka's `shareGroupLeave` sets only
/// the member id and the request's member epoch.
fn leave_resp(member_id: &str, member_epoch: i32) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse {
        member_id: Some(member_id.to_owned()),
        member_epoch,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::{
            config::NextGenConfig,
            offsets_log::fake::InMemoryOffsetsLog,
            share::actor::test_support::{
                TopicMetadataSetup, heartbeat, make_coordinator, metadata_with_topic,
                seed_initialized, subscribed_group, subscribed_request,
            },
            test_support::{
                assert_single_member_limit, assignment_interval_fixture, heartbeat_write_failures,
            },
        },
        assert2::{assert, check},
        std::{sync::Arc, time::Duration},
    };

    /// Kafka's `shareGroupHeartbeat`: only initialized partitions are
    /// assigned, the assignor keeps what it can when a member joins, and the
    /// response carries the assignment only for a join, a request with a
    /// subscription, or a changed assignment. Each row is one heartbeat and
    /// the whole response it gets.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn member_limit_rejects_only_new_members() {
        let (metadata, _id) = metadata_with_topic().call();
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(
            GroupCoordinator::new(
                NextGenConfig::assigning_at_once(),
                ShareGroupConfig {
                    max_size: 1,
                    ..ShareGroupConfig::assigning_at_once()
                },
                metadata,
                log,
                StreamsGroupConfig::default(),
            )
            .expect("valid test assignor configuration"),
        );
        let handle = coord.get_or_create_share("g");
        assert_single_member_limit(
            &handle,
            |member_id, epoch| {
                subscribed_request()
                    .member_id(member_id)
                    .epoch(epoch)
                    .call()
            },
            heartbeat,
        )
        .await;
    }

    /// Kafka's `shareGroupLeave`: a known member is fenced, with its records
    /// tombstoned and the group epoch bumped, and the response echoes its id
    /// and epoch `-1`. An unknown member answers `UNKNOWN_MEMBER_ID` and
    /// leaves the group and the log untouched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_epoch_is_fenced() {
        let (_coord, _log, handle) = subscribed_group()
            .name(TopicMetadataSetup::four_partitions().name)
            .partitions(TopicMetadataSetup::four_partitions().partitions)
            .call();
        let joined = heartbeat(&handle, subscribed_request().call()).await;
        assert!(joined.member_epoch == 2);
        // Re-send with an epoch ahead of the server → fenced.
        let resp = heartbeat(&handle, subscribed_request().epoch(99).call()).await;
        assert!(resp.error_code == codes::FENCED_MEMBER_EPOCH);
    }

    /// The member epoch rule of Kafka's `throwIfShareGroupMemberEpochIsInvalid`.
    /// Member `m1` is at epoch 4 with previous epoch 2: it joins at epoch 2,
    /// `m2` and `m3` join (group epochs 3 and 4), and `m1` heartbeats once at
    /// epoch 2. Each row sends one heartbeat on a fresh group and compares the
    /// whole response.
    /// A heartbeat that carries a rack id replaces the stored one, a rejoin at
    /// epoch 0 included, and the member metadata record carries the new rack.
    /// A heartbeat without a rack id keeps it (`maybeUpdateRackId`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heartbeat_updates_the_rack_id() {
        let (metadata, _id) = metadata_with_topic().call();
        let (coord, _log) = make_coordinator(metadata);
        let handle = coord.get_or_create_share("g");
        let request = |member_epoch, rack_id: Option<&str>| ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch,
            rack_id: rack_id.map(str::to_owned),
            subscribed_topic_names: Some(vec!["t".into()]),
            ..Default::default()
        };
        // (request epoch, request rack id, expected stored rack id)
        let rows = [
            (0, Some("rack-a"), Some("rack-a")),
            (0, Some("rack-b"), Some("rack-b")),
            (2, None, Some("rack-b")),
            (2, Some("rack-c"), Some("rack-c")),
        ];
        for (index, (member_epoch, rack_id, expected)) in rows.into_iter().enumerate() {
            let resp = heartbeat(&handle, request(member_epoch, rack_id)).await;
            check!(resp.error_code == codes::NONE, "row {index}");
            let seed = coord.cached_share_seed("g").expect("seed cached");
            check!(
                seed.members["m1"].rack_id.as_deref() == expected,
                "row {index}"
            );
        }
    }

    /// A heartbeat whose write fails answers the code of the failure, and the
    /// failed write leaves no partial batch. A write that is not committed
    /// answers what Kafka's `CoordinatorOperationExceptionHelper` answers for
    /// it, so a member that the coordinator never committed looks the
    /// coordinator up again rather than keep an epoch the next coordinator
    /// does not know.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_write_answers_its_code_and_writes_no_partial_batch() {
        for case in heartbeat_write_failures() {
            let fixture = case.coordinator(|| {
                let (metadata, _id) = metadata_with_topic().call();
                make_coordinator(metadata)
            });
            let handle = fixture.coordinator.get_or_create_share("g");

            let response = heartbeat(&handle, subscribed_request().call()).await;

            fixture
                .check_refused(&response, |error_code| ShareGroupHeartbeatResponse {
                    error_code,
                    ..Default::default()
                })
                .await;
        }
    }

    /// KIP-1263: a share group replays the `AssignmentTimestamp` of its target
    /// assignment metadata record, and Kafka's
    /// `canComputeNextTargetAssignment` runs the assignment interval from it.
    /// A stored time inside the interval holds the next assignment back, and
    /// an unknown time (0) or an elapsed interval lets it run, which writes
    /// the time it finished.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_replayed_assignment_timestamp_holds_the_interval() {
        use crate::coordinator::unified::{ShareGroupSeed, wall_clock_ms};

        let (rows, mut cases) = assignment_interval_fixture();

        for (case, ago, wanted) in rows {
            let (coordinator, _) = super::super::test_support::topic_coordinator()
                .name(TopicMetadataSetup::four_partitions().name)
                .partitions(TopicMetadataSetup::four_partitions().partitions)
                .share(ShareGroupConfig {
                    assignment_interval: Duration::from_mins(1),
                    ..ShareGroupConfig::default()
                })
                .call();
            let handle = coordinator.get_or_create_share("g");
            let stored = ago.map_or(0, |ago| wall_clock_ms() - ago);
            handle
                .tx
                .send(super::super::ShareGroupActorMessage::Seed(ShareGroupSeed {
                    group_epoch: 2,
                    target_epoch: 2,
                    assignment_timestamp_ms: stored,
                    ..ShareGroupSeed::default()
                }))
                .await
                .unwrap();
            let before = wall_clock_ms();
            let joined = heartbeat(&handle, subscribed_request().call()).await;
            let after = wall_clock_ms();
            let written = coordinator
                .cached_share_seed("g")
                .unwrap()
                .assignment_timestamp_ms;
            cases.record(case, joined.member_epoch, (before, after), written, wanted);
        }
        cases.check();
    }
    use {
        crate::coordinator::unified::share::actor::ShareGroupActorHandle,
        krabka_protocol::{
            owned::{
                common::share_group_heartbeat_response::topic_partitions::TopicPartitions,
                share_group_heartbeat_response::Assignment,
            },
            primitives::uuid::Uuid,
            records::RecordBatch,
        },
        rstest_bdd_macros::{given, then, when},
    };
    #[derive(Default)]
    struct ShareHeartbeatBehavior {
        coordinator: Option<Arc<GroupCoordinator>>,
        log: Option<Arc<InMemoryOffsetsLog>>,
        handle: Option<Arc<ShareGroupActorHandle>>,
        topic: Option<Uuid>,
        response: Option<ShareGroupHeartbeatResponse>,
        setup_epochs: Vec<i32>,
        setup_errors: Vec<i16>,
        advanced: Option<ShareGroupHeartbeatResponse>,
        before_batches: usize,
        batches: Vec<RecordBatch>,
        group_epoch: Option<i32>,
    }
    #[rstest::fixture]
    fn share_heartbeat() -> ShareHeartbeatBehavior {
        // Start this scenario with fresh inputs and observations.
        ShareHeartbeatBehavior::default()
    }
    #[given(
        "Share group g subscribes to four-partition topic t with initialized partitions {initialized}"
    )]
    async fn share_group_initialization(
        share_heartbeat: &mut ShareHeartbeatBehavior,
        initialized: String,
    ) {
        let (metadata, id) = metadata_with_topic()
            .name(TopicMetadataSetup::four_partitions().name)
            .partitions(TopicMetadataSetup::four_partitions().partitions)
            .call();
        let (coordinator, log) = make_coordinator(metadata);
        let handle = coordinator.get_or_create_share("g");
        if initialized == "all" {
            seed_initialized()
                .handle(&handle)
                .topic_id(id)
                .partitions((0..4).map(krabka_ids::PartitionIndex).collect())
                .call()
                .await;
        } else if initialized != "none" {
            panic!("unknown initialized partition set");
        }
        share_heartbeat.topic = Some(id);
        share_heartbeat.coordinator = Some(coordinator);
        share_heartbeat.log = Some(log);
        share_heartbeat.handle = Some(handle);
    }
    #[given("m1 has joined Share group g at epoch 2")]
    async fn joined_share_member(share_heartbeat: &mut ShareHeartbeatBehavior) {
        let response = heartbeat(
            share_heartbeat
                .handle
                .as_ref()
                .expect("Share group prepared"),
            subscribed_request().call(),
        )
        .await;
        share_heartbeat.setup_epochs.push(response.member_epoch);
        share_heartbeat.setup_errors.push(response.error_code);
        share_heartbeat.before_batches = share_heartbeat
            .log
            .as_ref()
            .expect("group log")
            .batches()
            .await
            .len();
    }
    #[given(
        "m1 joined at epoch 2 then m2 and m3 joined and m1 advanced from previous epoch 2 to current epoch 4"
    )]
    async fn advanced_share_member(share_heartbeat: &mut ShareHeartbeatBehavior) {
        let handle = share_heartbeat
            .handle
            .as_ref()
            .expect("Share group prepared");
        for request in [
            subscribed_request().call(),
            subscribed_request().member_id("m2").call(),
            subscribed_request().member_id("m3").call(),
            subscribed_request().epoch(2).call(),
        ] {
            let response = heartbeat(handle, request).await;
            share_heartbeat.setup_epochs.push(response.member_epoch);
            share_heartbeat.setup_errors.push(response.error_code);
            share_heartbeat.advanced = Some(response);
        }
    }
    #[when(
        "Share member {member_id} heartbeats at epoch {epoch} including topic subscription {subscribe}"
    )]
    async fn share_member_heartbeat(
        share_heartbeat: &mut ShareHeartbeatBehavior,
        member_id: String,
        epoch: i32,
        subscribe: bool,
    ) {
        let request = ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id,
            member_epoch: epoch,
            subscribed_topic_names: subscribe.then(|| vec!["t".into()]),
            ..Default::default()
        };
        share_heartbeat.response = Some(
            heartbeat(
                share_heartbeat
                    .handle
                    .as_ref()
                    .expect("Share group prepared"),
                request,
            )
            .await,
        );
    }
    #[then(
        "the complete Share response names member {member_id} at epoch {epoch} with assignment {partitions}"
    )]
    fn share_assignment_response(
        share_heartbeat: &ShareHeartbeatBehavior,
        member_id: String,
        epoch: i32,
        partitions: String,
    ) {
        let assignment = if partitions == "omitted" {
            None
        } else {
            let partitions = serde_json::from_str::<Vec<i32>>(&partitions)
                .expect("expected assigned partitions");
            Some(Assignment {
                topic_partitions: if partitions.is_empty() {
                    vec![]
                } else {
                    vec![TopicPartitions {
                        topic_id: share_heartbeat.topic.expect("topic identifier"),
                        partitions,
                        ..Default::default()
                    }]
                },
                ..Default::default()
            })
        };
        let expected = ShareGroupHeartbeatResponse {
            member_id: Some(member_id),
            member_epoch: epoch,
            heartbeat_interval_ms: 5000,
            assignment,
            ..Default::default()
        };
        check!(share_heartbeat.response.as_ref() == Some(&expected));
    }
    #[when("Share member {member_id} leaves with epoch minus one")]
    async fn leave_share_member(share_heartbeat: &mut ShareHeartbeatBehavior, member_id: String) {
        let request = ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id,
            member_epoch: -1,
            ..Default::default()
        };
        share_heartbeat.response = Some(
            heartbeat(
                share_heartbeat
                    .handle
                    .as_ref()
                    .expect("Share group prepared"),
                request,
            )
            .await,
        );
        share_heartbeat.batches = share_heartbeat
            .log
            .as_ref()
            .expect("group log")
            .batches()
            .await;
        share_heartbeat.group_epoch = Some(
            share_heartbeat
                .coordinator
                .as_ref()
                .expect("coordinator")
                .cached_share_seed("g")
                .expect("cached Share group")
                .group_epoch,
        );
    }
    #[then(
        "the Share leave result for {member_id} appends {batches} batches and leaves group epoch {epoch}"
    )]
    fn share_leave_result(
        share_heartbeat: &ShareHeartbeatBehavior,
        member_id: String,
        batches: usize,
        epoch: i32,
    ) {
        check!(share_heartbeat.setup_epochs == vec![2]);
        check!(share_heartbeat.setup_errors == vec![codes::NONE]);
        let expected = if member_id == "m1" {
            ShareGroupHeartbeatResponse {
                member_id: Some("m1".into()),
                member_epoch: -1,
                ..Default::default()
            }
        } else {
            ShareGroupHeartbeatResponse {
                error_code: codes::UNKNOWN_MEMBER_ID,
                error_message: Some("Member m9 is not a member of group g.".into()),
                ..Default::default()
            }
        };
        check!(share_heartbeat.response.as_ref() == Some(&expected));
        check!(share_heartbeat.batches.len() == share_heartbeat.before_batches + batches);
        check!(share_heartbeat.group_epoch == Some(epoch));
        if batches > 0 {
            check!(
                share_heartbeat
                    .batches
                    .last()
                    .expect("leave batch")
                    .records
                    .iter()
                    .any(|record| record.value.is_none())
            );
        }
    }
    #[then("the epoch validation returns the complete Share {outcome} response")]
    fn share_epoch_result(share_heartbeat: &ShareHeartbeatBehavior, outcome: String) {
        check!(share_heartbeat.setup_epochs == vec![2, 3, 4, 4]);
        check!(share_heartbeat.setup_errors == vec![0, 0, 0, 0]);
        let config = ShareGroupConfig::assigning_at_once();
        let expected = match outcome.as_str() {
            "accepted" => ShareGroupHeartbeatResponse {
                member_id: Some("m1".into()),
                member_epoch: 4,
                assignment: share_heartbeat
                    .advanced
                    .as_ref()
                    .expect("member advanced")
                    .assignment
                    .clone(),
                ..super::super::response::base_resp(codes::NONE, 4, &config)
            },
            "fenced" => super::super::response::error_resp(codes::FENCED_MEMBER_EPOCH, &config),
            "unknown-member" => {
                super::super::response::error_resp(codes::UNKNOWN_MEMBER_ID, &config)
            }
            _ => panic!("unknown epoch outcome"),
        };
        check!(share_heartbeat.response.as_ref() == Some(&expected));
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/coordinator/unified/share/actor/heartbeat.feature",
        name = "Initialized Share partitions rebalance on joins and omit unchanged assignments"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heartbeat_assignments_match_kafka(share_heartbeat: ShareHeartbeatBehavior) {
        let _ = share_heartbeat;
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/coordinator/unified/share/actor/heartbeat.feature",
        name = "Uninitialized Share partitions remain unassigned"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heartbeat_assignments_wait_for_initialization(
        share_heartbeat: ShareHeartbeatBehavior,
    ) {
        let _ = share_heartbeat;
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/coordinator/unified/share/actor/heartbeat.feature",
        name = "Known and unknown Share members leave with Kafka durability rules"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leave_matches_kafka(
        share_heartbeat: ShareHeartbeatBehavior,
        member: String,
        batches: usize,
        epoch: i32,
    ) {
        let _ = share_heartbeat;
        let _ = (member, batches, epoch);
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/coordinator/unified/share/actor/heartbeat.feature",
        name = "Share member epoch fencing accepts only rejoin previous or current member epochs"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn member_epoch_rule_matches_kafka(
        share_heartbeat: ShareHeartbeatBehavior,
        member: String,
        epoch: i32,
        outcome: String,
    ) {
        let _ = share_heartbeat;
        let _ = (member, epoch, outcome);
    }
}
