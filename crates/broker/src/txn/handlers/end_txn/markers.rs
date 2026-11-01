//! Phase 2 of `EndTxn`: the `WriteTxnMarkers` fan-out. This module groups the
//! transaction's partitions by their current leader, appends the marker batch
//! directly to every partition this broker leads, and hands each remote leader
//! to the RPC in [`super::marker_rpc`]. A marker counts as written only once
//! it is committed on its partition, local or remote.

use {
    super::marker_rpc::send_write_txn_markers,
    crate::{
        broker::Broker,
        coordinator::GroupCoordinator,
        error::BrokerError,
        network::client::InterBrokerClient,
        partition::Partition,
        partition_registry::PartitionRegistry,
        txn::{
            handlers::write_txn_markers::{
                MARKER_COMMIT_TIMEOUT, MarkerAppend, append_marker_as_leader,
            },
            marker::{MarkerFailureClass, MarkerType, classify_marker_failure},
            state::{TopicPartition, TxnEntry},
        },
    },
    futures_util::future::join_all,
    krabka_metadata::{MetadataImage, NodeId},
    krabka_security::ListenerProtocol,
    std::{
        collections::HashMap,
        sync::{Arc, atomic::Ordering},
        time::Instant,
    },
};

/// The outcome of one attempt to fan out `WriteTxnMarkers`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerFanOutOutcome {
    /// Every partition's marker is durable.
    Complete,
    /// At least one partition failed with a retriable code, and no fenced
    /// generation cancelled the fan-out. The `Prepare*` record is durable, so
    /// the caller hands the transaction to the completion task, which retries
    /// the fan-out.
    Retry,
    /// A fenced producer or coordinator generation cancelled the fan-out, or
    /// every partition that failed answered a code Kafka's completion handler
    /// treats as an illegal state (#882). Retrying cannot succeed, so the
    /// caller must not queue it for completion.
    GivenUp,
}

/// What one marker fan-out attempt achieved.
#[derive(Debug, Default)]
pub struct MarkerFanOut {
    /// The partitions whose marker is committed, those whose leader answered
    /// `UNSUPPORTED_FOR_MESSAGE_FORMAT` or `UNSUPPORTED_VERSION`, and those
    /// that no longer exist and so need none. Kafka's
    /// `TransactionMarkerRequestCompletionHandler` removes each of them from
    /// the transaction's partition set.
    pub(crate) written: Vec<TopicPartition>,
    /// The failure that decides what the caller does next, if any partition
    /// failed, as `record_deciding_failure` picks it.
    pub(crate) failure: Option<BrokerError>,
}

impl MarkerFanOut {
    /// Records `error`, keeping the failure that decides the next step.
    pub(crate) fn fail(&mut self, error: BrokerError) {
        record_deciding_failure(&mut self.failure, error);
    }

    /// Folds another group's outcome into this one.
    pub(crate) fn merge(&mut self, other: Self) {
        self.written.extend(other.written);
        if let Some(error) = other.failure {
            self.fail(error);
        }
    }
}

/// Write the markers for a prepared transaction.
pub(super) async fn dispatch_transaction_markers(
    broker: &Broker,
    snapshot: &mut TxnEntry,
    marker_type: MarkerType,
    transactional_id: &str,
) -> MarkerFanOutOutcome {
    let result = broker
        .txn_coordinator
        .dispatch_transaction_markers(snapshot, marker_type)
        .await;
    let class = result.as_ref().err().map(classify_marker_failure);
    let outcome = fan_out_outcome(class);
    if let Err(error) = &result {
        if outcome == MarkerFanOutOutcome::GivenUp {
            tracing::warn!(
                tid = transactional_id,
                producer_id = snapshot.producer_id.get(),
                producer_epoch = snapshot.producer_epoch,
                outstanding = ?snapshot.partitions,
                error = %error,
                class = ?class,
                "EndTxn: WriteTxnMarkers fan-out cannot succeed; giving up"
            );
        } else {
            tracing::warn!(
                tid = transactional_id,
                producer_id = snapshot.producer_id.get(),
                producer_epoch = snapshot.producer_epoch,
                outstanding = ?snapshot.partitions,
                error = %error,
                "EndTxn: WriteTxnMarkers fan-out failed; queued for completion"
            );
        }
    }
    outcome
}

/// What `EndTxn` does after its fan-out: `failure` is the class of its worst
/// failed marker, `None` when every marker is written.
pub fn fan_out_outcome(failure: Option<MarkerFailureClass>) -> MarkerFanOutOutcome {
    match failure {
        None => MarkerFanOutOutcome::Complete,
        Some(class) if class.stops_fan_out() => MarkerFanOutOutcome::GivenUp,
        Some(_) => MarkerFanOutOutcome::Retry,
    }
}

/// Dispatch `WriteTxnMarkers` to every partition leader involved in the
/// transaction. The function groups partitions by leader node:
///
/// - **local** (leader == `node_id`): [`write_local_markers`] appends to the
///   in-memory handle and waits for each marker to commit.
/// - **remote**: sends a
///   [`WriteTxnMarkersRequest`](krabka_protocol::owned::write_txn_markers_request::WriteTxnMarkersRequest)
///   over the shared
///   [`InterBrokerClient`], which runs TLS / SASL when the inter-broker
///   listener demands them.
///
/// Any `__consumer_offsets` partitions registered through `AddOffsetsToTxn`
/// live in `entry.partitions`, because Kafka's model has no separate group
/// list. The same loop therefore fans them out with the data partitions.
///
/// The local leader and every remote leader get their markers at the same
/// time, as Kafka's `TransactionMarkerChannelManager` sends to every broker at
/// once. Each one waits for its markers to commit.
#[derive(Clone, Copy)]
pub struct MarkerDispatchContext<'a> {
    pub(crate) node_id: NodeId,
    pub(crate) coordinator_epoch: i32,
    pub(crate) image: &'a MetadataImage,
    pub(crate) inter_broker_client: &'a InterBrokerClient,
    pub(crate) inter_broker_protocol: ListenerProtocol,
    pub(crate) inter_broker_listener_name: &'a str,
    pub(crate) inter_broker_server_name: &'a str,
    pub(crate) group_coordinator: Option<&'a Arc<GroupCoordinator>>,
}

pub async fn dispatch_markers(
    context: MarkerDispatchContext<'_>,
    partitions: &Arc<PartitionRegistry>,
    entry: &TxnEntry,
    marker_type: MarkerType,
) -> MarkerFanOut {
    let MarkerDispatchContext {
        node_id,
        coordinator_epoch,
        image,
        ..
    } = context;
    // Group every involved (topic, partition) by its current leader.
    let mut by_leader: HashMap<NodeId, Vec<TopicPartition>> = HashMap::new();
    let mut outcome = MarkerFanOut::default();

    for tp in &entry.partitions {
        let leader = if let Some(partition) = image.partition(&tp.topic, tp.partition.get()) {
            Some(partition.leader)
        } else if let Some(partition) = partitions.get(&tp.topic, tp.partition) {
            if partition.current_leader.load(Ordering::Acquire) != node_id.0 {
                return MarkerFanOut {
                    written: Vec::new(),
                    failure: Some(BrokerError::Txn(format!(
                        "transaction marker target {}-{} is materialized locally but missing from metadata",
                        tp.topic,
                        tp.partition.get()
                    ))),
                };
            }
            Some(node_id)
        } else {
            // The partition was deleted after joining the transaction. There
            // is no log left to mark, so it must not block completion.
            None
        };
        match leader {
            Some(leader) => by_leader.entry(leader).or_default().push(tp.clone()),
            None => outcome.written.push(tp.clone()),
        }
    }

    let mut local = Vec::new();
    for tp in by_leader.remove(&node_id).unwrap_or_default() {
        match partitions.get(&tp.topic, tp.partition) {
            Some(part) => local.push((tp, part)),
            None => outcome.fail(BrokerError::Txn(format!(
                "transaction marker target {}-{} is led locally but is not materialized",
                tp.topic,
                tp.partition.get()
            ))),
        }
    }
    let marker = MarkerAppend {
        producer_id: entry.producer_id,
        producer_epoch: entry.producer_epoch,
        marker_type,
        coordinator_epoch,
        commit_stamp: None,
        transaction_version: entry.client_transaction_version,
    };
    // Every leader group is attempted, even after another group fails: a
    // partition whose marker already landed must not be abandoned because a
    // different partition in the same fan-out round needs a retry (#882).
    // `record_deciding_failure` picks the failure the caller sees: a fenced
    // generation still cancels the whole attempt, and any retriable failure
    // keeps it queued.
    let (local, remote) = tokio::join!(
        write_local_markers(node_id, context.group_coordinator, marker, local),
        join_all(by_leader.iter().map(|(leader, tps)| {
            send_write_txn_markers(context, *leader, entry, marker_type, tps)
        })),
    );
    outcome.merge(local);
    for remote in remote {
        outcome.merge(remote);
    }
    outcome
}

/// Write `marker` to each of the `local` partitions as their leader, and wait
/// until every marker commits, under one deadline.
///
/// Kafka's coordinator sends the markers of the partitions it leads to itself
/// through `WriteTxnMarkers`. That handler appends every marker first and
/// waits for all of them under one `DelayedProduce`, so one timeout bounds the
/// whole request. A partition joins `written` only when its marker commits.
pub async fn write_local_markers(
    node_id: NodeId,
    group_coordinator: Option<&Arc<GroupCoordinator>>,
    marker: MarkerAppend,
    local: Vec<(TopicPartition, Arc<Partition>)>,
) -> MarkerFanOut {
    let mut outcome = MarkerFanOut::default();
    let mut pending = Vec::with_capacity(local.len());
    for (tp, part) in local {
        match append_marker_as_leader(&part, node_id, group_coordinator, &tp.topic, marker).await {
            Ok(appended) => pending.push((tp, appended)),
            Err(error) => outcome.fail(error),
        }
    }
    let deadline = Instant::now() + MARKER_COMMIT_TIMEOUT;
    let committed = join_all(
        pending
            .into_iter()
            .map(|(tp, appended)| async move { (tp, appended.committed(deadline).await) }),
    )
    .await;
    for (tp, result) in committed {
        match result {
            Ok(()) => outcome.written.push(tp),
            Err(error) => outcome.fail(error),
        }
    }
    outcome
}

/// Keeps `deciding` at the failure of the attempt that decides what the
/// caller does next, of what it already holds and `error`. The first error is
/// kept when both are the same class.
///
/// - A fenced generation wins over every other failure. Kafka's
///   `TransactionMarkerRequestCompletionHandler` cancels every queued marker
///   of the transaction on the first fenced partition
///   (`removeMarkersForTxn`), so the attempt does not retry.
/// - A retriable failure wins over an unexpected code. Kafka handles each
///   broker's answer on its own: it throws on an unexpected code, which ends
///   the handling of that one answer, and it still re-enqueues the retriable
///   partitions of every other answer. An attempt that stopped on the
///   unexpected code instead would never write the marker of a partition
///   that was only briefly short of replicas, and a prepared commit, which
///   nothing else retries, would leave that partition's last stable offset
///   at the transaction's first record for good.
/// - An unexpected code is reported only when no partition failed in another
///   way. The attempt then stops, as Kafka's does.
fn record_deciding_failure(deciding: &mut Option<BrokerError>, error: BrokerError) {
    let replace = deciding.as_ref().is_none_or(|existing| {
        failure_precedence(classify_marker_failure(&error))
            > failure_precedence(classify_marker_failure(existing))
    });
    if replace {
        *deciding = Some(error);
    }
}

/// The order [`record_deciding_failure`] keeps failures in.
fn failure_precedence(class: MarkerFailureClass) -> u8 {
    match class {
        MarkerFailureClass::Unexpected => 0,
        MarkerFailureClass::Retriable => 1,
        MarkerFailureClass::Fenced => 2,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            codes::{
                INVALID_PRODUCER_EPOCH, NOT_ENOUGH_REPLICAS, NOT_LEADER_OR_FOLLOWER,
                TRANSACTION_COORDINATOR_FENCED, UNKNOWN_SERVER_ERROR,
            },
            test_support::{broker_registration, open_partition},
            txn::handlers::end_txn::test_support::{marker_entry, plaintext_client, tps},
        },
        assert2::assert,
        krabka_ids::PartitionIndex,
    };

    /// The failure that decides an attempt: a fenced generation cancels it,
    /// any retriable failure keeps the transaction queued, and an unexpected
    /// code stops it only when nothing else failed. The first failure of a
    /// class is the one reported.
    ///
    /// A prepared commit whose marker reaches one partition with an
    /// unexpected code, and another partition while it is short of replicas,
    /// must be retried: nothing else writes the second partition's marker,
    /// and its last stable offset would stay at the transaction's first
    /// record for good.
    #[test]
    fn a_retriable_partition_keeps_the_attempt_queued_past_an_unexpected_code() {
        let refused = |code| BrokerError::MarkerWriteRefused {
            code,
            message: format!("code {code}"),
        };
        let reported = |failures: &[i16]| {
            let mut outcome = MarkerFanOut::default();
            for code in failures {
                outcome.fail(refused(*code));
            }
            outcome.failure.map(|error| {
                let class = classify_marker_failure(&error);
                let code = match error {
                    BrokerError::MarkerWriteRefused { code, .. } => code,
                    _ => UNKNOWN_SERVER_ERROR,
                };
                (code, class)
            })
        };
        let unexpected = UNKNOWN_SERVER_ERROR;
        let retriable = NOT_ENOUGH_REPLICAS;
        let fenced = INVALID_PRODUCER_EPOCH;
        // (label, failure codes in the order they arrive, the code reported)
        let cases = [
            (
                "unexpected, then retriable",
                vec![unexpected, retriable],
                retriable,
            ),
            (
                "retriable, then unexpected",
                vec![retriable, unexpected],
                retriable,
            ),
            ("unexpected alone", vec![unexpected], unexpected),
            ("retriable, then fenced", vec![retriable, fenced], fenced),
            ("fenced, then retriable", vec![fenced, retriable], fenced),
            ("unexpected, then fenced", vec![unexpected, fenced], fenced),
            (
                "two retriable keep the first",
                vec![retriable, NOT_LEADER_OR_FOLLOWER],
                retriable,
            ),
            (
                "two fenced keep the first",
                vec![TRANSACTION_COORDINATOR_FENCED, fenced],
                TRANSACTION_COORDINATOR_FENCED,
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (label, failures, code) in cases {
            actual.push((label, reported(&failures)));
            let class = classify_marker_failure(&refused(code));
            expected.push((label, Some((code, class))));
        }
        assert!(actual == expected);
    }

    fn marker_context<'a>(
        image: &'a MetadataImage,
        client: &'a InterBrokerClient,
    ) -> MarkerDispatchContext<'a> {
        MarkerDispatchContext {
            node_id: NodeId(1),
            coordinator_epoch: 0,
            image,
            inter_broker_client: client,
            inter_broker_protocol: ListenerProtocol::Plaintext,
            inter_broker_listener_name: "PLAINTEXT",
            inter_broker_server_name: "localhost",
            group_coordinator: None,
        }
    }

    #[tokio::test]
    async fn marker_dispatch_skips_deleted_partition() {
        let image = MetadataImage::default();
        let client = plaintext_client();
        let partitions = Arc::new(PartitionRegistry::new());
        let mut entry = marker_entry();
        entry.partitions.insert(tps().remove(0));

        let result = dispatch_markers(
            marker_context(&image, &client),
            &partitions,
            &entry,
            MarkerType::Commit,
        )
        .await;

        // A deleted partition needs no marker, so it leaves the transaction.
        assert!(result.failure.is_none());
        assert!(result.written == tps());
    }

    #[tokio::test]
    async fn marker_dispatch_marks_materialized_partition_missing_from_image() {
        use krabka_log::Offset;

        let image = MetadataImage::default();
        let client = plaintext_client();
        let partitions = Arc::new(PartitionRegistry::new());
        let dir = tempfile::tempdir().unwrap();
        let part = open_partition().log_dir(dir.path()).topic("t").call();
        part.install_leader_change(2, 0).await;
        partitions.insert("t".into(), PartitionIndex(0), Arc::clone(&part));
        let mut entry = marker_entry();
        let target_tp = tps().remove(0);
        entry.partitions.insert(target_tp);

        let context = marker_context(&image, &client);
        assert!(
            dispatch_markers(context, &partitions, &entry, MarkerType::Commit)
                .await
                .failure
                .is_some()
        );
        assert!(part.log_end_offset() == Offset(0));

        // The metadata reconcile installs this broker as the leader.
        part.install_leader_change(1, 1).await;
        let written = dispatch_markers(context, &partitions, &entry, MarkerType::Commit).await;
        assert!(written.failure.is_none());

        assert!(part.log_end_offset() == Offset(1));
        assert!(part.last_stable_offset(Offset(1)) == Offset(1));
    }

    /// #882: a marker failure on one partition must not abandon the others
    /// in the same fan-out round. The local, reachable partition still gets
    /// its marker even though the remote, unreachable one fails.
    #[tokio::test]
    async fn marker_dispatch_appends_every_reachable_partition_despite_one_failing() {
        use {
            krabka_ids::PartitionIndex as PIdx,
            krabka_log::Offset,
            krabka_metadata::{
                BrokerRegistrationRecord, MetadataRecord, PartitionRecord, TopicRecord,
            },
        };

        let mut image = MetadataImage::default();
        for (topic, leader) in [("local-topic", NodeId(1)), ("remote-topic", NodeId(2))] {
            image.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: topic.to_owned(),
                topic_id: uuid::Uuid::nil(),
                partitions: 1,
                replication_factor: 1,
            }));
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: topic.to_owned(),
                partition: 0,
                leader,
                replicas: vec![leader],
                isr: vec![leader],
                ..Default::default()
            }));
        }
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                // Discard port: refuses connections immediately.
                port: 9,
                ..broker_registration(krabka_raft::NodeId(2))
            },
        ));

        let client = plaintext_client();
        let partitions = Arc::new(PartitionRegistry::new());
        let dir = tempfile::tempdir().unwrap();
        let local_partition = open_partition()
            .log_dir(dir.path())
            .topic("local-topic")
            .call();
        local_partition.install_leader_change(1, 0).await;
        partitions.insert("local-topic".into(), PIdx(0), Arc::clone(&local_partition));

        let mut entry = marker_entry();
        entry.partitions.insert(TopicPartition {
            topic: "local-topic".into(),
            partition: PIdx(0),
        });
        entry.partitions.insert(TopicPartition {
            topic: "remote-topic".into(),
            partition: PIdx(0),
        });

        let result = dispatch_markers(
            marker_context(&image, &client),
            &partitions,
            &entry,
            MarkerType::Commit,
        )
        .await;

        // Retriable: the remote connect failure is not a fenced generation.
        // #852: only the local partition's marker is written.
        assert!(
            result.written
                == vec![TopicPartition {
                    topic: "local-topic".into(),
                    partition: PIdx(0),
                }]
        );
        let error = result
            .failure
            .expect("the unreachable remote partition must fail");
        assert!(classify_marker_failure(&error) == MarkerFailureClass::Retriable);
        // The local partition's marker landed despite the remote failure.
        assert!(local_partition.log_end_offset() == Offset(1));
    }

    /// The local leg of the fan-out counts a marker as written only once the
    /// high watermark covers it. When this broker loses the partition first,
    /// the partition stays outstanding and the attempt fails with Kafka's
    /// retriable `NOT_LEADER_OR_FOLLOWER`, so the completion sends the marker
    /// again to the next leader.
    #[tokio::test]
    async fn a_local_marker_counts_only_once_committed() {
        use {
            krabka_log::Offset,
            krabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord},
        };

        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".to_owned(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".to_owned(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2)],
            isr: vec![NodeId(1), NodeId(2)],
            ..Default::default()
        }));
        let client = plaintext_client();
        // (case, whether the follower catches up, written, failure code)
        let cases = [
            ("the follower catches up", true, tps(), None),
            (
                "another broker takes the partition first",
                false,
                vec![],
                Some(NOT_LEADER_OR_FOLLOWER),
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (case, catches_up, written, failure) in cases {
            let partitions = Arc::new(PartitionRegistry::new());
            let dir = tempfile::tempdir().expect("tempdir");
            let part = open_partition().log_dir(dir.path()).topic("t").call();
            part.install_leader_change(1, 0).await;
            // The follower has not fetched, so it holds the high watermark at
            // zero.
            part.install_isr(&[NodeId(1), NodeId(2)], &[NodeId(1), NodeId(2)], NodeId(1))
                .await;
            partitions.insert("t".into(), PartitionIndex(0), Arc::clone(&part));
            let mut entry = marker_entry();
            entry.partitions.insert(tps().remove(0));

            let change = {
                let part = Arc::clone(&part);
                tokio::spawn(async move {
                    // The change lands once the marker waits to commit.
                    loop {
                        let appended = part.append_notify.notified();
                        tokio::pin!(appended);
                        appended.as_mut().enable();
                        if part.log_end_offset() >= Offset(1) {
                            break;
                        }
                        appended.await;
                    }
                    if catches_up {
                        part.replica_state.lock().await.hw = Offset(1);
                        part.hw_advance_notify.notify_waiters();
                    } else {
                        part.install_leader_change(2, 1).await;
                    }
                })
            };
            let outcome = dispatch_markers(
                marker_context(&image, &client),
                &partitions,
                &entry,
                MarkerType::Commit,
            )
            .await;
            change.await.expect("the change lands");
            let code = outcome.failure.as_ref().map(|error| match error {
                BrokerError::MarkerWriteRefused { code, .. } => *code,
                other => panic!("not a marker refusal: {other}"),
            });
            actual.push((case, outcome.written, code));
            expected.push((case, written, failure));
        }
        assert!(actual == expected);
    }
}
