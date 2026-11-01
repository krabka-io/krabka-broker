//! The checks and the commit of one `CreateTopics` row.
//!
//! This is the part of Kafka's `ReplicationControlManager.createTopic` that
//! a topic goes through after authorization: the name, the existence check,
//! the configs, the shape, the replica placement, the topic policy, the
//! controller-mutation quota and the metadata commit. `CreateTopics` runs it
//! for each row that it gets, from a client or from the auto topic creation
//! of a broker ([`crate::auto_topic_creation::AutoTopicCreation`]).

use {
    super::{
        THROTTLING_QUOTA_EXCEEDED_MESSAGE, diskless_wal_placement_error, invalid_topic_shape,
        name::topic_name_error,
        placement::{
            PlacementCandidates, choose_leaderships, placement_candidates, resolve_assignments,
        },
        placement_failure_message,
        records::{null_config_error, topic_config_overrides, topic_records},
        resolve_default,
        response::topic_error_result,
        topic_exists_result,
    },
    crate::{
        broker::Broker,
        codes,
        config_keys::{self, resolve_preferred_leader_site},
        handlers::{
            partition_materialization::PartitionMaterialization,
            submit_failure::{controller_write_code, wrong_controller_write_message},
        },
        quota::ControllerMutationQuota,
        site_placement::PlacementRng,
        topic_policy::check,
    },
    krabka_protocol::owned::{
        create_topics_request::CreatableTopic, create_topics_response::CreatableTopicResult,
    },
    krabka_raft::RaftError,
    std::collections::BTreeMap,
    uuid::Uuid,
};

/// A topic that passed every check, and was committed unless the request is
/// validate-only.
pub struct NewTopic {
    pub name: String,
    pub topic_id: Uuid,
    /// The replicas of each partition, in partition order.
    pub assignments: Vec<Vec<krabka_raft::NodeId>>,
    /// The canonical config overrides the topic was created with.
    pub overrides: BTreeMap<String, String>,
}

/// The state that the checks of every row in one request share: the metadata
/// image the request reads, and what the broker derives from it once.
pub struct TopicCreation<'a> {
    broker: &'a Broker,
    image: &'a krabka_metadata::MetadataImage,
    preferred_site: Option<&'a str>,
    topic_defaults: config_keys::TopicDefaults,
    /// KIP-108: a validate-only request runs every check and commits nothing.
    validate_only: bool,
}

impl<'a> TopicCreation<'a> {
    pub fn new(
        broker: &'a Broker,
        image: &'a krabka_metadata::MetadataImage,
        validate_only: bool,
    ) -> Self {
        Self {
            broker,
            image,
            preferred_site: resolve_preferred_leader_site(image),
            topic_defaults: config_keys::TopicDefaults::from_image(
                image,
                broker.config.features.unstable_api_versions,
            ),
            validate_only,
        }
    }

    /// Runs the checks of one row and, unless the request is validate-only,
    /// commits the topic and opens its local partitions.
    ///
    /// `quota` is the KIP-599 controller-mutation quota of the request.
    ///
    /// # Errors
    ///
    /// Returns the refused row, in the shape Kafka answers it.
    pub async fn create(
        &self,
        topic_req: CreatableTopic,
        quota: &mut ControllerMutationQuota,
    ) -> Result<NewTopic, Box<CreatableTopicResult>> {
        let mut topic_req = topic_req;
        let broker = self.broker;
        let image = self.image;
        let name = topic_req.name.clone();

        // Kafka checks the name before anything else. The name becomes part
        // of the partition directory path, so no later step may see a name
        // that this check refuses.
        if let Some((code, message)) = topic_name_error(image, &name) {
            return Err(Box::new(topic_error_result(name, code, Some(message))));
        }

        // Kafka answers an existing topic TOPIC_ALREADY_EXISTS before it
        // looks at the configs, the counts, the placement or the policy, so
        // `kafka-topics --create --if-not-exists` succeeds whatever else the
        // request carries.
        if image.topic(&name).is_some() {
            return Err(Box::new(topic_exists_result(name)));
        }

        // Kafka's `computeConfigChanges` refuses a config with a null value
        // before it validates the others.
        if let Some(message) = null_config_error(&topic_req) {
            return Err(Box::new(topic_error_result(
                name,
                codes::INVALID_CONFIG,
                Some(message),
            )));
        }

        // Kafka validates a topic's configs before it looks at placement, so a
        // rejected config wins over INVALID_PARTITIONS on the same topic.
        // The stored map carries each value in the form Kafka reports it, so
        // every reader of it parses ` TRUE ` as it parses `true`.
        let config_overrides = match config_keys::canonical_topic_config_map(
            &topic_config_overrides(&topic_req),
            &self.topic_defaults,
            broker.config.remote_storage_backend.is_some(),
            broker.config.features.unstable_api_versions,
        ) {
            Ok(canonical) => canonical,
            Err(reason) => {
                return Err(Box::new(topic_error_result(
                    name,
                    codes::INVALID_CONFIG,
                    Some(reason),
                )));
            }
        };

        // `canonical_topic_config_map` checks tiered storage against the
        // broker's backend, but not the diskless flag. A diskless topic needs
        // the same thing from the broker: an object-store backend. Without
        // `remote_storage_backend` there is no `DisklessReadHandle`, so the
        // broker starts neither the WAL index projection nor the object
        // flusher. The topic would still accept writes through its WAL
        // quorum, but nothing would ever move them to the object tier and
        // nothing would ever trim the local logs, so the durability model the
        // flag advertises would not exist and local storage would grow without
        // bound. Refuse at creation rather than accept a topic that cannot
        // work.
        //
        // This reads *this* broker's configuration, and the topic is
        // cluster-wide, so it is a guard against the common
        // one-configuration-fleet mistake rather than a cluster-wide
        // guarantee: a fleet where only some brokers carry a backend can still
        // create a topic that some of them cannot serve. Catching the default
        // configuration is the case worth having.
        let diskless = config_keys::resolve_diskless(Some(&config_overrides));
        if diskless && broker.config.remote_storage_backend.is_none() {
            return Err(Box::new(topic_error_result(
                name,
                codes::INVALID_CONFIG,
                Some(format!(
                    "{}=true requires an object-store tier, but this broker has no \
                     `remote_storage_backend` configured; the diskless WAL could never flush \
                     or trim",
                    config_keys::DISKLESS
                )),
            )));
        }

        // Kafka's `ReplicationControlManager.createTopic` checks the
        // replication factor first and the partition count second. Each may
        // be -1, which KIP-464 resolves to the broker's `num.partitions` or
        // `default.replication.factor`. A manual assignment requires -1 for
        // both, and `resolve_assignments` checks that.
        if topic_req.assignments.is_empty() {
            if let Some((code, message)) = invalid_topic_shape(&topic_req) {
                return Err(Box::new(topic_error_result(
                    name,
                    code,
                    Some(message.to_owned()),
                )));
            }
            topic_req.num_partitions =
                resolve_default(topic_req.num_partitions, broker.config.num_partitions);
            topic_req.replication_factor = resolve_default(
                topic_req.replication_factor,
                broker.config.default_replication_factor,
            );
        }

        let manual = !topic_req.assignments.is_empty();
        let PlacementCandidates { brokers, inactive } =
            placement_candidates(broker, image, manual).await;

        let assignments = match resolve_assignments(
            &topic_req,
            &brokers,
            self.preferred_site,
            &mut PlacementRng::from_entropy(),
        ) {
            Ok(assignments) => assignments,
            Err((code, message)) => {
                return Err(Box::new(topic_error_result(name, code, Some(message))));
            }
        };

        if assignments.is_empty() {
            // The placement cannot satisfy the request. RF above the broker
            // count, fenced brokers included, and a cluster with no unfenced
            // broker are the common causes. Surface INVALID_REPLICATION_FACTOR
            // with the message of Kafka's replica placer.
            return Err(Box::new(topic_error_result(
                name,
                codes::INVALID_REPLICATION_FACTOR,
                Some(placement_failure_message(
                    topic_req.replication_factor,
                    &brokers,
                )),
            )));
        }

        let leaderships = match choose_leaderships(&assignments, &inactive, image, manual, 0) {
            Ok(leaderships) => leaderships,
            Err(message) => {
                return Err(Box::new(topic_error_result(
                    name,
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    Some(message),
                )));
            }
        };

        if diskless
            && let Some(reason) =
                diskless_wal_placement_error(image, &broker.config, 0, &leaderships)
        {
            return Err(Box::new(topic_error_result(
                name,
                codes::INVALID_CONFIG,
                Some(reason),
            )));
        }

        // KIP-108: the operator-declared topic policy, on the effective
        // partition count and replication factor the placement resolved and
        // on the topic's own config overrides. Kafka calls
        // `CreateTopicPolicy.validate` here too: after config validation, and
        // before the records are generated.
        if let Err(reason) = check(
            &broker.config.topic_policy,
            &name,
            Some(assignments.len()),
            assignments.first().map(Vec::len),
            &config_overrides,
        ) {
            return Err(Box::new(topic_error_result(
                name,
                codes::POLICY_VIOLATION,
                Some(reason),
            )));
        }

        // KIP-599: charge the partitions this topic creates.
        let partition_count = u64::try_from(assignments.len()).unwrap_or(u64::MAX);
        if quota.record(partition_count).is_err() {
            return Err(Box::new(topic_error_result(
                name,
                codes::THROTTLING_QUOTA_EXCEEDED,
                Some(THROTTLING_QUOTA_EXCEEDED_MESSAGE.into()),
            )));
        }

        // Kafka's `Uuid.randomUuid`: never a leading dash.
        let topic_id = krabka_format::random_uuid();

        // A validate-only request has now passed every check the committing
        // path runs, and commits nothing.
        if !self.validate_only {
            let placement = Placement {
                topic_id,
                assignments: &assignments,
                leaderships: &leaderships,
                overrides: &config_overrides,
                diskless,
            };
            self.commit(&topic_req, &placement).await?;
        }
        Ok(NewTopic {
            name,
            topic_id,
            assignments,
            overrides: config_overrides,
        })
    }

    /// Commits the records of a checked topic, and opens the partitions this
    /// broker holds.
    async fn commit(
        &self,
        topic_req: &CreatableTopic,
        placement: &Placement<'_>,
    ) -> Result<(), Box<CreatableTopicResult>> {
        let broker = self.broker;
        let name = &topic_req.name;
        // Build the batch: one TopicRecord + N PartitionRecords.
        let records = topic_records(
            topic_req,
            placement.topic_id,
            placement.assignments,
            placement.leaderships,
            placement.overrides,
        );

        let failure = match broker.controller.submit_change(records).await {
            Ok(_) => {
                PartitionMaterialization {
                    broker,
                    log_dirs: &broker.config.all_log_dirs(),
                    diskless: placement.diskless,
                    topic_id: placement.topic_id,
                }
                .materialize(
                    "CreateTopics",
                    name,
                    (0..placement.assignments.len()).map(|index| i32::try_from(index).unwrap_or(0)),
                    placement.assignments,
                    placement.leaderships,
                )
                .await;
                return Ok(());
            }
            Err(error) => {
                let leader = *broker.controller.watch_leader().borrow();
                submit_failure_result(name.clone(), &error, leader)
            }
        };
        Err(Box::new(failure))
    }
}

/// The row a topic answers when its metadata commit fails with `error`.
/// `leader` is the controller leader this node knows of when the commit
/// failed.
fn submit_failure_result(
    name: String,
    error: &RaftError,
    leader: Option<krabka_raft::NodeId>,
) -> CreatableTopicResult {
    match error {
        // Another request created the name after this one read the image. The
        // quorum decides that race, and the row is the one Kafka's existence
        // check answers.
        RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_)) => {
            topic_exists_result(name)
        }
        RaftError::Metadata(krabka_metadata::MetadataError::InvalidRecord(_)) => {
            // E.g., `partitions <= 0` rejected by image::validate.
            topic_error_result(name, codes::INVALID_PARTITIONS, None)
        }
        // A lost or unknown leader, and a new leader that refuses the write
        // with `UncommittedTail` until it commits its own epoch, answer
        // `NOT_CONTROLLER`, as Kafka's inactive controller does. Kafka's
        // `ApiError.fromThrowable` keeps the message of the controller's
        // `newWrongControllerException`, which names the raft leader the
        // controller knows of: the redirect of a lost leadership, and the
        // leader this node knows for a tail that is not committed yet.
        error => {
            let code = controller_write_code(error, codes::UNKNOWN_SERVER_ERROR);
            if code == codes::UNKNOWN_SERVER_ERROR {
                tracing::error!(topic = %name, %error, "CreateTopics submit_change failed");
            }
            let message = wrong_controller_write_message(error, leader);
            topic_error_result(name, code, message)
        }
    }
}

/// The placement and configs a checked topic is committed with.
struct Placement<'a> {
    topic_id: Uuid,
    assignments: &'a [Vec<krabka_raft::NodeId>],
    leaderships: &'a [super::InitialLeadership],
    overrides: &'a BTreeMap<String, String>,
    diskless: bool,
}

#[cfg(test)]
mod tests {
    use {
        super::submit_failure_result,
        crate::codes,
        krabka_metadata::MetadataError,
        krabka_protocol::owned::create_topics_response::CreatableTopicResult,
        krabka_raft::{NodeId, RaftError},
    };

    /// The row Kafka's `CreateTopicsRequest.getErrorResponse` builds for a
    /// refused topic, with the empty `Configs` Kafka sends.
    fn row(error_code: i16, error_message: Option<&str>) -> CreatableTopicResult {
        CreatableTopicResult {
            name: "orders".to_owned(),
            error_code,
            error_message: error_message.map(str::to_owned),
            configs: Some(Vec::new()),
            ..Default::default()
        }
    }

    /// A newly elected controller leader refuses a write with
    /// `UncommittedTail` until it commits its own epoch. Kafka's controller is
    /// not active in that window and answers `NOT_CONTROLLER`, which the admin
    /// client retries, as it answers a lost leadership.
    ///
    /// The row carries the message of Kafka's
    /// `ControllerExceptions.newWrongControllerException`, which names
    /// `QuorumController.latestController()`, the raft leader the refusing
    /// node knows of. A lost leadership names the leader it was redirected to.
    /// A new leader that has not committed its epoch is that raft leader, so
    /// the row names the leader this node knows. Every other row keeps its
    /// own message.
    #[yare::parameterized(
        not_leader = {
            RaftError::NotLeader { current_leader: Some(NodeId(2)) },
            Some(NodeId(3)),
            row(codes::NOT_CONTROLLER, Some("The active controller appears to be node 2.")),
        },
        not_leader_without_hint = {
            RaftError::NotLeader { current_leader: None },
            Some(NodeId(3)),
            row(codes::NOT_CONTROLLER, Some("No controller appears to be active.")),
        },
        leader_unknown = {
            RaftError::LeaderUnknown,
            None,
            row(codes::NOT_CONTROLLER, Some("No controller appears to be active.")),
        },
        uncommitted_tail = {
            RaftError::UncommittedTail,
            Some(NodeId(1)),
            row(codes::NOT_CONTROLLER, Some("The active controller appears to be node 1.")),
        },
        uncommitted_tail_without_leader = {
            RaftError::UncommittedTail,
            None,
            row(codes::NOT_CONTROLLER, Some("No controller appears to be active.")),
        },
        topic_exists = {
            RaftError::Metadata(MetadataError::TopicExists("orders".to_owned())),
            Some(NodeId(1)),
            row(codes::TOPIC_ALREADY_EXISTS, Some("Topic 'orders' already exists.")),
        },
        invalid_record = {
            RaftError::Metadata(MetadataError::InvalidRecord("partitions")),
            Some(NodeId(1)),
            row(codes::INVALID_PARTITIONS, None),
        },
        shutdown = { RaftError::Shutdown, Some(NodeId(1)), row(codes::UNKNOWN_SERVER_ERROR, None) },
        change_rejected = {
            RaftError::ChangeRejected("stale".to_owned()),
            Some(NodeId(1)),
            row(codes::UNKNOWN_SERVER_ERROR, None),
        },
    )]
    fn a_failed_commit_answers_kafkas_row(
        error: RaftError,
        leader: Option<NodeId>,
        expected: CreatableTopicResult,
    ) {
        assert2::assert!(submit_failure_result("orders".to_owned(), &error, leader) == expected);
    }
}
