//! Construction of a running controller: the low-level spawn over an already
//! seeded state, and [`KraftController::open`], which recovers the image,
//! the control state and the durable quorum state from a data directory first.

use {
    super::{
        Engine, KraftConfig, KraftControlState, KraftController, METADATA_LOG_CLEAN_INTERVAL,
        PendingDowngradeSnapshot,
        activation::{Activation, check_bootstrap_records},
        checkpoint::{BOOTSTRAP_SNAPSHOT_ID, latest_checkpoint_id, load_latest_checkpoint},
        quorum_state_file::load_quorum_state,
        records::features_epoch_after_snapshot,
        recovery::{
            CommittedReplay, control_state_at, replay_committed_with_timestamp,
            replay_control_records,
        },
        timing::initial_election_at,
    },
    crate::{
        config::{
            ControllerFetchMissLimit, MetadataLogConfig, MetadataRaftCommandQueueCapacity,
            MetadataRaftFetchMax,
        },
        error::RaftError,
        event_queue::{EventQueueMeter, EventQueueRecorder},
        kraft::{
            core::{QuorumStateMachine, QuorumTimeouts},
            log::KraftLog,
            snapshot_fetch::MetadataSnapshotFetchMax,
            transport::{CommandSender, PeerSender},
            types::{NodeId, QuorumState},
        },
        snapshot::SnapshotReader,
    },
    krabka_ids::Offset,
    krabka_metadata::{MetadataImage, MetadataRecord, VotersRecord},
    krabka_units::{
        fmt::Human as _,
        prelude::{ByteSize, Time},
    },
    std::{collections::BTreeMap, path::PathBuf, sync::Arc},
    tokio::{
        sync::{mpsc, watch},
        time::Instant,
    },
    uuid::Uuid,
};

/// Metadata recovered before the controller engine is spawned.
struct RecoveredMetadata {
    image: MetadataImage,
    last_snapshot_end_offset: Offset,
    last_snapshot_timestamp_ms: i64,
    last_applied_record_timestamp_ms: i64,
    downgrade_snapshot_pending: Option<PendingDowngradeSnapshot>,
    /// The finalized-features epoch of `image`, or `-1` before any features
    /// delta.
    features_epoch: i64,
}

impl KraftController {
    /// Build the engine over an already-opened [`KraftLog`] and spawn its loop
    /// task. Recovery (snapshot + replay + quorum-state file) is wired by
    /// [`Self::open`]; this lower-level entrypoint takes the seed state directly
    /// and is used by tests/drivers that supply their own [`KraftLog`].
    ///
    /// The returned handle's loop runs until [`Self::shutdown`] (or all handles
    /// drop). `data_dir` is where the engine writes the quorum-state file and
    /// checkpoints.
    ///
    /// # Panics
    ///
    /// Panics if constructing a fresh controller unexpectedly requires
    /// downgrade-checkpoint recovery. Fresh controllers have no recovered
    /// downgrade boundary, so this indicates an internal invariant violation.
    #[must_use]
    pub fn spawn(config: KraftConfig, log: KraftLog, data_dir: PathBuf) -> Self {
        let cluster_id = config.cluster_id;
        let image = MetadataImage::new(cluster_id);
        Self::spawn_with_image(config, log, data_dir, RecoveredMetadata {
            image,
            last_snapshot_end_offset: Offset(0),
            last_snapshot_timestamp_ms: 0,
            last_applied_record_timestamp_ms: 0,
            downgrade_snapshot_pending: None,
            features_epoch: -1,
        })
        .expect("fresh controller cannot have a pending downgrade checkpoint")
    }

    /// Spawn the engine starting from an already-recovered [`MetadataImage`]
    /// (the restart-recovery path through [`Self::open`] threads the rebuilt
    /// image in here so the published `current_image` reflects it immediately).
    fn spawn_with_image(
        config: KraftConfig,
        log: KraftLog,
        data_dir: PathBuf,
        recovered: RecoveredMetadata,
    ) -> Result<Self, RaftError> {
        let RecoveredMetadata {
            image,
            last_snapshot_end_offset,
            last_snapshot_timestamp_ms,
            last_applied_record_timestamp_ms,
            downgrade_snapshot_pending,
            features_epoch,
        } = recovered;
        let KraftConfig {
            me,
            cluster_id: _,
            directory_id,
            mut initial_state,
            timeouts,
            heartbeat_interval,
            controller_fetch_miss_limit,
            metadata_raft_command_queue_capacity,
            metadata_raft_fetch_max,
            peers,
            snapshot_interval_records,
            max_bytes_between_snapshots,
            max_snapshot_interval,
            metadata_snapshot_fetch_max,
            metadata_log,
            activation,
            event_queue_recorder,
        } = config;

        // Every record in a cleanly reopened log is committed. Recover the
        // latest control state before constructing the core so elections never
        // briefly use stale configured voters.
        replay_control_records(&log, &mut initial_state, metadata_raft_fetch_max)?;
        let controls =
            KraftControlState::new(initial_state.voters.clone(), initial_state.kraft_version);
        let core = QuorumStateMachine::new(me, initial_state, timeouts);
        let initial_leader = core.quorum_state().leader_id;
        let initial_was_leader = core.role().is_leader();
        let initial_epoch = core.quorum_state().leader_epoch;

        // The controller voter set lives in the raft `QuorumState` (seeded from
        // config under KIP-595 static voters, recovered from the quorum-state
        // file on restart), NOT on the KIP-631-framed metadata log — `V1Voters`
        // is a raft-control record with no KIP-631 counterpart. Mirror it into
        // the published `MetadataImage` so image readers (e.g. the broker's
        // voter-set views, auto-join) observe the live quorum membership.
        let mut image = image;
        image.apply(&MetadataRecord::V1KRaftVersion(
            krabka_metadata::KRaftVersionRecord {
                kraft_version: core.quorum_state().kraft_version,
            },
        ));
        image.apply(&MetadataRecord::V1Voters(VotersRecord {
            voters: core.quorum_state().voters.clone(),
        }));

        let (image_tx, image_rx) = watch::channel(Arc::new(image.clone()));
        let (features_epoch_tx, features_epoch_rx) = watch::channel(features_epoch);
        let (leader_tx, leader_rx) = watch::channel(initial_leader);
        // Captured before the log moves into `Engine`; it seeds both the first
        // published snapshot and the engine's `leader_reported_hwm`, which a
        // node that has heard from no leader yet answers from.
        let initial_hwm = log.hwm().0;
        let initial_snapshot = super::queries::initial_quorum_snapshot(
            &core,
            &log,
            initial_hwm,
            last_snapshot_timestamp_ms,
        );
        let (quorum_tx, quorum_rx) = watch::channel(initial_snapshot);
        let (fault_tx, fault_rx) = watch::channel(None);
        let (cmd_tx, cmd_rx) = CommandSender::channel(metadata_raft_command_queue_capacity.get());

        let clock_base = Instant::now();
        // A fresh voter arms its election timer so a bootstrap cluster elects
        // without an injected event. Observers/followers leave it disarmed.
        let election_at = initial_election_at(
            &core,
            initial_leader,
            clock_base,
            me,
            initial_epoch,
            timeouts.election,
        );

        let engine_peers = Arc::clone(&peers);
        let mut engine = Engine {
            me,
            core,
            log,
            image,
            peers,
            image_tx,
            features_epoch,
            features_epoch_tx,
            leader_tx,
            quorum_tx,
            cmd_tx: cmd_tx.clone(),
            event_queue: EventQueueMeter::new(event_queue_recorder),
            turn: None,
            fault_tx,
            data_dir,
            clock_base,
            timeouts,
            heartbeat_interval,
            controller_fetch_miss_limit,
            metadata_raft_fetch_max,
            election_at,
            fetch_at: None,
            check_quorum_at: None,
            fetch_misses: 0,
            discovery_attempts: 0,
            commit_waiters: Vec::new(),
            was_leader: initial_was_leader,
            held_epoch: initial_epoch,
            registration_writes: BTreeMap::new(),
            snapshot_interval_records,
            max_bytes_between_snapshots,
            max_snapshot_interval,
            metadata_snapshot_fetch_max,
            metadata_log,
            noop_at: None,
            clean_at: clock_base + METADATA_LOG_CLEAN_INTERVAL,
            last_snapshot_end_offset,
            last_snapshot_timestamp_ms,
            last_applied_record_timestamp_ms,
            pending_record_processing_time_us_total: 0,
            pending_record_processing_time_samples: 0,
            pending_record_processing_time_us_max: 0,
            last_snapshot_at_ms: 0,
            bytes_since_snapshot: 0,
            downgrade_snapshot_pending,
            #[cfg(test)]
            downgrade_snapshot_failures_remaining: 0,
            snapshot_fetch: None,
            installed_snapshot_epoch: None,
            controls,
            directory_id,
            observers: BTreeMap::new(),
            fetch_purgatory: Vec::new(),
            wall_clock_base: std::time::SystemTime::now(),
            leader_reported_hwm: initial_hwm,
            pending_reconfig: None,
            activation,
            activation_fault: None,
            replay_fault: None,
        };

        // A restart can rediscover a committed downgrade whose earlier local
        // checkpoint failed. Make one synchronous recovery attempt before the
        // engine is exposed; the regular event loop keeps retrying thereafter.
        while engine.downgrade_snapshot_pending.is_some() {
            engine.write_downgrade_snapshot_and_prune()?;
        }

        tokio::spawn(engine.run(cmd_rx));

        Ok(Self {
            cmd_tx,
            image_rx,
            features_epoch_rx,
            fault_rx,
            leader_rx,
            quorum_rx,
            peers: engine_peers,
            me,
            registration_admission: Arc::default(),
        })
    }

    /// Open the engine over `data_dir`: recover the [`MetadataImage`] from the
    /// latest checkpoint + replay committed log batches, and seed the durable
    /// [`QuorumState`] from the node-local quorum-state file. The
    /// `bootstrap` voter set/cluster id is used only when no quorum-state file
    /// exists yet.
    ///
    /// The bootstrap checkpoint, at offset 0 and epoch 0, is not a snapshot
    /// of the log. The engine takes its KIP-853 control state, but it does
    /// not apply its metadata records to the image. Those records replace
    /// the bootstrap records of `activation` when there are any, and the
    /// leader writes them to an empty log, as Kafka's
    /// `QuorumController.handleLoadBootstrap` keeps them for its activation.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the log/checkpoint cannot be opened or read, or
    /// `validate_image` refuses the recovered metadata before checkpoint repair
    /// or an engine task can start.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = me.0, %cluster_id, election_timeout = %timeouts.election.human(), fetch_timeout = %timeouts.fetch.human()),
        err
    )]
    #[allow(
        clippy::too_many_arguments,
        reason = "recovery inputs are independent and explicit at this low-level boundary"
    )]
    pub fn open(
        data_dir: PathBuf,
        me: NodeId,
        cluster_id: Uuid,
        directory_id: Uuid,
        bootstrap_voters: krabka_metadata::voters::VoterSet,
        timeouts: QuorumTimeouts,
        heartbeat_interval: Option<Time>,
        controller_fetch_miss_limit: ControllerFetchMissLimit,
        metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity,
        metadata_raft_fetch_max: MetadataRaftFetchMax,
        peers: Arc<dyn PeerSender>,
        snapshot_interval_records: u64,
        max_bytes_between_snapshots: ByteSize,
        max_snapshot_interval: Time,
        metadata_snapshot_fetch_max: MetadataSnapshotFetchMax,
        metadata_log: MetadataLogConfig,
        mut activation: Activation,
        event_queue_recorder: Arc<dyn EventQueueRecorder>,
        validate_image: impl FnOnce(&MetadataImage) -> Result<(), RaftError>,
    ) -> Result<Self, RaftError> {
        std::fs::create_dir_all(&data_dir).map_err(krabka_log::LogError::Io)?;
        let log = KraftLog::open(&data_dir, &metadata_log)?;

        // Recover the image from the checkpoint plus only the durable committed
        // prefix. An uncommitted voter record can remain at the log end after a
        // crash and must not become authoritative during restart.
        let mut image = MetadataImage::new(cluster_id);
        let mut snapshot_control = None;
        let mut last_snapshot_end_offset = Offset(0);
        let mut last_snapshot_timestamp_ms = 0;
        let mut features_epoch = -1;
        let mut bootstrap_source = "the controller configuration";
        if let Some(bytes) = load_latest_checkpoint(&data_dir)? {
            let contents = SnapshotReader::read(&bytes)?;
            // The records this checkpoint contains are below its boundary and
            // gone from the log, so its header is the only place their
            // create-time survives a restart.
            last_snapshot_timestamp_ms = contents.last_contained_log_timestamp;
            if latest_checkpoint_id(&data_dir) == Some(BOOTSTRAP_SNAPSHOT_ID) {
                // Kafka's `KafkaRaftClient` gives the bootstrap checkpoint to
                // `handleLoadBootstrap` and not to `handleLoadSnapshot`. Its
                // records are not in the log, so they are not in the image:
                // the leader writes them to the log, and every replica
                // applies them from there.
                if !contents.metadata_records.is_empty() {
                    tracing::info!(
                        records = contents.metadata_records.len(),
                        "loaded the bootstrap records of the bootstrap checkpoint"
                    );
                    activation.bootstrap_records = contents.metadata_records;
                    bootstrap_source = "the bootstrap checkpoint";
                }
            } else {
                image = MetadataImage::from_records(cluster_id, &contents.metadata_records);
                if let Some((end_offset, _)) = latest_checkpoint_id(&data_dir) {
                    features_epoch = features_epoch_after_snapshot(
                        &contents.metadata_records,
                        end_offset,
                        features_epoch,
                    );
                }
            }
            if let Some(control) = contents.control_state {
                image.apply(&MetadataRecord::V1KRaftVersion(
                    krabka_metadata::KRaftVersionRecord {
                        kraft_version: control.kraft_version,
                    },
                ));
                image.apply(&MetadataRecord::V1Voters(VotersRecord {
                    voters: control.voters.clone(),
                }));
                snapshot_control = Some(control);
            }
            if let Some((off, _ep)) = latest_checkpoint_id(&data_dir) {
                // Checkpoint filenames encode the raw offset (on-disk boundary).
                last_snapshot_end_offset = Offset(off);
            }
            // Replay starts at the durable log start below. This deliberately
            // rediscovers a downgrade when a crash landed after checkpoint
            // rename but before prefix pruning.
        }
        check_bootstrap_records(&activation.bootstrap_records, bootstrap_source)?;
        let CommittedReplay {
            pending: mut downgrade_snapshot_pending,
            last_applied_timestamp_ms: replayed_timestamp_ms,
            features_epoch: replayed_features_epoch,
        } = replay_committed_with_timestamp(
            &log,
            &mut image,
            log.log_start_offset(),
            metadata_raft_fetch_max,
        )?;
        let last_applied_record_timestamp_ms =
            replayed_timestamp_ms.unwrap_or(last_snapshot_timestamp_ms);
        // The replay starts at the log start, which can sit below a checkpoint
        // whose prefix is not yet cleaned. A feature batch met there is older
        // than the checkpoint's epoch and must not move it back.
        let features_epoch =
            replayed_features_epoch.map_or(features_epoch, |replayed| replayed.max(features_epoch));

        let mut boundary_state = QuorumState::bootstrap(cluster_id, bootstrap_voters.clone());
        if let Some(control) = &snapshot_control {
            boundary_state.kraft_version = control.kraft_version;
            boundary_state.voters = control.voters.clone();
        }

        // Seed the durable quorum state from the file, falling back to a fresh
        // bootstrap when absent.
        let mut initial_state = load_quorum_state(&data_dir, cluster_id, &bootstrap_voters)?
            .unwrap_or_else(|| QuorumState::bootstrap(cluster_id, bootstrap_voters));
        if let Some(control) = &snapshot_control {
            initial_state.kraft_version = control.kraft_version;
            initial_state.voters = control.voters.clone();
        }
        if let Some(pending) = &mut downgrade_snapshot_pending {
            let boundary_state = control_state_at(
                &log,
                &boundary_state,
                pending.end_offset,
                metadata_raft_fetch_max,
            )?;
            pending.image.apply(&MetadataRecord::V1KRaftVersion(
                krabka_metadata::KRaftVersionRecord {
                    kraft_version: boundary_state.kraft_version,
                },
            ));
            pending.image.apply(&MetadataRecord::V1Voters(VotersRecord {
                voters: boundary_state.voters,
            }));
        }
        replay_control_records(&log, &mut initial_state, metadata_raft_fetch_max)?;
        image.apply(&MetadataRecord::V1KRaftVersion(
            krabka_metadata::KRaftVersionRecord {
                kraft_version: initial_state.kraft_version,
            },
        ));
        image.apply(&MetadataRecord::V1Voters(VotersRecord {
            voters: initial_state.voters.clone(),
        }));

        validate_image(&image)?;

        Self::spawn_with_image(
            KraftConfig {
                me,
                cluster_id,
                directory_id,
                initial_state,
                timeouts,
                heartbeat_interval,
                controller_fetch_miss_limit,
                metadata_raft_command_queue_capacity,
                metadata_raft_fetch_max,
                peers,
                snapshot_interval_records,
                max_bytes_between_snapshots,
                max_snapshot_interval,
                metadata_snapshot_fetch_max,
                metadata_log,
                activation,
                event_queue_recorder,
            },
            log,
            data_dir,
            RecoveredMetadata {
                image,
                last_snapshot_end_offset,
                last_snapshot_timestamp_ms,
                last_applied_record_timestamp_ms,
                downgrade_snapshot_pending,
                features_epoch,
            },
        )
    }
}
