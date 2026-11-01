//! Construction of a running controller: the low-level spawn over an already
//! seeded state, and [`KraftController::open`], which recovers the image,
//! the control state and the durable quorum state from a data directory first.

use {
    super::{
        Engine, KraftConfig, KraftControlState, KraftController, KraftOpenConfig,
        METADATA_LOG_CLEAN_INTERVAL, PendingDowngradeSnapshot,
        activation::{Activation, check_bootstrap_records},
        applied_image::AppliedImage,
        checkpoint::{
            BOOTSTRAP_SNAPSHOT_ID, checkpoint_ids, latest_checkpoint_id, load_checkpoint_by_id,
            load_latest_checkpoint,
        },
        quorum_state_file::load_quorum_state,
        records::features_epoch_after_snapshot,
        recovery::{
            CommittedReplay, control_state_at, holds_metadata_version_downgrade,
            replay_committed_with_timestamp, replay_control_records,
            with_uncommitted_feature_levels,
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
            snapshot_fetch::{MetadataSnapshotFetchMax, SnapshotId},
            transport::{CommandSender, PeerSender, QueuedCommand},
            types::{NodeId, QuorumState},
        },
        snapshot::{SnapshotReader, mirror_controls},
    },
    krabka_ids::Offset,
    krabka_metadata::{MetadataImage, MetadataRecord, VotersRecord, voters::VoterSet},
    krabka_units::{
        fmt::Human as _,
        prelude::{ByteSize, Time},
    },
    std::{
        collections::BTreeMap,
        fs,
        path::{Path, PathBuf},
        sync::Arc,
        time::SystemTime,
    },
    tokio::{
        sync::{mpsc, watch},
        time::{Duration, Instant},
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
    /// # Errors
    ///
    /// Returns an error if starting the engine fails. A fresh controller has
    /// no recovered downgrade boundary, so it needs no downgrade-checkpoint
    /// recovery.
    pub fn spawn(config: KraftConfig, log: KraftLog, data_dir: PathBuf) -> Result<Self, RaftError> {
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
        let Assembled {
            controller,
            engine,
            cmd_rx,
        } = Self::assemble(config, log, data_dir, recovered)?;
        tokio::spawn(engine.run(cmd_rx));
        Ok(controller)
    }

    /// The engine over `log` and `recovered`, the handle that drives it, and
    /// the queue the handle sends to, before the engine loop is spawned.
    fn assemble(
        config: KraftConfig,
        log: KraftLog,
        data_dir: PathBuf,
        recovered: RecoveredMetadata,
    ) -> Result<Assembled, RaftError> {
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
            ..
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
        mirror_controls(
            &mut image,
            core.quorum_state().kraft_version,
            core.quorum_state().voters.clone(),
        );

        let (image, image_rx) = AppliedImage::new(image);
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
            snapshot_job: None,
            snapshot_write_delay: Duration::ZERO,
            downgrade_snapshot_pending,
            #[cfg(test)]
            downgrade_snapshot_failures_remaining: 0,
            #[cfg(test)]
            apply_read_failure_at: None,
            snapshot_fetch: None,
            installed_snapshot_epoch: None,
            controls,
            directory_id,
            observers: BTreeMap::new(),
            fetch_purgatory: Vec::new(),
            wall_clock_base: SystemTime::now(),
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

        Ok(Assembled {
            controller: Self {
                cmd_tx,
                image_rx,
                features_epoch_rx,
                fault_rx,
                leader_rx,
                quorum_rx,
                peers: engine_peers,
                me,
                registration_admission: Arc::default(),
            },
            engine,
            cmd_rx,
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
    /// `validate_image` refuses the committed image, or that image with every
    /// feature level the log holds above its high watermark, before
    /// checkpoint repair or an engine task can start.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            node = config.me.0,
            cluster_id = %config.cluster_id,
            election_timeout = %config.timeouts.election.human(),
            fetch_timeout = %config.timeouts.fetch.human(),
        ),
        err
    )]
    pub fn open(
        data_dir: PathBuf,
        config: KraftOpenConfig,
        validate_image: impl Fn(&MetadataImage) -> Result<(), RaftError>,
    ) -> Result<Self, RaftError> {
        let (config, log, recovered) = Self::recover(&data_dir, config, validate_image)?;
        Self::spawn_with_image(config, log, data_dir, recovered)
    }

    /// The engine [`Self::open`] spawns, assembled over `data_dir` but not
    /// spawned, so a test can drive the recovered engine turn by turn.
    ///
    /// # Errors
    /// As [`Self::open`].
    #[cfg(test)]
    pub(super) fn open_assembled(
        data_dir: PathBuf,
        config: KraftOpenConfig,
        validate_image: impl Fn(&MetadataImage) -> Result<(), RaftError>,
    ) -> Result<Assembled, RaftError> {
        let (config, log, recovered) = Self::recover(&data_dir, config, validate_image)?;
        Self::assemble(config, log, data_dir, recovered)
    }

    /// The recovery half of [`Self::open`]: the log, the image and the seed
    /// quorum state that `data_dir` holds, checked by `validate_image`.
    fn recover(
        data_dir: &Path,
        mut config: KraftOpenConfig,
        validate_image: impl Fn(&MetadataImage) -> Result<(), RaftError>,
    ) -> Result<(KraftConfig, KraftLog, RecoveredMetadata), RaftError> {
        let cluster_id = config.cluster_id;
        let metadata_raft_fetch_max = config.metadata_raft_fetch_max;
        fs::create_dir_all(data_dir).map_err(krabka_log::LogError::Io)?;
        let mut log = KraftLog::open(data_dir, &config.metadata_log)?;

        // Recover the image from the checkpoint plus only the durable committed
        // prefix. An uncommitted voter record can remain at the log end after a
        // crash and must not become authoritative during restart.
        let mut image = MetadataImage::new(cluster_id);
        let mut snapshot_control = None;
        let mut last_snapshot_end_offset = Offset(0);
        let mut last_snapshot_timestamp_ms = 0;
        let mut features_epoch = -1;
        let mut bootstrap_source = "the controller configuration";
        if let Some(bytes) = load_latest_checkpoint(data_dir)? {
            let contents = SnapshotReader::read(&bytes)?;
            // The records this checkpoint contains are below its boundary and
            // gone from the log, so its header is the only place their
            // create-time survives a restart.
            last_snapshot_timestamp_ms = contents.last_contained_log_timestamp;
            if latest_checkpoint_id(data_dir) == Some(BOOTSTRAP_SNAPSHOT_ID) {
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
                    config.activation.bootstrap_records = contents.metadata_records;
                    bootstrap_source = "the bootstrap checkpoint";
                }
            } else {
                image = MetadataImage::from_records(cluster_id, &contents.metadata_records);
                if let Some((end_offset, _)) = latest_checkpoint_id(data_dir) {
                    features_epoch = features_epoch_after_snapshot(
                        &contents.metadata_records,
                        end_offset,
                        features_epoch,
                    );
                }
            }
            if let Some(control) = contents.control_state {
                mirror_controls(&mut image, control.kraft_version, control.voters.clone());
                snapshot_control = Some(control);
            }
            if let Some((off, _ep)) = latest_checkpoint_id(data_dir) {
                // Checkpoint filenames encode the raw offset (on-disk boundary).
                last_snapshot_end_offset = Offset(off);
            }
        }
        check_bootstrap_records(&config.activation.bootstrap_records, bootstrap_source)?;
        // The image stands at the checkpoint end, and the replay continues
        // from there, as Kafka's `MetadataLoader` applies only the batches
        // after the snapshot it loaded. The log keeps the prefix below the
        // checkpoint until retention cleans it, and a second apply of that
        // prefix would bump the epochs of every partition it changed again.
        let downgrade_below_checkpoint = match latest_checkpoint_id(data_dir) {
            Some(id) if id != BOOTSTRAP_SNAPSHOT_ID => {
                log.recover_to_snapshot(last_snapshot_end_offset)?;
                unfinished_downgrade_prune(
                    data_dir,
                    &log,
                    cluster_id,
                    &image,
                    id,
                    metadata_raft_fetch_max,
                )?
            }
            _ => None,
        };
        let CommittedReplay {
            pending: replayed_downgrade,
            last_applied_timestamp_ms: replayed_timestamp_ms,
            features_epoch: replayed_features_epoch,
        } = replay_committed_with_timestamp(
            &log,
            &mut image,
            log.log_start_offset().max(last_snapshot_end_offset),
            metadata_raft_fetch_max,
        )?;
        // The prune of a downgrade below the checkpoint comes first. Its
        // repair replays the log above the checkpoint again, and finds there
        // any downgrade that follows it.
        let mut downgrade_snapshot_pending = downgrade_below_checkpoint.or(replayed_downgrade);
        let last_applied_record_timestamp_ms =
            replayed_timestamp_ms.unwrap_or(last_snapshot_timestamp_ms);
        let features_epoch = replayed_features_epoch.unwrap_or(features_epoch);

        let mut boundary_state =
            QuorumState::bootstrap(cluster_id, config.bootstrap_voters.clone());
        if let Some(control) = &snapshot_control {
            boundary_state.kraft_version = control.kraft_version;
            boundary_state.voters = control.voters.clone();
        }

        // Seed the durable quorum state from the file, falling back to a fresh
        // bootstrap when absent.
        let mut initial_state = load_quorum_state(data_dir, cluster_id, &config.bootstrap_voters)?
            .unwrap_or_else(|| QuorumState::bootstrap(cluster_id, config.bootstrap_voters.clone()));
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
            mirror_controls(
                &mut pending.image,
                boundary_state.kraft_version,
                boundary_state.voters,
            );
        }
        replay_control_records(&log, &mut initial_state, metadata_raft_fetch_max)?;
        mirror_controls(
            &mut image,
            initial_state.kraft_version,
            initial_state.voters.clone(),
        );

        // The guard reads the committed image, which the engine starts from,
        // and then every feature level the log holds above the high
        // watermark: the high watermark recovered from its cache file can sit
        // below a level that the engine would commit and then refuse. The
        // committed image is read on its own too, because an uncommitted
        // record can supersede a committed level that the engine still
        // starts at.
        validate_image(&image)?;
        validate_image(&with_uncommitted_feature_levels(
            &log,
            &image,
            metadata_raft_fetch_max,
        )?)?;

        Ok((
            config.into_kraft_config(initial_state),
            log,
            RecoveredMetadata {
                image,
                last_snapshot_end_offset,
                last_snapshot_timestamp_ms,
                last_applied_record_timestamp_ms,
                downgrade_snapshot_pending,
                features_epoch,
            },
        ))
    }
}

/// A controller handle, the engine behind it, and the queue between them,
/// before the engine loop is spawned.
pub(super) struct Assembled {
    pub(super) controller: KraftController,
    pub(super) engine: Engine,
    pub(super) cmd_rx: mpsc::Receiver<QueuedCommand>,
}

/// The repair a restart owes the checkpoint `checkpoint` when the log prefix
/// below it holds a `metadata.version` downgrade: a crash after the
/// downgrade checkpoint was written and before the prefix was pruned. The
/// repair rewrites `image`, the checkpoint's image, at the same boundary and
/// prunes the prefix. `None` when the log starts at the checkpoint or the
/// prefix holds no downgrade.
///
/// The fold starts from the image at the log start: an empty image at offset
/// 0, and otherwise the checkpoint the log start sits on, which is where a
/// cleaning moves it. Without one, the first level in the prefix counts as no
/// downgrade.
fn unfinished_downgrade_prune(
    data_dir: &Path,
    log: &KraftLog,
    cluster_id: Uuid,
    image: &MetadataImage,
    checkpoint: SnapshotId,
    max: MetadataRaftFetchMax,
) -> Result<Option<PendingDowngradeSnapshot>, RaftError> {
    let (end_offset, epoch) = checkpoint;
    let log_start = log.log_start_offset();
    if log_start >= Offset(end_offset) {
        return Ok(None);
    }
    let base = checkpoint_ids(data_dir)
        .into_iter()
        .find(|(offset, _)| Offset(*offset) == log_start)
        .and_then(|(offset, epoch)| load_checkpoint_by_id(data_dir, offset, epoch))
        .map(|bytes| SnapshotReader::read(&bytes))
        .transpose()?
        .map_or_else(
            || MetadataImage::new(cluster_id),
            |contents| MetadataImage::from_records(cluster_id, &contents.metadata_records),
        );
    let downgraded = holds_metadata_version_downgrade(log, base, Offset(end_offset), max)?;
    Ok(downgraded.then(|| PendingDowngradeSnapshot {
        image: image.clone(),
        end_offset: Offset(end_offset),
        epoch,
    }))
}
