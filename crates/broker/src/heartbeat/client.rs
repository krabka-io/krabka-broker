//! Broker-side heartbeat client. It sends `BrokerHeartbeat` to the
//! controller leader at every configured `heartbeat_interval`, over one
//! connection that it keeps across ticks. It finds the current controller in
//! the metadata image, and it retries after transient errors.
//!
//! KIP-919 puts `BrokerHeartbeat` on the controller's CONTROLLER listener, so
//! the leader's address is the one the raft transport already dials it on --
//! its KIP-853 voter endpoint -- not a broker registration. A controller-only
//! node never registers as a broker, so resolving the leader through
//! `image.broker()` would strand every broker in a role-separated cluster: no
//! heartbeat would ever arrive, and the controller's liveness registry would
//! fence the whole cluster one `liveness_tick_interval` after boot.

use {
    crate::{
        codes::{CLUSTER_AUTHORIZATION_FAILED, NONE, NOT_CONTROLLER},
        controller_endpoint::leader_endpoint,
        cordoned_log_dirs::CordonedLogDirs,
        incarnation::own_broker_epoch,
        log_dir_id::LogDirIds,
        log_dir_status::LogDirRegistry,
        metadata_source::MetadataSource,
        network::client::InterBrokerClient,
    },
    krabka_client_core::ConnectionOptions,
    krabka_metadata::{MetadataImage, metadata_version},
    krabka_protocol::{
        owned::{
            broker_heartbeat_request::BrokerHeartbeatRequest,
            broker_heartbeat_response::BrokerHeartbeatResponse,
        },
        primitives::uuid::Uuid,
    },
    krabka_security::ListenerProtocol,
    krabka_units::{Time, convert::TimeExt as _, fmt::Human as _, millis, secs},
    std::{path::PathBuf, sync::Arc, time::Duration},
    tokio::{
        sync::watch::{Receiver, Sender},
        time,
    },
    tokio_util::sync::CancellationToken,
    tracing::{debug, warn},
};

/// Where this broker's first unfencing stands: Kafka's
/// `BrokerLifecycleManager.initialUnfenceFuture`, which broker startup waits
/// on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialUnfence {
    /// No heartbeat answer has unfenced the broker yet.
    Pending,
    /// A heartbeat answer said the broker is unfenced.
    Unfenced,
    /// The controller refused the heartbeat with
    /// `CLUSTER_AUTHORIZATION_FAILED`. Nothing but an ACL change lets the
    /// broker unfence, so startup stops waiting for it.
    Refused,
}

pub struct Config {
    pub broker_id: i32,
    /// The broker epoch this process registered at, which every heartbeat
    /// names: Kafka's `BrokerLifecycleManager.brokerEpoch`. The local image
    /// can still hold the previous incarnation's registration for a while
    /// after a restart, and a heartbeat that named its epoch would be refused
    /// as `STALE_BROKER_EPOCH`.
    pub broker_epoch: i64,
    /// This process's incarnation, which picks its own registrations out of
    /// the image; see [`own_broker_epoch`].
    pub incarnation_id: uuid::Uuid,
    pub interval: Time,
    pub controller: Arc<dyn MetadataSource>,
    pub shutdown: CancellationToken,
    /// Shared outbound dialer that reaches the controller leader. It runs
    /// TLS / SASL when the controller listener needs them. If not, it uses
    /// plain TCP. This is the same dialer the raft transport uses to reach
    /// its peers, so a heartbeat travels the channel the quorum already
    /// authenticates on.
    pub outbound_client: Arc<InterBrokerClient>,
    pub controller_listener_protocol: ListenerProtocol,
    /// SNI and SASL server name for the controller listener, matching the one
    /// the raft dialer presents.
    pub controller_server_name: String,
    /// The statically configured quorum, `controller_quorum_voters`. It backs
    /// the voter set as a source of the leader's address, for the window
    /// before the committed voter set names it. See
    /// [`crate::controller_endpoint::leader_endpoint`].
    pub controller_quorum_voters: Vec<(krabka_raft::NodeId, String)>,
    /// When `true`, the client stamps `want_shut_down=true` on outbound
    /// `BrokerHeartbeat` requests.
    /// [`crate::BrokerHandle::controlled_shutdown`] drives this flag.
    pub want_shutdown: Receiver<bool>,
    /// The client sets this to `true` when the controller responds with
    /// `should_shut_down=true`. The caller of `controlled_shutdown`
    /// awaits this flag.
    pub should_shutdown: Arc<Sender<bool>>,
    /// The client moves this out of [`InitialUnfence::Pending`] on the first
    /// answer that unfences the broker or refuses it for authorization.
    pub unfenced: Sender<InitialUnfence>,
    /// Per-log-dir health registry. Each heartbeat reports the offline dirs
    /// to the controller as `offline_log_dirs` UUIDs (KIP-858).
    pub log_dir_status: LogDirRegistry,
    /// Stable per-log-dir UUIDs, to translate offline dir paths to ids.
    pub log_dir_ids: LogDirIds,
    /// All configured log dirs. When every one of them is offline, the
    /// broker shuts itself down (KIP-112).
    pub all_log_dirs: Vec<PathBuf>,
    /// The broker cancels this when all dirs go offline. This stops
    /// replication and materialization against dead disks before teardown.
    pub supervisor_shutdown: CancellationToken,
    /// KIP-1066: the log directories this broker holds cordoned. Heartbeats
    /// report their ids once the broker has caught up with the metadata.
    pub cordoned_log_dirs: CordonedLogDirs,
}

/// UUIDs of the currently-offline log dirs, for the heartbeat's `offline_log_dirs`.
fn offline_dir_uuids(status: &LogDirRegistry, ids: &LogDirIds) -> Vec<Uuid> {
    status
        .offline()
        .into_iter()
        .filter_map(|(path, _reason)| ids.id_for(&path))
        .map(|u| Uuid(*u.as_bytes()))
        .collect()
}

/// True when every configured log dir is offline. The broker then shuts itself
/// down.
fn all_dirs_offline(all_log_dirs: &[PathBuf], status: &LogDirRegistry) -> bool {
    !all_log_dirs.is_empty() && all_log_dirs.iter().all(|d| status.is_offline(d))
}

/// Returns `true` when every configured log dir is currently offline.
fn all_log_dirs_offline(cfg: &Config) -> bool {
    all_dirs_offline(&cfg.all_log_dirs, &cfg.log_dir_status)
}

/// How soon a broker in controlled shutdown asks again after the controller's
/// first answer that it may not stop yet: Kafka's `BrokerLifecycleManager`
/// schedules that heartbeat 50 ms out, rather than one interval, because the
/// controller has usually moved the broker's leaderships by then.
const CONTROLLED_SHUTDOWN_RECHECK: Duration = Duration::from_millis(50);

fn heartbeat_rpc_timeout(interval: Time) -> Time { (interval * 2.0).max(millis(500)).min(secs(1)) }

fn heartbeat_connection_options(broker_id: i32, interval: Time) -> ConnectionOptions {
    let timeout = heartbeat_rpc_timeout(interval);
    ConnectionOptions {
        client_id: format!("krabka-broker-{broker_id}-heartbeat"),
        socket_connection_setup_timeout: timeout,
        socket_connection_setup_timeout_max: timeout,
        request_timeout: timeout,
        ..ConnectionOptions::default()
    }
}

/// The cordoned directory ids a heartbeat carries, or `None` when it carries
/// none.
///
/// Kafka's `BrokerLifecycleManager.sendBrokerHeartbeat` sets
/// `CordonedLogDirs` only once the broker has caught up with the metadata
/// (`initialCatchUpFuture`), because the dynamic config is up to date only
/// then, and only while `metadata.version` is at least `4.3-IV0`.
fn cordoned_dir_uuids(
    caught_up: bool,
    image: &krabka_metadata::MetadataImage,
    cordoned: &CordonedLogDirs,
    ids: &LogDirIds,
) -> Option<Vec<Uuid>> {
    let supported = image
        .finalized_metadata_version()
        .is_some_and(|level| level >= metadata_version::CORDONED_LOG_DIRS_MIN_LEVEL);
    (caught_up && supported).then(|| {
        cordoned
            .cordoned()
            .iter()
            .filter_map(|dir| ids.id_for(dir))
            .map(|id| Uuid(*id.as_bytes()))
            .collect()
    })
}

/// The two directory lists a heartbeat reports.
#[derive(Debug, Default)]
struct HeartbeatLogDirs {
    /// KIP-858 directories that went offline.
    offline: Vec<Uuid>,
    /// KIP-1066 cordoned directories, `None` before the broker catches up.
    cordoned: Option<Vec<Uuid>>,
}

fn heartbeat_request(
    broker_id: i32,
    broker_epoch: i64,
    current_metadata_offset: i64,
    want_shut_down: bool,
    log_dirs: HeartbeatLogDirs,
) -> BrokerHeartbeatRequest {
    BrokerHeartbeatRequest {
        broker_id,
        broker_epoch,
        current_metadata_offset,
        want_shut_down,
        offline_log_dirs: log_dirs.offline,
        cordoned_log_dirs: log_dirs.cordoned,
        ..Default::default()
    }
}

/// The heartbeat this tick sends: the registration it names, the metadata it
/// has applied, whether it asks to shut down, and its log dirs.
fn next_heartbeat(
    cfg: &Config,
    caught_up: bool,
    image: &MetadataImage,
    want_shut_down: bool,
) -> BrokerHeartbeatRequest {
    let log_dirs = HeartbeatLogDirs {
        offline: offline_dir_uuids(&cfg.log_dir_status, &cfg.log_dir_ids),
        cordoned: cordoned_dir_uuids(caught_up, image, &cfg.cordoned_log_dirs, &cfg.log_dir_ids),
    };
    heartbeat_request(
        cfg.broker_id,
        registered_epoch(cfg, image),
        cfg.controller.current_metadata_offset(),
        want_shut_down,
        log_dirs,
    )
}

/// Triggers the KIP-112 self-shutdown. It latches `should_shutdown` and
/// cancels the supervisor. Every early-exit path calls it, so the check is not
/// accidentally skipped when the controller is temporarily unreachable.
fn trigger_all_dirs_offline_shutdown(cfg: &Config, reason: &str) {
    tracing::error!(
        reason,
        "all log dirs offline — initiating broker self-shutdown (KIP-112)"
    );
    let _ = cfg.should_shutdown.send(true);
    cfg.supervisor_shutdown.cancel();
}

/// Runs the KIP-112 self-shutdown, with `reason`, when every configured log
/// dir is offline, and says whether it did.
fn stop_if_all_dirs_offline(cfg: &Config, reason: &str) -> bool {
    let offline = all_log_dirs_offline(cfg);
    if offline {
        trigger_all_dirs_offline_shutdown(cfg, reason);
    }
    offline
}

/// The broker epoch a heartbeat names: the newest registration of this
/// process, as [`own_broker_epoch`] reads it from `image`.
fn registered_epoch(cfg: &Config, image: &krabka_metadata::MetadataImage) -> i64 {
    u64::try_from(cfg.broker_id).map_or(cfg.broker_epoch, |node_id| {
        own_broker_epoch(
            image,
            krabka_metadata::NodeId(node_id),
            cfg.incarnation_id,
            cfg.broker_epoch,
        )
    })
}

/// What the heartbeat loop carries from one answer to the next heartbeat.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct HeartbeatState {
    /// Kafka's `initialCatchUpFuture`: set by the first answer that says the
    /// broker has caught up, and never cleared.
    caught_up: bool,
    /// Kafka's `gotControlledShutdownResponse`: whether the controller has
    /// answered a heartbeat that asked to shut down.
    controlled_shutdown_answered: bool,
}

/// What the heartbeat loop does after the controller answers a heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseOutcome {
    /// The controller refused the heartbeat. The tick ends at once, without
    /// the post-send log-dir check. The connection stays open unless the
    /// controller answered `NOT_CONTROLLER`.
    Rejected { keep_channel: bool },
    /// The controller accepted the heartbeat, and the connection stays open.
    /// `recheck_soon` asks for the next heartbeat after
    /// [`CONTROLLED_SHUTDOWN_RECHECK`] rather than one interval.
    Accepted { recheck_soon: bool },
}

/// Applies the controller's answer to a heartbeat that asked to shut down
/// when `want_shut_down`: it moves the initial unfence, latches
/// `should_shutdown` and updates `state`.
fn apply_response(
    cfg: &Config,
    state: &mut HeartbeatState,
    want_shut_down: bool,
    r: &BrokerHeartbeatResponse,
) -> ResponseOutcome {
    if r.error_code == CLUSTER_AUTHORIZATION_FAILED {
        cfg.unfenced.send_if_modified(|state| {
            let pending = *state == InitialUnfence::Pending;
            if pending {
                *state = InitialUnfence::Refused;
            }
            pending
        });
    }
    if r.error_code != NONE {
        warn!(
            error_code = r.error_code,
            "heartbeat rejected by controller"
        );
        return ResponseOutcome::Rejected {
            keep_channel: r.error_code != NOT_CONTROLLER,
        };
    }
    state.caught_up |= r.is_caught_up;
    if !r.is_fenced {
        cfg.unfenced.send_replace(InitialUnfence::Unfenced);
    }
    if r.should_shut_down {
        // Latch true; never flip back. The `BrokerHandle::controlled_shutdown`
        // waiter is single-shot.
        let _ = cfg.should_shutdown.send(true);
    } else if want_shut_down && !state.controlled_shutdown_answered {
        state.controlled_shutdown_answered = true;
        return ResponseOutcome::Accepted { recheck_soon: true };
    }
    ResponseOutcome::Accepted {
        recheck_soon: false,
    }
}

/// The controller leader, and the controller-listener endpoint the metadata
/// names for it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ControllerAddress {
    leader: krabka_raft::NodeId,
    host: String,
    port: u16,
}

/// The current controller leader and the address of its controller listener,
/// with the metadata image that named it, or `None` while either is unknown.
fn resolve_controller(cfg: &Config) -> Option<(ControllerAddress, Arc<MetadataImage>)> {
    let leader_id = *cfg.controller.watch_leader().borrow();
    let Some(leader_id) = leader_id else {
        debug!("heartbeat: no controller leader yet");
        return None;
    };
    let image = cfg.controller.current_image();
    let Some((host, port)) = leader_endpoint(&image, &cfg.controller_quorum_voters, leader_id)
    else {
        debug!(
            leader = leader_id.0,
            "heartbeat: controller leader has no known controller endpoint yet"
        );
        return None;
    };
    let controller = ControllerAddress {
        leader: leader_id,
        host,
        port,
    };
    Some((controller, image))
}

/// The connection the heartbeats travel on, and the controller it reaches.
struct ControllerChannel {
    controller: ControllerAddress,
    connection: krabka_client_core::Connection,
}

/// Dials the controller listener of `controller`, with the TLS and SASL that
/// the listener needs, or gives up after `rpc_timeout`.
async fn dial(
    cfg: &Config,
    controller: &ControllerAddress,
    rpc_timeout: Time,
) -> Option<krabka_client_core::Connection> {
    let opts = heartbeat_connection_options(cfg.broker_id, cfg.interval);
    let dialled = time::timeout(
        rpc_timeout.to_std(),
        cfg.outbound_client.connect_as_connection(
            &controller.host,
            controller.port,
            cfg.controller_listener_protocol,
            &cfg.controller_server_name,
            opts,
        ),
    )
    .await;
    match dialled {
        Ok(Ok(connection)) => Some(connection),
        Ok(Err(error)) => {
            debug!(%error, "heartbeat: connect failed");
            None
        }
        Err(_) => {
            debug!(
                rpc_timeout = %rpc_timeout.human(),
                "heartbeat: connect timed out"
            );
            None
        }
    }
}

/// The open connection in `channel` when it still reaches `controller`, or a
/// new one dialled to `controller` after closing the stale one. `None` when
/// the dial fails.
async fn reuse_or_dial(
    cfg: &Config,
    channel: Option<ControllerChannel>,
    controller: ControllerAddress,
    rpc_timeout: Time,
) -> Option<ControllerChannel> {
    match channel {
        Some(open) if open.controller == controller && !open.connection.is_closed() => Some(open),
        stale => {
            if let Some(stale) = stale {
                stale.connection.close();
            }
            let connection = dial(cfg, &controller, rpc_timeout).await?;
            Some(ControllerChannel {
                controller,
                connection,
            })
        }
    }
}

/// Sends a `BrokerHeartbeat` to the controller leader at every tick.
///
/// Every heartbeat travels on one connection, as Kafka's
/// `BrokerLifecycleManager` sends them through one
/// `NodeToControllerChannelManager`. Its `NodeToControllerRequestThread` keeps
/// one `NetworkClient` connection to the active controller. That connection
/// closes when it fails, when a heartbeat times out, and when the controller
/// answers `NOT_CONTROLLER`. A tick that finds no open connection to the
/// current leader dials one, so a new leader, or a new address for the same
/// leader, gets a new connection.
pub async fn run(mut cfg: Config) {
    // A node that never registered as a broker has no heartbeat to send, as a
    // Kafka controller-only node has no `BrokerLifecycleManager`.
    if cfg.broker_epoch < 0 {
        return;
    }
    let mut tick = time::interval(cfg.interval.to_std());
    let mut channel: Option<ControllerChannel> = None;
    let mut state = HeartbeatState::default();
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            // Kafka's `BeginControlledShutdownEvent` heartbeats at once rather
            // than at the next tick, so the drain starts without waiting out an
            // interval.
            Ok(()) = cfg.want_shutdown.changed() => tick.reset(),
            () = cfg.shutdown.cancelled() => return,
        }
        // KIP-112 check: even if we cannot reach the controller, self-shutdown
        // must fire as long as every log dir is offline.
        if stop_if_all_dirs_offline(&cfg, "detected before controller resolution") {
            return;
        }
        // Resolve the current controller leader's CONTROLLER-listener address
        // from the metadata image, or skip this tick if it is not known yet.
        let Some((controller, image)) = resolve_controller(&cfg) else {
            continue;
        };
        let rpc_timeout = heartbeat_rpc_timeout(cfg.interval);
        let Some(open) = reuse_or_dial(&cfg, channel.take(), controller, rpc_timeout).await else {
            continue;
        };
        let want_shut_down = *cfg.want_shutdown.borrow_and_update();
        let request = next_heartbeat(&cfg, state.caught_up, &image, want_shut_down);
        let resp = time::timeout(rpc_timeout.to_std(), open.connection.send(request)).await;
        match resp {
            Ok(Ok(r)) => match apply_response(&cfg, &mut state, want_shut_down, &r) {
                ResponseOutcome::Rejected { keep_channel } => {
                    if keep_channel {
                        channel = Some(open);
                    } else {
                        open.connection.close();
                    }
                    continue;
                }
                ResponseOutcome::Accepted { recheck_soon } => {
                    channel = Some(open);
                    if recheck_soon {
                        tick.reset_after(CONTROLLED_SHUTDOWN_RECHECK);
                    }
                }
            },
            Ok(Err(e)) => {
                warn!(error = %e, "heartbeat send failed");
                open.connection.close();
            }
            Err(_) => {
                warn!(
                    rpc_timeout = %rpc_timeout.human(),
                    "heartbeat send timed out"
                );
                open.connection.close();
            }
        }

        // KIP-112: re-check after the heartbeat round-trip. This covers the
        // window where dirs went offline *during* the connect/send. The
        // top-of-tick check already handles dirs that were offline before
        // leader resolution; this one handles the same-tick race.
        if stop_if_all_dirs_offline(&cfg, "detected after heartbeat send") {
            // Returning stops heartbeats; if shutdown drags, the controller's
            // session timeout fences this broker independently.
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            codes::{CLUSTER_AUTHORIZATION_FAILED, NONE, NOT_CONTROLLER, STALE_BROKER_EPOCH},
            cordoned_log_dirs::CordonedLogDirs,
            features::METADATA_VERSION,
            log_dir_id::LogDirIds,
            log_dir_status::LogDirRegistry,
            metadata_source::MetadataSource,
            test_support::{FakeMetadataSource, broker_registration, two_log_dirs},
        },
        assert2::{assert, check},
        bytes::BytesMut,
        krabka_client_core::{MockBroker, MockReply},
        krabka_metadata::{BrokerRegistrationRecord, MetadataRecord},
        krabka_protocol::{
            Encode as _,
            owned::{
                api_versions_request,
                api_versions_response::{ApiVersion, ApiVersionsResponse},
                broker_heartbeat_request,
                broker_heartbeat_response::BrokerHeartbeatResponse,
            },
        },
        std::{
            mem,
            path::Path,
            sync::Mutex,
            time::{Duration, Instant},
        },
        tokio::{
            sync::watch,
            time::{sleep, timeout},
        },
    };

    // How a fake controller answers a heartbeat.
    #[derive(Debug, Clone, Copy)]
    enum Answer {
        // No error.
        Accept,
        // `NOT_CONTROLLER`, as a controller that lost the leadership answers.
        NotController,
        // `CLUSTER_AUTHORIZATION_FAILED`, as a controller that refuses the
        // broker's principal answers.
        Refuse,
        // No answer, so the heartbeat times out.
        Silent,
        // No answer, and the fake closes the connection.
        Close,
    }

    #[derive(Debug)]
    struct Seen {
        // Every connection starts with one `ApiVersions` exchange, so this
        // counts the connections the client dialled.
        connections: usize,
        heartbeats: usize,
        next_answer: Answer,
        // The heartbeat, counted from one, from which every answer says
        // `should_shut_down`, or `None` when none does.
        shut_down_from: Option<usize>,
    }

    // A controller listener that answers `ApiVersions` and `BrokerHeartbeat`.
    struct FakeController {
        broker: MockBroker,
        seen: Arc<Mutex<Seen>>,
    }

    impl FakeController {
        async fn start() -> Self {
            let seen = Arc::new(Mutex::new(Seen {
                connections: 0,
                heartbeats: 0,
                next_answer: Answer::Accept,
                shut_down_from: None,
            }));
            let handled = Arc::clone(&seen);
            let broker = MockBroker::start_with_replies(move |api_key, version, _, _| {
                let mut seen = handled.lock().unwrap();
                let mut body = BytesMut::new();
                if api_key == api_versions_request::API_KEY {
                    seen.connections += 1;
                    ApiVersionsResponse {
                        api_keys: vec![
                            ApiVersion {
                                api_key: api_versions_request::API_KEY,
                                min_version: 0,
                                max_version: api_versions_request::MAX_VERSION,
                                ..Default::default()
                            },
                            ApiVersion {
                                api_key: broker_heartbeat_request::API_KEY,
                                min_version: 0,
                                max_version: broker_heartbeat_request::MAX_VERSION,
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }
                    .encode(&mut body, version)
                    .unwrap();
                    return MockReply::Respond(body.to_vec());
                }
                seen.heartbeats += 1;
                let error_code = match mem::replace(&mut seen.next_answer, Answer::Accept) {
                    Answer::Accept => NONE,
                    Answer::NotController => NOT_CONTROLLER,
                    Answer::Refuse => CLUSTER_AUTHORIZATION_FAILED,
                    Answer::Silent => return MockReply::Silent,
                    Answer::Close => return MockReply::Close,
                };
                let should_shut_down = seen
                    .shut_down_from
                    .is_some_and(|from| seen.heartbeats >= from);
                drop(seen);
                // `BrokerHeartbeat` is flexible, so its response header ends
                // with an empty tagged-field section.
                body.extend_from_slice(&[0]);
                BrokerHeartbeatResponse {
                    error_code,
                    should_shut_down,
                    ..Default::default()
                }
                .encode(&mut body, version)
                .unwrap();
                MockReply::Respond(body.to_vec())
            })
            .await;
            Self { broker, seen }
        }

        fn connections(&self) -> usize { self.seen.lock().unwrap().connections }

        fn heartbeats(&self) -> usize { self.seen.lock().unwrap().heartbeats }

        fn answer_next(&self, answer: Answer) { self.seen.lock().unwrap().next_answer = answer; }

        fn shut_down_from(&self, heartbeat: usize) {
            self.seen.lock().unwrap().shut_down_from = Some(heartbeat);
        }

        // Waits until the fake has seen `more` heartbeats after the ones it
        // has seen already.
        async fn wait_for_heartbeats(&self, more: usize) {
            let target = self.heartbeats() + more;
            let deadline = Instant::now() + Duration::from_secs(10);
            while self.heartbeats() < target {
                assert!(Instant::now() < deadline, "no heartbeat #{target} in 10s");
                sleep(Duration::from_millis(5)).await;
            }
        }
    }

    // Broker 7, registered at epoch 11, with voter 1 the controller leader.
    fn registered_broker_source() -> Arc<FakeMetadataSource> {
        Arc::new(
            FakeMetadataSource::builder()
                .records(&[MetadataRecord::V1BrokerRegistration(
                    BrokerRegistrationRecord {
                        broker_epoch: 11,
                        host: "localhost".into(),
                        ..broker_registration(krabka_raft::NodeId(7))
                    },
                )])
                .leader(Some(krabka_raft::NodeId(1)))
                .build(),
        )
    }

    // The heartbeat client of broker 7 against `controllers`, which are voters
    // 1, 2, ... in order, with `log_dirs` as its log directories.
    #[bon::builder]
    fn heartbeat_config(
        source: &Arc<FakeMetadataSource>,
        shutdown: &CancellationToken,
        interval: Time,
        controllers: &[&FakeController],
        want_shutdown: Receiver<bool>,
        #[builder(default = Arc::new(watch::channel(false).0))] should_shutdown: Arc<Sender<bool>>,
        #[builder(default = watch::channel(InitialUnfence::Pending).0)] unfenced: Sender<
            InitialUnfence,
        >,
        #[builder(default)] log_dirs: Vec<PathBuf>,
    ) -> Config {
        Config {
            broker_id: 7,
            broker_epoch: 11,
            incarnation_id: uuid::Uuid::nil(),
            interval,
            controller: Arc::clone(source) as Arc<dyn MetadataSource>,
            shutdown: shutdown.clone(),
            outbound_client: Arc::new(InterBrokerClient::new(None, None)),
            controller_listener_protocol: ListenerProtocol::Plaintext,
            controller_server_name: "localhost".into(),
            controller_quorum_voters: (1..)
                .zip(controllers)
                .map(|(id, controller)| {
                    (krabka_raft::NodeId(id), controller.broker.addr.to_string())
                })
                .collect(),
            want_shutdown,
            should_shutdown,
            unfenced,
            log_dir_status: LogDirRegistry::probe(&log_dirs),
            log_dir_ids: LogDirIds::resolve(&log_dirs),
            all_log_dirs: log_dirs,
            supervisor_shutdown: CancellationToken::new(),
            cordoned_log_dirs: CordonedLogDirs::default(),
        }
    }

    // Kafka's `NodeToControllerRequestThread` keeps one connection to the
    // active controller. It closes it when it fails, when a request times out
    // and when the controller answers `NOT_CONTROLLER`, and the next request
    // goes to the controller the metadata names then.
    #[tokio::test]
    async fn heartbeats_share_one_connection_until_it_breaks_or_the_leader_moves() {
        let first = FakeController::start().await;
        let second = FakeController::start().await;
        let dials = || (first.connections(), second.connections());
        let source = registered_broker_source();
        let (_want_shutdown, want_shutdown) = watch::channel(false);
        let shutdown = CancellationToken::new();
        let heartbeats = tokio::spawn(run(heartbeat_config()
            .source(&source)
            .shutdown(&shutdown)
            .interval(millis(20))
            .controllers(&[&first, &second])
            .want_shutdown(want_shutdown)
            .call()));

        // The dials each fake has taken after each step, compared at the end.
        first.wait_for_heartbeats(5).await;
        let mut dialled = vec![("five heartbeats", dials())];
        for (step, answer) in [
            ("a closed connection", Answer::Close),
            ("a heartbeat that timed out", Answer::Silent),
            ("NOT_CONTROLLER", Answer::NotController),
        ] {
            first.answer_next(answer);
            first.wait_for_heartbeats(3).await;
            dialled.push((step, dials()));
        }
        source.set_leader(Some(krabka_raft::NodeId(2)));
        second.wait_for_heartbeats(3).await;
        dialled.push(("a new leader", dials()));
        shutdown.cancel();
        heartbeats.await.unwrap();

        assert!(
            dialled
                == vec![
                    ("five heartbeats", (1, 0)),
                    ("a closed connection", (2, 0)),
                    ("a heartbeat that timed out", (3, 0)),
                    ("NOT_CONTROLLER", (4, 0)),
                    ("a new leader", (4, 1)),
                ]
        );
    }

    /// Kafka's `BrokerLifecycleManager`: `beginControlledShutdown` heartbeats
    /// at once, and the first answer that says the broker may not stop yet is
    /// followed 50 ms later by the next heartbeat. The interval here is a
    /// minute, so only those two heartbeats can reach the controller's
    /// `should_shut_down` in time.
    #[tokio::test]
    async fn a_controlled_shutdown_heartbeats_at_once_and_asks_again_soon() {
        let controller = FakeController::start().await;
        // The first heartbeat is the boot one; the second asks to shut down,
        // and the third is told it may.
        controller.shut_down_from(3);
        let source = registered_broker_source();
        let (want_shutdown_tx, want_shutdown) = watch::channel(false);
        let should_shutdown = Arc::new(watch::channel(false).0);
        let shutdown = CancellationToken::new();
        let heartbeats = tokio::spawn(run(heartbeat_config()
            .source(&source)
            .shutdown(&shutdown)
            .interval(secs(60))
            .controllers(&[&controller])
            .want_shutdown(want_shutdown)
            .should_shutdown(Arc::clone(&should_shutdown))
            .call()));
        controller.wait_for_heartbeats(1).await;

        let asked = Instant::now();
        want_shutdown_tx.send_replace(true);
        let mut told = should_shutdown.subscribe();
        let answered = timeout(Duration::from_secs(10), told.wait_for(|stop| *stop))
            .await
            .is_ok();
        let took = asked.elapsed();
        shutdown.cancel();
        heartbeats.await.unwrap();

        check!((answered, controller.heartbeats()) == (true, 3));
        check!(took < Duration::from_secs(5), "{took:?}");
    }

    /// Kafka's `BrokerLifecycleManager` completes `initialUnfenceFuture`
    /// exceptionally on `CLUSTER_AUTHORIZATION_FAILED`. The refusal is not
    /// `NOT_CONTROLLER`, so the connection stays open for the next heartbeat.
    #[tokio::test]
    async fn a_refused_heartbeat_stops_the_initial_unfence_and_keeps_the_connection() {
        let controller = FakeController::start().await;
        controller.answer_next(Answer::Refuse);
        let source = registered_broker_source();
        let (want_shutdown_tx, want_shutdown) = watch::channel(false);
        let (unfenced, mut initial_unfence) = watch::channel(InitialUnfence::Pending);
        let shutdown = CancellationToken::new();
        let heartbeats = tokio::spawn(run(heartbeat_config()
            .source(&source)
            .shutdown(&shutdown)
            .interval(secs(60))
            .controllers(&[&controller])
            .want_shutdown(want_shutdown)
            .unfenced(unfenced)
            .call()));

        let settled = timeout(
            Duration::from_secs(10),
            initial_unfence.wait_for(|state| *state != InitialUnfence::Pending),
        )
        .await
        .ok()
        .and_then(|state| state.ok().map(|state| *state));
        // A controlled shutdown heartbeats at once, without waiting out the
        // minute-long interval.
        want_shutdown_tx.send_replace(true);
        controller.wait_for_heartbeats(1).await;
        shutdown.cancel();
        heartbeats.await.unwrap();

        check!(settled == Some(InitialUnfence::Refused));
        check!(controller.connections() == 1);
    }

    /// KIP-112: a broker whose every log directory is offline shuts itself
    /// down at the next tick without a heartbeat.
    #[tokio::test]
    async fn every_log_dir_offline_shuts_the_broker_down_before_a_heartbeat() {
        let controller = FakeController::start().await;
        let (a, b, paths) = two_log_dirs();
        let source = registered_broker_source();
        let (_want_shutdown, want_shutdown) = watch::channel(false);
        let should_shutdown = Arc::new(watch::channel(false).0);
        let told = should_shutdown.subscribe();
        let shutdown = CancellationToken::new();
        let cfg = heartbeat_config()
            .source(&source)
            .shutdown(&shutdown)
            .interval(millis(20))
            .controllers(&[&controller])
            .want_shutdown(want_shutdown)
            .should_shutdown(should_shutdown)
            .log_dirs(paths)
            .call();
        cfg.log_dir_status.mark_offline(a.path(), "test");
        cfg.log_dir_status.mark_offline(b.path(), "test");
        let supervisor = cfg.supervisor_shutdown.clone();

        let stopped = timeout(Duration::from_secs(10), run(cfg)).await;

        check!(stopped.is_ok());
        check!(*told.borrow());
        check!(supervisor.is_cancelled());
        check!(controller.heartbeats() == 0);
    }

    // Everything `apply_response` changes, compared whole.
    #[derive(Debug, PartialEq, Eq)]
    struct Applied {
        outcome: ResponseOutcome,
        state: HeartbeatState,
        unfence: InitialUnfence,
        told_to_shut_down: bool,
    }

    // Applies `response` to a client in `before` whose initial unfence stands
    // at `unfence`.
    fn apply(
        before: HeartbeatState,
        unfence: InitialUnfence,
        want_shut_down: bool,
        response: &BrokerHeartbeatResponse,
    ) -> Applied {
        let source = registered_broker_source();
        let shutdown = CancellationToken::new();
        let (_want_shutdown, want_shutdown) = watch::channel(want_shut_down);
        let cfg = heartbeat_config()
            .source(&source)
            .shutdown(&shutdown)
            .interval(secs(1))
            .controllers(&[])
            .want_shutdown(want_shutdown)
            .unfenced(watch::channel(unfence).0)
            .call();
        let told = cfg.should_shutdown.subscribe();
        let mut state = before;
        let outcome = apply_response(&cfg, &mut state, want_shut_down, response);
        Applied {
            outcome,
            state,
            unfence: *cfg.unfenced.borrow(),
            told_to_shut_down: *told.borrow(),
        }
    }

    /// Kafka's `BrokerLifecycleManager` heartbeat response handler: an error
    /// answer changes nothing but the authorization refusal, and closes the
    /// connection only on `NOT_CONTROLLER`; an accepted answer latches the
    /// catch-up, the unfencing and the shutdown, and the first one to a
    /// shutdown request asks again soon.
    #[test]
    fn apply_response_follows_the_lifecycle_rules() {
        let fresh = HeartbeatState::default();
        let caught_up = HeartbeatState {
            caught_up: true,
            ..fresh
        };
        let answered = HeartbeatState {
            controlled_shutdown_answered: true,
            ..fresh
        };
        let fenced = |error_code| BrokerHeartbeatResponse {
            error_code,
            is_fenced: true,
            ..Default::default()
        };
        // Every flag that an accepted answer would act on.
        let eager = |error_code| BrokerHeartbeatResponse {
            error_code,
            is_caught_up: true,
            is_fenced: false,
            should_shut_down: true,
            ..Default::default()
        };
        let accepted = |recheck_soon| ResponseOutcome::Accepted { recheck_soon };
        let rejected = |keep_channel| ResponseOutcome::Rejected { keep_channel };
        let applied = |outcome, state, unfence, told_to_shut_down| Applied {
            outcome,
            state,
            unfence,
            told_to_shut_down,
        };
        let pending = InitialUnfence::Pending;
        let unfenced = InitialUnfence::Unfenced;
        let refused = InitialUnfence::Refused;
        let cases = [
            (
                "accepted, caught up and unfenced",
                (fresh, pending, false),
                BrokerHeartbeatResponse {
                    is_caught_up: true,
                    is_fenced: false,
                    ..fenced(NONE)
                },
                applied(accepted(false), caught_up, unfenced, false),
            ),
            (
                "accepted while fenced",
                (fresh, pending, false),
                fenced(NONE),
                applied(accepted(false), fresh, pending, false),
            ),
            (
                "the catch-up is never cleared",
                (caught_up, pending, false),
                fenced(NONE),
                applied(accepted(false), caught_up, pending, false),
            ),
            (
                "told to shut down",
                (fresh, pending, true),
                BrokerHeartbeatResponse {
                    should_shut_down: true,
                    ..fenced(NONE)
                },
                applied(accepted(false), fresh, pending, true),
            ),
            (
                "the first answer to a shutdown request",
                (fresh, pending, true),
                fenced(NONE),
                applied(accepted(true), answered, pending, false),
            ),
            (
                "a later answer to a shutdown request",
                (answered, pending, true),
                fenced(NONE),
                applied(accepted(false), answered, pending, false),
            ),
            (
                "NOT_CONTROLLER",
                (fresh, pending, true),
                eager(NOT_CONTROLLER),
                applied(rejected(false), fresh, pending, false),
            ),
            (
                "CLUSTER_AUTHORIZATION_FAILED before unfencing",
                (fresh, pending, true),
                eager(CLUSTER_AUTHORIZATION_FAILED),
                applied(rejected(true), fresh, refused, false),
            ),
            (
                "CLUSTER_AUTHORIZATION_FAILED after unfencing",
                (fresh, unfenced, false),
                eager(CLUSTER_AUTHORIZATION_FAILED),
                applied(rejected(true), fresh, unfenced, false),
            ),
            (
                "another error",
                (fresh, pending, true),
                eager(STALE_BROKER_EPOCH),
                applied(rejected(true), fresh, pending, false),
            ),
        ];
        for (label, (before, unfence, want_shut_down), response, want) in cases {
            check!(
                apply(before, unfence, want_shut_down, &response) == want,
                "{label}"
            );
        }
    }

    #[test]
    fn offline_dir_uuids_maps_offline_paths() {
        let (a, _b, paths) = two_log_dirs();
        let ids = LogDirIds::resolve(&paths);
        let status = LogDirRegistry::probe(&paths);

        // Initially no dirs are offline.
        assert!(offline_dir_uuids(&status, &ids).is_empty());

        // Mark dir `a` as offline.
        status.mark_offline(a.path(), "test");
        let result = offline_dir_uuids(&status, &ids);
        assert!(result.len() == 1);
        let expected_id = ids.id_for(a.path()).unwrap();
        assert!(result[0].0 == *expected_id.as_bytes());
    }

    #[test]
    fn all_dirs_offline_true_only_when_every_dir_offline() {
        let (a, b, paths) = two_log_dirs();
        let status = LogDirRegistry::probe(&paths);

        // Empty all_log_dirs: always false.
        assert!(!all_dirs_offline(&[], &status));

        // No dirs offline yet.
        assert!(!all_dirs_offline(&paths, &status));

        // Only `a` offline: still false.
        status.mark_offline(a.path(), "disk error");
        assert!(!all_dirs_offline(&paths, &status));

        // Both offline: true.
        status.mark_offline(b.path(), "disk error");
        assert!(all_dirs_offline(&paths, &status));
    }

    #[yare::parameterized(
        minimum_timeout = { millis(50), millis(500) },
        interval_timeout = { millis(500), secs(1) },
        maximum_timeout = { secs(5), secs(1) },
    )]
    fn heartbeat_rpc_timeout_tracks_interval_with_bounds(interval: Time, want: Time) {
        assert2::assert!(heartbeat_rpc_timeout(interval) == want, "{interval:?}");
    }

    #[test]
    fn heartbeat_connection_options_use_bounded_rpc_timeout() {
        let opts = heartbeat_connection_options(9, millis(500));

        check!(opts.client_id == "krabka-broker-9-heartbeat");
        check!(opts.socket_connection_setup_timeout == secs(1));
        check!(opts.socket_connection_setup_timeout_max == secs(1));
        check!(opts.request_timeout == secs(1));
    }

    #[test]
    fn heartbeat_request_reports_registration_and_applied_metadata() {
        let offline = Uuid([7; 16]);
        let cordoned = Uuid([9; 16]);
        let req = heartbeat_request(3, 41, 47, true, HeartbeatLogDirs {
            offline: vec![offline],
            cordoned: Some(vec![cordoned]),
        });

        assert!(
            req == BrokerHeartbeatRequest {
                broker_id: 3,
                broker_epoch: 41,
                current_metadata_offset: 47,
                want_fence: false,
                want_shut_down: true,
                offline_log_dirs: vec![offline],
                cordoned_log_dirs: Some(vec![cordoned]),
                ..Default::default()
            }
        );
    }

    /// Kafka's `BrokerLifecycleManager.sendBrokerHeartbeat`: the cordoned
    /// directory ids go out once the broker has caught up and only from
    /// `metadata.version` `4.3-IV0`, and an empty set is still sent.
    #[test]
    fn heartbeats_carry_cordoned_dirs_once_caught_up_on_a_supporting_version() {
        let (_a, b, paths) = two_log_dirs();
        let ids = LogDirIds::resolve(&paths);
        let wire = |dir: &Path| Uuid(*ids.id_for(dir).unwrap().as_bytes());
        let image_at = |level: Option<i16>| {
            let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
            if let Some(lvl) = level {
                let rec = krabka_metadata::FeatureLevelRecord {
                    name: METADATA_VERSION.into(),
                    level: lvl,
                };
                image.apply(&MetadataRecord::V1FeatureLevel(rec));
            }
            image
        };
        let one = CordonedLogDirs::new(paths.clone(), Some(b.path().display().to_string()));
        let none = CordonedLogDirs::new(paths, None);
        let cases = [
            ("not caught up", false, Some(30), &one, None),
            (
                "caught up",
                true,
                Some(30),
                &one,
                Some(vec![wire(b.path())]),
            ),
            ("nothing cordoned", true, Some(30), &none, Some(vec![])),
            ("below 4.3-IV0", true, Some(29), &one, None),
            ("no metadata.version", true, None, &one, None),
        ];
        for (label, caught_up, level, cordoned, want) in cases {
            check!(
                cordoned_dir_uuids(caught_up, &image_at(level), cordoned, &ids) == want,
                "{label}"
            );
        }
    }
}
