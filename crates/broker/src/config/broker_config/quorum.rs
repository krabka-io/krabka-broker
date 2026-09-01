//! This node's place in the `KRaft` metadata quorum: its raft identity, the
//! voter set and the endpoints it dials, election and heartbeat timing, the
//! metadata snapshot policy, and the rack and stretch site it reports to the
//! controller.

// Link 3 of the `BrokerConfig` field chain: it adds this group to the
// fields collected so far and hands them to `security_fields`.
macro_rules! quorum_fields {
    ($($collected:tt)*) => {
        security_fields! {
            $($collected)*
            /// Raft node id. Conventionally equal to `broker_id as NodeId`.
            pub node_id: NodeId,

            /// Address the controller listener binds on. `KRaft` convention: same
            /// host as `listen_addr`, port 9093. Test default: `127.0.0.1:0`.
            pub controller_listen_addr: SocketAddr,

            /// Static voter set: `[(node_id, "<host>:<port>"), …]`. The address is the
            /// peer controller listener's `<host>:<port>`, carried verbatim and NOT
            /// pre-resolved. The dialer resolves the host again on every connect and
            /// reconnect, so a peer that restarts on a new pod IP stays reachable.
            /// Defaults to a single-voter cluster of just this broker, so
            /// single-broker setups get a quorum of one without config changes.
            pub controller_quorum_voters: Vec<(NodeId, String)>,

            /// TLS server name (SNI) presented when dialing a peer's controller
            /// listener for the KIP-595 quorum. Set to a SAN shared by every
            /// broker's serving cert, the headless-Service FQDN, so mTLS validates
            /// whichever peer the broker dials, even a pod IP. `None` falls back to
            /// `"localhost"`.
            pub controller_server_name: Option<String>,

            /// KIP-853 dynamic quorum: controller endpoints used only to discover
            /// the leader at cold start (the joiner path). Empty for a standalone
            /// bootstrap node. Maps to Kafka's `controller.quorum.bootstrap.servers`.
            pub bootstrap_servers: Vec<String>,

            /// KIP-853: this replica's stable directory id, recovered from
            /// `meta.properties.json` at boot. Identifies which voter this node *is*.
            pub directory_id: uuid::Uuid,

            /// UUID for this broker process invocation. The broker keeps it in
            /// `{log_dir}/incarnation_id` and reloads it on restart. The internal
            /// `load_or_generate` helper sets it before self-registration. Tests
            /// generate a random UUID for each call through [`Self::for_tests`].
            pub incarnation_id: uuid::Uuid,

            /// KIP-966 clean-shutdown proof: the broker epoch this node held
            /// when it last stopped gracefully, recovered from
            /// `{log_dir}/clean_shutdown` at boot and offered back at
            /// registration. Kafka carries the same value as
            /// `BrokerRegistrationRequest.previousBrokerEpoch`. It is
            /// [`crate::clean_shutdown::UNPROVEN`] when this node cannot prove
            /// it stopped gracefully, which is what a crash leaves behind and
            /// what a first-ever boot has.
            pub previous_broker_epoch: i64,

            /// KIP-853: when true, an observer issues `AddVoter` for itself once it
            /// has caught up to the leader. The observer joins the quorum without
            /// operator action. Maps to Kafka's `controller.quorum.auto.join.enable`.
            pub auto_join: bool,

            /// KIP-853: maximum log-entry lag an observer may have and still be
            /// promotable to a voter. The broker forwards it to `ControllerConfig`.
            pub observer_lag_bound: u64,

            /// How often each broker sends `BrokerHeartbeat` to the controller
            /// leader. Default 3s.
            pub heartbeat_interval: Time,
            /// Controller marks a broker dead after this long without a
            /// heartbeat. Default 9s.
            pub heartbeat_timeout: Time,
            /// Leader proposes ISR shrink when a follower lags more than this.
            /// Default 30s.
            pub replica_lag_time_max: Time,

            /// Openraft election timeout. It sets `election_timeout_min`, and the
            /// maximum is 2×. It also sets `leader_lease = election_timeout_max`
            /// inside openraft's engine. Peers refuse to grant a new leader's vote
            /// until the lease expires, so this value is also the lower bound on how
            /// fast a 3-broker cluster recovers from a dead controller leader.
            /// Default 5s. The default is conservative and avoids a split vote on
            /// slow runners.
            pub controller_election_timeout: Time,

            /// Openraft heartbeat interval. Default 500ms. It should be ≤
            /// `controller_election_timeout / 3` by raft consensus norms.
            pub controller_heartbeat_interval: Time,
            /// Whether the heartbeat interval was explicitly configured. Omitted
            /// values preserve the Raft engine's election-timeout-derived cadence.
            pub controller_heartbeat_interval_explicit: bool,
            /// Consecutive follower fetch misses tolerated before a new election.
            pub controller_fetch_miss_limit: ControllerFetchMissLimit,
            /// Capacity of the metadata Raft engine command queue.
            pub metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity,
            /// Per-read and per-snapshot-request metadata Raft byte budget.
            pub metadata_raft_fetch_max: MetadataRaftFetchMax,

            /// `metadata.log.max.record.bytes.between.snapshots` (default 20 MiB).
            pub metadata_max_bytes_between_snapshots: ByteSize,

            /// `metadata.log.max.snapshot.interval.ms` (default 1 h; zero = disabled).
            pub metadata_max_snapshot_interval: Time,

            /// KIP-630: snapshot the metadata log once committed offset advances this
            /// many records past the last snapshot, then prune below it.
            pub metadata_snapshot_interval_records: u64,

            /// Maximum metadata snapshot size a follower will fetch. The core enforces
            /// an immutable 1 GiB security ceiling.
            pub metadata_snapshot_fetch_max: ByteSize,

            /// How this broker takes part in cluster formation. See
            /// [`krabka_raft::BootstrapMode`] for the trade-offs. The first broker of
            /// a fresh multi-broker cluster uses `Bootstrap`. Later brokers use
            /// `Join`. A restart of any previously-formatted broker uses `Rejoin`.
            /// Single-broker setups always use `Bootstrap`.
            pub bootstrap_mode: BootstrapMode,

            /// Cluster UUID that the broker forwards to
            /// `ControllerConfig::cluster_id`. The operator supplies it as the
            /// `KafkaCluster` UID through `--cluster-id`. `None` defaults to
            /// `Uuid::nil()` inside `Controller::start`.
            pub cluster_id: Option<uuid::Uuid>,

            /// KIP-392: this broker's rack identifier (`broker.rack`). The broker
            /// reports it in its `BrokerRegistrationRecord`, and the leader's
            /// rack-aware replica selector reads it. `None` (default) means no rack.
            pub rack: Option<String>,

            /// KIP-392: which replica selector the leader runs to populate
            /// `FetchResponse.preferred_read_replica` for rack-aware consumers.
            /// Default `Leader` (never redirect).
            pub replica_selector: crate::replica_selector::ReplicaSelectorKind,

            /// The three-site stretch deployment this node belongs to. `None`
            /// (default) is an ordinary, non-stretched cluster. When it is `Some`,
            /// [`rack`][Self::rack] must name one of the profile's sites, and
            /// [`validate`][Self::validate] checks that the roles of this node agree
            /// with that site.
            pub stretch: Option<StretchProfile>,
        }
    };
}
