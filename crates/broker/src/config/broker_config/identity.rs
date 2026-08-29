//! Which node this is and where its data lives: the broker id and the roles
//! it registers with the controller, the endpoint it advertises to clients,
//! and the directories that hold the metadata log and the partition logs.

// Link 2 of the `BrokerConfig` field chain: it appends this group to
// the fields collected so far and forwards them to `quorum_fields`.
macro_rules! identity_fields {
    ($($collected:tt)*) => {
        $crate::config::broker_config::quorum::quorum_fields! {
            $($collected)*
            /// Broker id reported in `Metadata` responses. Default: 1.
            pub broker_id: i32,

            /// `KRaft` `process.roles`. It controls whether this node is a metadata
            /// quorum voter (`Controller`), hosts data partitions and registers as a
            /// broker (`Broker`), or both. Default: `[Controller, Broker]`.
            pub roles: Vec<NodeRole>,

            /// TCP address to listen on. Default: `127.0.0.1:9092`.
            pub listen_addr: SocketAddr,

            /// `host:port` returned in `Metadata` responses as this broker's
            /// advertised endpoint. Defaults to `listen_addr`'s string form.
            pub advertised_listener: String,

            /// Primary log directory. It holds the `__cluster_metadata` raft log, and
            /// the broker reads it to detect bootstrap mode. It is also a data
            /// directory: when [`extra_log_dirs`][Self::extra_log_dirs] is empty,
            /// partition data lives only here. The broker creates the directory on
            /// startup if it is missing. Default: `./krabka-data`.
            pub log_dir: PathBuf,

            /// Extra JBOD data directories (KIP-113). When this list is non-empty,
            /// the broker spreads new partitions across `[log_dir] + extra_log_dirs`
            /// by least-loaded placement. `__cluster_metadata` always stays on
            /// [`log_dir`][Self::log_dir]. Maps to a Kafka `log.dirs` value with more
            /// than one entry. Default: empty, which gives a single-directory broker.
            pub extra_log_dirs: Vec<PathBuf>,

            /// Per-log configuration applied to every partition this broker hosts.
            pub log_config: LogConfig,

            /// Optional internal timestamp source shared by every hosted partition.
            ///
            /// `None` is the Kafka-only default: no `.stampindex` sidecar is opened and
            /// record bytes, offsets, LSO, and high-watermark behavior stay unchanged.
            /// A combined SQL/Kafka runtime injects its tenant timestamp source here.
            pub stamp_source: Option<Arc<dyn krabka_log::StampSource>>,
        }
    };
}

pub(crate) use identity_fields;
