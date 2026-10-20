//! What a contract asks of the cluster: the broker configuration, the topic
//! each path writes to, and the contracts this executor cannot run.
//!
//! All of it is pure, so the hermetic tests below check it without a cluster.

use {
    krabka_bench_driver::soak::contract::{Contract, LoadPath, Profile, Semantics, Storage},
    krabka_units::prelude::*,
    std::fmt::Write as _,
};

/// Where every node mounts the shared host directory that is the local object
/// store: the remote tier of a tiered path and the flushed WAL objects of a
/// diskless one.
pub(crate) const REMOTE_TIER: &str = "/var/lib/krabka-remote";

/// Segment size of a compacted topic. The default 1 GiB segment would never
/// roll in a short run, and the cleaner only compacts sealed segments.
const COMPACTED_SEGMENT_BYTES: &str = "65536";

/// Segment size and age of a tiered topic. Only sealed segments are copied to
/// the remote tier, so a segment must seal well inside the run; 4 MiB and 15s
/// keep the remote object count of a 72-hour run near a hundred thousand.
const TIERED_SEGMENT_BYTES: &str = "4194304";
const TIERED_SEGMENT_MS: &str = "15000";

/// The most nodes the fixture boots: every node is a voter, and a quorum of
/// five is the largest the CI runner's four CPUs carry under load.
const MAX_BROKERS: u32 = 5;

/// Every reason this executor cannot run `contract`, one line each.
pub(crate) fn unsupported(contract: &Contract) -> Vec<String> {
    let mut reasons = Vec::new();
    if contract.profile == Profile::DisasterRecovery {
        reasons.push(
            "a disaster_recovery contract runs in the qualification drill, under \
             krabka-soak-drill, against its secured cluster"
                .to_owned(),
        );
    }
    for path in &contract.paths {
        if (path.storage, path.semantics) == (Storage::Tiered, Semantics::Compacted) {
            reasons.push(format!(
                "path `{}` is compacted and tiered: Kafka refuses remote.storage.enable on a \
                 compacted topic",
                path.name
            ));
        }
    }
    if !(3..=MAX_BROKERS).contains(&contract.cluster.brokers) {
        reasons.push(format!(
            "the cluster has {} brokers; the soak fixture boots three to {MAX_BROKERS}",
            contract.cluster.brokers
        ));
    }
    reasons
}

/// The configuration every node adds to the fixture's file: top-level keys,
/// then whole tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeToml {
    pub(crate) root: String,
    pub(crate) tables: String,
}

/// The node configuration `contract` declares.
///
/// The check-quorum window and the replica lag are set from the contract
/// rather than left at the broker's defaults, so the limits the contract's
/// pause plans were validated against are the ones the cluster runs with.
///
/// A tiered or diskless path gets the shared local object store and the
/// topic-backed remote-log metadata manager: the remote tier keeps its segment
/// metadata there, and the WAL keeps its flush index there. A diskless path
/// also gets one WAL voter per broker. The listener and rack a diskless quorum
/// needs are per node, so the fixture writes them; see [`diskless`].
pub(crate) fn node_toml(contract: &Contract) -> NodeToml {
    let cluster = &contract.cluster;
    let mut root = format!(
        "controller_fetch_timeout = \"{}ms\"\n\
         replica_lag_time_max = \"{}ms\"\n",
        cluster.check_quorum_window.millis_i64(),
        cluster.replica_lag_time_max.millis_i64(),
    );
    if let Some(session) = cluster.broker_session_timeout {
        writeln!(root, "heartbeat_timeout = \"{}ms\"", session.millis_i64())
            .expect("write configuration into a String");
    }
    // The remote-log manager's pass runs every second rather than every
    // thirty, so a sealed segment reaches the remote tier inside a short run
    // as well as a long one.
    let remote = if object_store(contract) {
        format!(
            "remote_log_manager_interval = \"1s\"\n\
             \n\
             [remote_storage]\n\
             storage_dir = \"{REMOTE_TIER}\"\n\
             \n\
             [remote_storage.kafka_metadata]\n\
             replication = {rf}\n\
             min_isr = {min_isr}\n\
             num_partitions = 3\n",
            rf = cluster.replication_factor,
            min_isr = cluster.min_insync_replicas,
        )
    } else {
        String::new()
    };
    let wal = if diskless(contract) {
        format!("diskless_wal_local_replica_count = {}\n", cluster.brokers)
    } else {
        String::new()
    };
    let tables = format!(
        "\n[runtime]\n\
         default_min_insync_replicas = {min_isr}\n\
         offsets_topic_replication_factor = {rf}\n\
         transaction_state_replication_factor = {rf}\n\
         {wal}\
         {remote}",
        min_isr = cluster.min_insync_replicas,
        rf = cluster.replication_factor,
    );
    NodeToml { root, tables }
}

/// Whether any path of `contract` is diskless, so that the cluster has to run
/// a diskless WAL quorum.
pub(crate) fn diskless(contract: &Contract) -> bool { any_storage(contract, Storage::Diskless) }

/// Whether any path of `contract` is tiered or diskless, so that the nodes
/// share a local object store at [`REMOTE_TIER`].
pub(crate) fn object_store(contract: &Contract) -> bool {
    any_storage(contract, Storage::Tiered) || diskless(contract)
}

fn any_storage(contract: &Contract, storage: Storage) -> bool {
    contract.paths.iter().any(|path| path.storage == storage)
}

/// The topic configuration of `path`, in the order it is sent.
///
/// Every unbounded topic keeps all it is given, because reconciliation reads
/// every acknowledged record back from the earliest offset. A tiered topic
/// keeps one byte locally, so that read is served from the remote tier. A
/// diskless topic sets `krabka.diskless`, which is fixed at creation.
pub(crate) fn topic_configs(
    path: &LoadPath,
    min_insync_replicas: i16,
) -> Vec<(&'static str, String)> {
    let mut configs = vec![("min.insync.replicas", min_insync_replicas.to_string())];
    let owned = |pairs: &[(&'static str, &str)]| -> Vec<(&'static str, String)> {
        pairs
            .iter()
            .map(|(name, value)| (*name, (*value).to_owned()))
            .collect()
    };
    if path.semantics == Semantics::Compacted {
        configs.extend(owned(&[
            ("cleanup.policy", "compact"),
            ("internal.segment.bytes", COMPACTED_SEGMENT_BYTES),
            ("segment.ms", "5000"),
            ("min.cleanable.dirty.ratio", "0.01"),
            ("min.compaction.lag.ms", "0"),
        ]));
    } else {
        configs.extend(owned(&[
            ("cleanup.policy", "delete"),
            ("retention.ms", "-1"),
            ("retention.bytes", "-1"),
        ]));
    }
    if path.storage == Storage::Tiered {
        configs.extend(owned(&[
            ("remote.storage.enable", "true"),
            ("internal.segment.bytes", TIERED_SEGMENT_BYTES),
            ("segment.ms", TIERED_SEGMENT_MS),
            ("local.retention.bytes", "1"),
        ]));
    }
    if path.storage == Storage::Diskless {
        configs.extend(owned(&[("krabka.diskless", "true")]));
    }
    configs
}

#[cfg(test)]
mod tests {
    use {
        super::{REMOTE_TIER, diskless, node_toml, topic_configs, unsupported},
        crate::support::{configs::apply_node_file, manifest_dir},
        assert2::check,
        krabka_bench_driver::soak::contract::{Contract, Semantics, Storage},
        krabka_broker::config::{BrokerConfig, RemoteStorageBackend, RlmmKind},
        krabka_units::prelude::*,
    };

    fn checked_in(name: &str) -> Contract {
        let path = manifest_dir().join("../../bench/soak").join(name);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        Contract::from_yaml(&text).unwrap_or_else(|error| panic!("{name}: {error}"))
    }

    fn with_path(mut contract: Contract, storage: Storage) -> Contract {
        let mut path = contract.paths[0].clone();
        path.name = format!("{storage:?}-idempotent").to_lowercase();
        path.storage = storage;
        contract.paths.push(path);
        contract
    }

    fn with_tiered_path(contract: Contract) -> Contract { with_path(contract, Storage::Tiered) }

    /// The node file the fixture writes, with the parts the fixture owns kept
    /// to the minimum that parses.
    fn applied(contract: &Contract) -> BrokerConfig {
        let toml = node_toml(contract);
        let text = format!(
            "broker_id = 1\n{}\n[process]\nroles = [\"controller\", \"broker\"]\n{}",
            toml.root, toml.tables
        );
        apply_node_file(&text)
    }

    #[yare::parameterized(
        self_check = { "self-check.yaml" },
        soak = { "rf3-72h.yaml" },
    )]
    fn every_checked_in_contract_is_runnable(file: &str) {
        let reasons = unsupported(&checked_in(file));
        check!(reasons.is_empty(), "{reasons:?}");
    }

    #[test]
    fn a_drill_is_left_to_the_qualification_drill() {
        check!(
            unsupported(&checked_in("dr-secured.yaml"))
                == [
                    "a disaster_recovery contract runs in the qualification drill, under \
             krabka-soak-drill, against its secured cluster"
                ]
        );
    }

    #[test]
    fn a_compacted_tiered_path_is_refused() {
        let mut contract = with_tiered_path(checked_in("self-check.yaml"));
        let last = contract.paths.len() - 1;
        contract.paths[last].semantics = Semantics::Compacted;
        contract.paths[last].key_space = Some(8);
        check!(
            unsupported(&contract)
                == [
                    "path `tiered-idempotent` is compacted and tiered: Kafka refuses remote.storage.enable \
             on a compacted topic"
                ]
        );
    }

    // `OpaAuthorizer::new` captures the current tokio runtime handle, so the
    // apply step runs inside one, as it does in the binary.
    #[tokio::test]
    async fn the_node_file_carries_the_contracts_cluster_limits() {
        for file in ["self-check.yaml", "rf3-72h.yaml"] {
            let contract = checked_in(file);
            let config = applied(&contract);
            check!(config.controller_fetch_timeout == contract.cluster.check_quorum_window);
            check!(
                config.heartbeat_timeout
                    == contract.cluster.broker_session_timeout.unwrap_or(secs(9))
            );
            check!(config.replica_lag_time_max == contract.cluster.replica_lag_time_max);
            check!(config.default_min_insync_replicas == 2);
            check!(config.offsets_topic_replication_factor == 3);
            check!(config.transaction_state_replication_factor == 3);
        }
        check!(
            applied(&checked_in("self-check.yaml"))
                .remote_storage_backend
                .is_none()
        );
    }

    #[yare::parameterized(
        tiered = { Storage::Tiered },
        diskless = { Storage::Diskless },
    )]
    #[test_macro(tokio::test)]
    async fn an_object_store_contract_shares_one_local_store(storage: Storage) {
        let config = applied(&with_path(checked_in("self-check.yaml"), storage));
        check!(matches!(
            &config.remote_storage_backend,
            Some(RemoteStorageBackend::Local { dir }) if dir.as_path() == std::path::Path::new(REMOTE_TIER)
        ));
        check!(config.remote_log_manager_interval == secs(1));
        let RlmmKind::TopicBacked(rlmm) = &config.remote_log_metadata else {
            panic!(
                "expected the topic-backed RLMM: {:?}",
                config.remote_log_metadata
            );
        };
        check!(rlmm.replication == 3);
        check!(rlmm.num_partitions == 3);
    }

    #[yare::parameterized(
        classic = { "self-check.yaml", false },
        soak = { "rf3-72h.yaml", true },
    )]
    fn only_a_diskless_contract_runs_a_wal_quorum(file: &str, quorum: bool) {
        check!(diskless(&checked_in(file)) == quorum);
    }

    #[tokio::test]
    async fn a_diskless_contract_votes_every_broker_into_the_wal() {
        let contract = checked_in("rf3-72h.yaml");
        let config = applied(&contract);
        check!(config.diskless_wal_local_replica_count == 3);
    }

    #[yare::parameterized(
        idempotent = { 0, Storage::Classic, &[
            ("min.insync.replicas", "2"),
            ("cleanup.policy", "delete"),
            ("retention.ms", "-1"),
            ("retention.bytes", "-1"),
        ] },
        compacted = { 2, Storage::Classic, &[
            ("min.insync.replicas", "2"),
            ("cleanup.policy", "compact"),
            ("internal.segment.bytes", "65536"),
            ("segment.ms", "5000"),
            ("min.cleanable.dirty.ratio", "0.01"),
            ("min.compaction.lag.ms", "0"),
        ] },
        tiered = { 0, Storage::Tiered, &[
            ("min.insync.replicas", "2"),
            ("cleanup.policy", "delete"),
            ("retention.ms", "-1"),
            ("retention.bytes", "-1"),
            ("remote.storage.enable", "true"),
            ("internal.segment.bytes", "4194304"),
            ("segment.ms", "15000"),
            ("local.retention.bytes", "1"),
        ] },
        diskless = { 0, Storage::Diskless, &[
            ("min.insync.replicas", "2"),
            ("cleanup.policy", "delete"),
            ("retention.ms", "-1"),
            ("retention.bytes", "-1"),
            ("krabka.diskless", "true"),
        ] },
    )]
    fn each_path_gets_the_topic_its_semantics_need(
        index: usize,
        storage: Storage,
        expected: &[(&str, &str)],
    ) {
        let contract = checked_in("self-check.yaml");
        let mut path = contract.paths[index].clone();
        path.storage = storage;
        let configs = topic_configs(&path, contract.cluster.min_insync_replicas);
        let configs: Vec<(&str, &str)> = configs
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .collect();
        check!(configs == expected);
    }
}
