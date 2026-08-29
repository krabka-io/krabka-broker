//! KIP-405 tiered storage: which remote backend holds offloaded segments,
//! how often the `RemoteLogManager` copy and retention task runs, which
//! remote-log metadata manager serves it, and whether the object store is a
//! WORM archive.

// Link 8 of the `BrokerConfig` field chain: it adds this group to the
// fields collected so far and hands them to `audit_fields`.
macro_rules! remote_storage_fields {
    ($($collected:tt)*) => {
        audit_fields! {
            $($collected)*
            /// KIP-405: tiered-storage backend selection. `Some(_)` enables tiered
            /// storage broker-wide and spawns the `RemoteLogManager` copy task. This
            /// one field replaces Kafka's `remote.log.storage.system.enable` plus the
            /// RSM selection. `remote.storage.enable` still gates per-topic offload.
            /// `None` (default) leaves tiered storage off.
            ///
            /// TOML:
            /// - Local: `[remote_storage] storage_dir = "..."`
            /// - S3:    `[remote_storage.s3] bucket = "..." region = "..."`
            pub remote_storage_backend: Option<RemoteStorageBackend>,

            /// KIP-405: tick cadence of the `RemoteLogManager` copy /
            /// retention task. Defaults to 30s (Kafka's
            /// `remote.log.manager.task.interval.ms`). Acceptance tests lower this
            /// so segments are tiered and locally evicted in seconds rather than
            /// minutes; production deployments leave it at the default.
            pub remote_log_manager_interval: Time,

            /// KIP-405: which RLMM the broker runs when tiered storage is enabled.
            /// It defaults to [`RlmmKind::TopicBacked`] in production, and to
            /// [`RlmmKind::InMemory`] for in-process tests. The broker ignores it
            /// when `remote_storage_backend` is `None`.
            pub remote_log_metadata: RlmmKind,

            // A sibling of `remote_storage_backend`, deliberately not a
            // `RemoteStorageBackend` variant. WORM is orthogonal to which object
            // store is in use — it layers over S3 and GCS alike — and
            // `RemoteStorageBackend` is also consumed by `build_diskless_read_handle`,
            // which must not change shape. Do not "tidy" this into the enum.
            /// WORM archive mode for the tiered-storage object store (`Some`), or
            /// ordinary mutable tiered storage (`None`, the default).
            ///
            /// When set, every object the `RemoteStorageManager` writes is a
            /// conditional create, each segment gets a hash-chained and optionally
            /// Ed25519-signed integrity manifest, the backend refuses every delete,
            /// and the `RemoteLogManager`'s remote-retention pass is disabled for its
            /// partitions. Local retention is unaffected: the broker still evicts
            /// local segments once they are archived.
            ///
            /// Requires an object-store backend. `storage_dir` cannot enforce
            /// write-once.
            ///
            /// TOML: `[remote_storage.worm]`
            pub remote_storage_worm: Option<krabka_remote_storage::WormConfig>,
        }
    };
}
