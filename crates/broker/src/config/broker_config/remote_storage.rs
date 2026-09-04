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

            /// KIP-405: deadline on one segment copy to the remote tier.
            /// Defaults to 10 minutes.
            ///
            /// The sweep copies a partition's segments one after another, so
            /// a copy that hangs -- an object store that accepts the
            /// connection and then answers nothing -- holds up every other
            /// partition on the broker behind it. Past this deadline the copy
            /// is abandoned: the segment stays in `CopySegmentStarted`, which
            /// local retention refuses to delete against, and the next tick
            /// retries it under a fresh segment id.
            ///
            /// TOML: `[remote_storage] copy_timeout = "10m"`
            pub remote_copy_timeout: Time,

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

            /// KIP-405: how many cold-tier reads may be in flight at once
            /// (Kafka's `remote.log.reader.threads`, default 10).
            ///
            /// Every remote read runs on the tokio blocking pool that WAL
            /// fsync, local fetch and replica IO also use, so without a cap a
            /// burst of cold-tier consumers starves the local paths. The
            /// broker ignores it when `remote_storage_backend` is `None`.
            ///
            /// TOML: `[remote_storage] reader_threads = 10`
            pub remote_reader_threads: usize,

            /// KIP-405: how many cold-tier reads may wait for a reader slot
            /// before the broker refuses one (Kafka's
            /// `remote.log.reader.max.pending.tasks`, default 100).
            ///
            /// A `Fetch` that arrives with the queue already full is answered
            /// with an error for that partition rather than parked, which is
            /// what Kafka's `RejectedExecutionException` path does.
            ///
            /// TOML: `[remote_storage] reader_max_pending_tasks = 100`
            pub remote_reader_max_pending_tasks: usize,

            /// KIP-405: the total-byte budget of the on-disk cache of remote
            /// segment index objects under
            /// `<log_dir>/remote-log-index-cache` (Kafka's
            /// `remote.log.index.file.cache.total.size.bytes`, default 1 GiB).
            ///
            /// Without it every `Fetch` that lands in the cold tier
            /// re-downloads the segment's `.index`, and a read-committed one
            /// its `.txnindex` as well. The broker ignores it when
            /// `remote_storage_backend` is `None`.
            ///
            /// TOML: `[remote_storage] index_cache_size = "1GiB"`
            pub remote_index_cache_size: ByteSize,
        }
    };
}
