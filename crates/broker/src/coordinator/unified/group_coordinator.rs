//! The [`GroupCoordinator`] itself: the registry struct that owns every
//! per-group actor, the [`GroupType`] lock that keeps the four group
//! namespaces apart, and the constructor and installation hooks that
//! `Broker::start` drives.
//!
//! The behaviour that operates on this state lives in the sibling modules, so
//! this file holds only the shape of the coordinator and the wiring that has
//! to exist before any of it runs.

use std::sync::Arc;

use dashmap::DashMap;

use super::{
    actor::{GroupActorHandle, MetadataProvider},
    config::NextGenConfig,
    offsets_log::OffsetsLog,
    seeds::{GroupSeed, ShareGroupSeed, StreamsGroupSeed},
    share::{actor::ShareGroupActorHandle, config::ShareGroupConfig},
    streams::{actor::StreamsGroupActorHandle, config::StreamsGroupConfig},
};

/// Locked protocol identity for a `group_id`.
///
/// Classic and next-gen actors enforce their lock through the actor's
/// [`GroupKindTag`]. Share groups from KIP-932 live in a separate
/// `share_groups` registry and record their lock here, so that the
/// classic/next-gen namespace and the share namespace cannot collide on the
/// same id.
///
/// [`GroupKindTag`]: super::actor::GroupKindTag
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupType {
    Classic,
    NextGen,
    Share,
    Streams,
}

#[derive(Debug)]
pub struct GroupCoordinator {
    pub config: Arc<NextGenConfig>,
    pub share_config: Arc<ShareGroupConfig>,
    pub metadata: Arc<dyn MetadataProvider>,
    pub offsets_log: Arc<dyn OffsetsLog>,
    pub groups: Arc<DashMap<String, Arc<GroupActorHandle>>>,
    /// Per-`group_id` share-group actor handles (KIP-932).
    pub share_groups: Arc<DashMap<String, Arc<ShareGroupActorHandle>>>,
    /// The first record persisted for a `group_id` locks its type for life.
    /// This is the classic↔next-gen↔share namespace guard.
    pub group_types: Arc<DashMap<String, GroupType>>,
    /// Bootstrap-time accumulator for next-gen state. `finalize_bootstrap`
    /// drains it.
    pub seeds: Arc<DashMap<String, GroupSeed>>,
    /// Bootstrap-time share-group accumulator. `finalize_bootstrap` drains it.
    pub share_seeds: Arc<DashMap<String, ShareGroupSeed>>,
    /// Last-known-good next-gen state per group. Every successful actor write
    /// also writes here. The coordinator seeds a fresh actor from this cache
    /// when the previous instance crashed after a log-write failure.
    pub seeds_cache: Arc<DashMap<String, GroupSeed>>,
    /// Last-known-good share-group state, the share-group analogue of
    /// `seeds_cache`.
    pub share_seeds_cache: Arc<DashMap<String, ShareGroupSeed>>,
    /// KIP-932 group-coordinator → share-state-persister bridge.
    ///
    /// `Broker::start` sets it once, after both the `ShareCoordinator` and
    /// this coordinator exist. Per-group share actors read it through
    /// [`Self::share_persister`] to drive the Initialize and Delete lifecycle
    /// calls after reconcile. It is `None` in the pure-coordinator unit tests,
    /// where the lifecycle hook does nothing.
    pub(crate) share_persister:
        std::sync::OnceLock<Arc<crate::share_coordinator::persister_client::SharePersister>>,

    // ── KIP-1071 streams groups ──────────────────────────────────────────
    pub streams_config: Arc<StreamsGroupConfig>,
    /// Per-`group_id` streams-group actor handles (KIP-1071).
    pub streams_groups: Arc<DashMap<String, Arc<StreamsGroupActorHandle>>>,
    /// Bootstrap-time streams-group accumulator. `finalize_bootstrap` drains
    /// it.
    pub streams_seeds: Arc<DashMap<String, StreamsGroupSeed>>,
    /// Last-known-good streams-group state, the streams analogue of
    /// `seeds_cache`.
    pub streams_seeds_cache: Arc<DashMap<String, StreamsGroupSeed>>,
    /// KIP-1071 metadata authority.
    ///
    /// `Broker::start` sets it once. Per-group streams actors read it through
    /// [`Self::metadata_source`] for the full `MetadataImage`, which they need
    /// for topology resolution and internal-topic creation. It is `None` in
    /// the pure-coordinator unit tests, where reconcile does nothing and
    /// returns `NotReady`.
    pub(crate) metadata_source: std::sync::OnceLock<MetadataSourceHandle>,
    /// The metric bundle whose per-group lag series this coordinator owns the
    /// lifetime of.
    ///
    /// `Broker::start` sets it once. It is `None` in the pure-coordinator unit
    /// tests, where nothing samples group lag and there is no series to
    /// release.
    pub(crate) metrics: std::sync::OnceLock<crate::metrics::BrokerMetrics>,
}

/// `Debug`-able wrapper around an `Arc<dyn MetadataSource>` so that it can
/// live in the `#[derive(Debug)]` [`GroupCoordinator`].
///
/// The trait object itself is not `Debug`. This wrapper prints an opaque
/// placeholder.
#[derive(Clone)]
pub(crate) struct MetadataSourceHandle(pub(crate) Arc<dyn crate::metadata_source::MetadataSource>);

impl std::fmt::Debug for MetadataSourceHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataSourceHandle")
            .finish_non_exhaustive()
    }
}

impl GroupCoordinator {
    pub fn new(
        config: NextGenConfig,
        share_config: ShareGroupConfig,
        metadata: Arc<dyn MetadataProvider>,
        offsets_log: Arc<dyn OffsetsLog>,
        streams_config: StreamsGroupConfig,
    ) -> Self {
        Self {
            config: Arc::new(config),
            share_config: Arc::new(share_config),
            metadata,
            offsets_log,
            groups: Arc::new(DashMap::new()),
            share_groups: Arc::new(DashMap::new()),
            group_types: Arc::new(DashMap::new()),
            seeds: Arc::new(DashMap::new()),
            share_seeds: Arc::new(DashMap::new()),
            seeds_cache: Arc::new(DashMap::new()),
            share_seeds_cache: Arc::new(DashMap::new()),
            share_persister: std::sync::OnceLock::new(),
            streams_config: Arc::new(streams_config),
            streams_groups: Arc::new(DashMap::new()),
            streams_seeds: Arc::new(DashMap::new()),
            streams_seeds_cache: Arc::new(DashMap::new()),
            metadata_source: std::sync::OnceLock::new(),
            metrics: std::sync::OnceLock::new(),
        }
    }

    /// Install the KIP-932 share-state persister bridge.
    ///
    /// `Broker::start` calls this once. A second call does nothing, because
    /// the `OnceLock` keeps the first value. Construction order therefore does
    /// not matter.
    pub(crate) fn set_share_persister(
        &self,
        persister: Arc<crate::share_coordinator::persister_client::SharePersister>,
    ) {
        let _ = self.share_persister.set(persister);
    }

    /// The installed share-state persister, if there is one.
    ///
    /// It is `None` in the unit tests that construct a bare
    /// `GroupCoordinator`. The lifecycle hook then does nothing.
    #[must_use]
    pub(crate) fn share_persister(
        &self,
    ) -> Option<&Arc<crate::share_coordinator::persister_client::SharePersister>> {
        self.share_persister.get()
    }

    /// Install the KIP-1071 metadata source.
    ///
    /// `Broker::start` calls this once. A second call does nothing, because
    /// the `OnceLock` keeps the first value.
    pub(crate) fn set_metadata_source(&self, src: Arc<dyn crate::metadata_source::MetadataSource>) {
        let _ = self.metadata_source.set(MetadataSourceHandle(src));
    }

    /// The installed metadata source, if there is one.
    ///
    /// It is `None` in the unit tests that construct a bare
    /// `GroupCoordinator`. The streams reconcile then does nothing and returns
    /// `NotReady`.
    #[must_use]
    pub(crate) fn metadata_source(
        &self,
    ) -> Option<Arc<dyn crate::metadata_source::MetadataSource>> {
        self.metadata_source.get().map(|h| h.0.clone())
    }

    /// Install the metric bundle whose group-lag series this coordinator
    /// releases.
    ///
    /// `Broker::start` calls this once. A second call does nothing, because
    /// the `OnceLock` keeps the first value.
    pub(crate) fn set_metrics(&self, metrics: crate::metrics::BrokerMetrics) {
        let _ = self.metrics.set(metrics);
    }

    /// Release every `consumer_group_lag` series for `group_id`.
    ///
    /// A group's lifetime ends in three places — `DeleteGroups`, the streams
    /// delete, and losing the offsets partition that hosts the group — and
    /// none of them is a metadata-image event the series evictor can see. Each
    /// calls this instead, so the widest family the broker emits loses its
    /// series at the moment the group stops being this broker's to report.
    pub(crate) fn forget_group_metrics(&self, group_id: &str) {
        if let Some(metrics) = self.metrics.get() {
            metrics.evict_group_series(group_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::{
        image_metadata::ImageMetadataProvider,
        test_support::{fixed_source, make_coord, make_share_persister, real_uuid},
    };

    #[test]
    fn group_type_has_share_variant() {
        // KIP-932: a third locked group type alongside Classic and NextGen.
        let t = GroupType::Share;
        check!(t == GroupType::Share);
        check!(t != GroupType::Classic);
        check!(t != GroupType::NextGen);
    }

    #[test]
    fn debug_wrappers_write_type_names() {
        let source = fixed_source(krabka_metadata::MetadataImage::new(real_uuid(1)));
        assert!(
            format!("{:?}", MetadataSourceHandle(source.clone())).contains("MetadataSourceHandle")
        );
        assert!(
            format!("{:?}", ImageMetadataProvider { controller: source })
                .contains("ImageMetadataProvider")
        );
    }

    #[test]
    fn once_lock_getters_return_installed_first_values() {
        let coord = make_coord();
        assert!(coord.metadata_source().is_none());
        assert!(coord.share_persister().is_none());

        let first_source = fixed_source(krabka_metadata::MetadataImage::new(real_uuid(1)));
        let second_source = fixed_source(krabka_metadata::MetadataImage::new(real_uuid(2)));
        coord.set_metadata_source(first_source.clone());
        coord.set_metadata_source(second_source);
        let got_source = coord.metadata_source().unwrap();
        assert!(Arc::ptr_eq(&got_source, &first_source));

        let first_persister = make_share_persister(first_source.clone());
        let second_persister = make_share_persister(first_source);
        coord.set_share_persister(first_persister.clone());
        coord.set_share_persister(second_persister);
        let got_persister = coord.share_persister().unwrap();
        assert!(Arc::ptr_eq(got_persister, &first_persister));
    }
}
