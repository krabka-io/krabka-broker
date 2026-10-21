//! The remote archive's share of a `DeleteRecords` trim on a tiered partition.
//!
//! A tiered follower may lower an established log-start floor when its leader
//! reports a lower start, but only while the archive does not enforce the
//! floor, that is while it still holds a finished segment below it (issue
//! #1481). Remote retention deletes those segments on the leader's next pass,
//! and a leadership move before that pass would hand the partition to a
//! leader that can lower the floor again, so deleted records would become
//! readable once more. The trim therefore marks every archived segment wholly
//! below the target `DeleteSegmentStarted` before it removes anything locally,
//! and the archive enforces the floor from the moment the trim exists.

use {
    super::TrimEnv,
    crate::{
        error::BrokerError, partition::Partition, remote_log_manager::mark_segments_below,
        remote_reader::RemoteReader,
    },
    krabka_log::Offset,
    krabka_remote_storage::{RemoteStorageError, TopicIdPartition},
};

/// The remote archive behind one tiered partition, which a trim has to make
/// enforce its new floor.
pub(super) struct ArchiveFloor<'a> {
    pub(super) reader: &'a RemoteReader,
    pub(super) tp: TopicIdPartition,
    pub(super) broker_id: i32,
}

impl ArchiveFloor<'_> {
    /// Mark every archived segment wholly below `floor` as deleted. Answers
    /// how many segments this call marked.
    async fn enforce(&self, floor: Offset) -> Result<usize, RemoteStorageError> {
        mark_segments_below(
            &self.tp,
            floor,
            self.broker_id,
            &self.reader.rlmm,
            &self.reader.index_cache,
        )
        .await
    }
}

/// The archive a trim of `part` has to enforce its floor on: `None` for a
/// partition whose log does not tier, on a broker with no remote tier, or for
/// a topic the image no longer names.
pub(super) fn archive_floor<'a>(
    env: &'a TrimEnv<'_>,
    topic: &str,
    part: &Partition,
) -> Option<ArchiveFloor<'a>> {
    let tiered = part
        .log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .config_snapshot()
        .remote_storage_enable;
    if !tiered {
        return None;
    }
    let reader = env.broker.remote_reader.as_deref()?;
    let topic_id = env.image.topic(topic)?.topic_id;
    Some(ArchiveFloor {
        reader,
        tp: TopicIdPartition::new(topic_id, topic, part.index.get()),
        broker_id: env.broker.config.broker_id,
    })
}

/// Why a trim behind the archive did not happen.
#[derive(Debug, thiserror::Error)]
pub(super) enum TrimFailure {
    /// The archive could not be made to enforce the floor, so nothing was
    /// trimmed.
    #[error("the remote archive could not enforce the delete floor: {0}")]
    Archive(#[from] RemoteStorageError),
    /// The local trim failed.
    #[error("trim_to_offset failed: {0}")]
    Trim(#[from] BrokerError),
}

/// Trim `part` to `target`, with the archive enforcing the floor first.
///
/// The archive goes first, and a failure there fails the trim before anything
/// is removed, by the rule `publish_diskless_delete_floor` states: a client
/// told its records are gone must not see them again after a leadership move.
/// After the trim the archive is marked once more, best effort, which catches
/// a copy that finished between the first listing and the trim. That second
/// failure only logs, since the trim has already happened.
///
/// `archive` is `None` for a partition with no archive to enforce, which trims
/// exactly as it would without one.
///
/// # Errors
///
/// Returns [`TrimFailure::Archive`] when the first marking fails and
/// [`TrimFailure::Trim`] when the local trim does.
pub(super) async fn trim_behind_archive(
    archive: Option<&ArchiveFloor<'_>>,
    part: &Partition,
    target: Offset,
) -> Result<Offset, TrimFailure> {
    if let Some(archive) = archive {
        archive.enforce(target).await?;
    }
    let new_start = part.trim_to_offset(target).await?;
    if let Some(archive) = archive
        && let Err(error) = archive.enforce(target).await
    {
        tracing::warn!(
            topic = %archive.tp.topic, partition = archive.tp.partition, %error,
            "DeleteRecords trimmed, but could not re-mark the archive below the floor"
        );
    }
    Ok(new_start)
}
