//! The share-session lifecycle: the fetch and acknowledge epoch bumps, and the
//! release path that a member's disconnect takes.
//!
//! Session bookkeeping is a thin pass-through to [`ShareSessionCache`], but the
//! release path is not: a closing session gives its acquired records back to
//! the group, which is the one place session state reaches into the
//! acquisition machines. The two sit together because a disconnect runs both.

use std::collections::HashSet;

use super::SharePartitionLeaderManager;
use crate::share_partition::session::{ShareFetchSessionUpdate, SharePartitionKey};

impl SharePartitionLeaderManager {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn update_fetch_session(
        &self,
        group: &str,
        member: &str,
        connection_id: &str,
        epoch: i32,
        requested: &HashSet<SharePartitionKey>,
        forgotten: &HashSet<SharePartitionKey>,
        has_acknowledgements: bool,
        final_has_additions: bool,
    ) -> Result<ShareFetchSessionUpdate, i16> {
        self.sessions.update_fetch(
            group,
            member,
            connection_id,
            epoch,
            requested,
            forgotten,
            has_acknowledgements,
            final_has_additions,
        )
    }

    pub(crate) fn update_acknowledge_session(
        &self,
        group: &str,
        member: &str,
        epoch: i32,
    ) -> Result<HashSet<SharePartitionKey>, i16> {
        self.sessions.update_acknowledge(group, member, epoch)
    }

    /// Releases all acquisitions owned by `member` in the supplied session
    /// partitions. Only already-loaded cells can contain live acquisitions;
    /// durable state stores acquired records as available.
    pub(crate) async fn release_session_partitions(
        &self,
        group: &str,
        member: &str,
        partitions: &HashSet<SharePartitionKey>,
    ) {
        for &(topic_id, partition) in partitions {
            let cell = self
                .leaders
                .get(&(group.to_string(), topic_id, partition))
                .map(|entry| entry.value().clone());
            let Some(cell) = cell else {
                continue;
            };
            let mut state = cell.lock().await;
            state.release_member(member);
            self.persist_if_dirty(group, topic_id, partition, &mut state)
                .await;
        }
    }

    /// Closes the share session tied to a disconnected client and releases
    /// its outstanding acquisitions.
    pub(crate) async fn release_connection(&self, connection_id: &str) {
        let Some(session) = self.sessions.disconnect(connection_id) else {
            return;
        };
        self.release_session_partitions(&session.group, &session.member, &session.partitions)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use assert2::assert;
    use krabka_log::Offset;

    use crate::share_partition::manager::test_support::{
        LOCK, manager, manager_with_unlimited_fallback,
    };

    #[test]
    fn nondefault_unlimited_fallback_bounds_sessions() {
        let manager = manager_with_unlimited_fallback(2);
        let partitions = HashSet::new();

        assert!(
            manager
                .update_fetch_session(
                    "g",
                    "m1",
                    "connection-1",
                    0,
                    &partitions,
                    &partitions,
                    false,
                    false,
                )
                .is_ok()
        );
        assert!(
            manager
                .update_fetch_session(
                    "g",
                    "m2",
                    "connection-2",
                    0,
                    &partitions,
                    &partitions,
                    false,
                    false,
                )
                .is_ok()
        );
        assert!(
            manager.update_fetch_session(
                "g",
                "m3",
                "connection-3",
                0,
                &partitions,
                &partitions,
                false,
                false,
            ) == Err(crate::codes::SHARE_SESSION_LIMIT_REACHED)
        );
    }

    #[tokio::test]
    async fn disconnect_releases_the_sessions_acquired_records() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([27; 16]);
        let cell = mgr.get_or_load("g1", tid, 0).await;
        {
            let mut state = cell.lock().await;
            state.materialize(Offset(1), 100);
            let acquired = state.acquire("m1", 1, i32::MAX, std::time::Instant::now(), LOCK, 5);
            assert!(acquired.len() == 1);
        }
        let partitions = HashSet::from([(tid, 0)]);
        mgr.update_fetch_session(
            "g1",
            "m1",
            "connection-1",
            0,
            &partitions,
            &HashSet::new(),
            false,
            false,
        )
        .expect("open session");

        mgr.release_connection("connection-1").await;

        let mut state = cell.lock().await;
        let redelivered = state.acquire("m2", 1, i32::MAX, std::time::Instant::now(), LOCK, 5);
        assert!(redelivered.len() == 1);
        assert!(redelivered[0].delivery_count == 2);
    }
}
