//! KIP-932 share-session cache.
//!
//! A share session tracks the incremental `ShareFetch`/`ShareAcknowledge`
//! conversation between one consumer (`(group, member)`) and the
//! share-partition leader. The `share_session_epoch` on each request drives a
//! small state machine, Kafka's `SharePartitionManager.newContext`:
//!
//! - epoch `0` opens a session, and the stored epoch becomes `1`. It replaces
//!   a live session of the same member without releasing that member's
//!   records: they stay acquired until the member acknowledges them or their
//!   locks run out.
//! - epoch `-1` (`FINAL_EPOCH`) closes the session, and the cache removes the
//!   entry.
//! - any other epoch must match the stored epoch exactly. The cache then bumps
//!   the stored epoch for the next request.
//!
//! Mismatches map to Kafka's share-session error codes:
//! `INVALID_SHARE_SESSION_EPOCH` for a stale or ahead epoch,
//! `SHARE_SESSION_NOT_FOUND` for a non-zero epoch with no live session, and
//! `SHARE_SESSION_LIMIT_REACHED` when the cache is full.
//!
//! A session keeps its partitions in order, with Kafka's
//! `CachedSharePartition.requiresUpdateInResponse` flag on each, so that an
//! incremental response leaves out a partition that has nothing new to say.
//!
//! Locking discipline: each cache operation releases the mutex before it
//! returns, and this module holds nothing across an `.await`.

use {
    crate::codes,
    std::{
        collections::{HashMap, HashSet, hash_map::Entry},
        sync::Mutex,
    },
};

/// Epoch value a client sends to close its share session (Kafka's
/// `ShareRequestMetadata.FINAL_EPOCH`).
const FINAL_EPOCH: i32 = -1;
/// Epoch value a client sends to open a fresh share session.
const INITIAL_EPOCH: i32 = 0;

/// One share partition of a session: `(topic_id, partition)`.
pub(crate) type SharePartitionKey = (uuid::Uuid, i32);

/// Inputs to one share-fetch session transition.
#[derive(Clone, Copy, bon::Builder)]
pub(crate) struct ShareFetchSessionRequest<'a> {
    identity: (&'a str, &'a str),
    connection_id: &'a str,
    epoch: i32,
    partitions: FetchPartitions<'a>,
    #[builder(default)]
    has_acknowledgements: bool,
}

/// One partition of a live session, Kafka's `CachedSharePartition`.
#[derive(Debug, Clone, Copy)]
struct CachedPartition {
    /// The next incremental response must carry this partition: it was just
    /// added, or its last row carried an error.
    requires_update: bool,
}

/// One live share session. It holds the current epoch and the share
/// partitions that the member has in its session, in Kafka's order: an
/// initial fetch sets them in request order, an incremental fetch appends the
/// new ones, and a partition that returned records moves to the end.
#[derive(Debug)]
struct ShareSession {
    epoch: i32,
    partitions: Vec<SharePartitionKey>,
    cached: HashMap<SharePartitionKey, CachedPartition>,
    connection_id: String,
}

/// What a `ShareFetch` does after the share-session update.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ShareFetchSessionUpdate {
    /// The partitions to fetch, in the session's order. A final request
    /// fetches nothing.
    pub(crate) partitions: Vec<SharePartitionKey>,
    /// Partitions whose outstanding acquisitions the member gives back
    /// because its final request closed the session.
    pub(crate) released: HashSet<SharePartitionKey>,
    pub(crate) final_request: bool,
    /// The request continued a session, so the response carries only the
    /// partitions that have something to say.
    pub(crate) incremental: bool,
}

/// One partition row of an incremental response, as
/// [`ShareSessionCache::select_response`] judges it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResponseRow {
    pub(crate) key: SharePartitionKey,
    pub(crate) has_records: bool,
    pub(crate) has_error: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ClosedShareSession {
    pub(crate) group: String,
    pub(crate) member: String,
    pub(crate) partitions: HashSet<SharePartitionKey>,
}

/// The group and member that own a session.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SessionKey {
    group: String,
    member: String,
}

impl SessionKey {
    fn new(group: &str, member: &str) -> Self {
        Self {
            group: group.to_string(),
            member: member.to_string(),
        }
    }
}

#[derive(Debug, Default)]
struct Inner {
    sessions: HashMap<SessionKey, ShareSession>,
    connections: HashMap<String, SessionKey>,
}

/// The share partitions of one `ShareFetch`: the ones it names, in request
/// order, and the ones it forgets.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FetchPartitions<'a> {
    pub(crate) requested: &'a [SharePartitionKey],
    pub(crate) forgotten: &'a HashSet<SharePartitionKey>,
}

/// Process-wide cache of live share sessions keyed by `(group, member)`.
#[derive(Debug)]
pub(crate) struct ShareSessionCache {
    inner: Mutex<Inner>,
    max: usize,
}

impl ShareSessionCache {
    /// Create a cache that holds at most `max` concurrent sessions.
    pub(crate) fn new(max: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            max,
        }
    }

    /// Kafka's `SharePartitionManager.newContext` for a `ShareFetch`.
    ///
    /// A final request needs a live session, closes it, and fetches nothing,
    /// whatever partitions it names or forgets. An initial request refuses
    /// acknowledgements with `INVALID_REQUEST`, replaces a live session of
    /// the member without releasing its records, ignores the forgotten
    /// partitions, and answers `SHARE_SESSION_LIMIT_REACHED` when the cache
    /// is full. An incremental request adds the named partitions that the
    /// session does not hold yet and removes the forgotten ones.
    pub(crate) fn update_fetch(
        &self,
        request: ShareFetchSessionRequest<'_>,
    ) -> Result<ShareFetchSessionUpdate, i16> {
        let ShareFetchSessionRequest {
            identity: (group, member),
            connection_id,
            epoch,
            partitions,
            has_acknowledgements,
        } = request;
        let key = SessionKey::new(group, member);
        let mut inner = self.inner.lock().expect("share-session mutex poisoned");

        if epoch == FINAL_EPOCH {
            let session = remove_session(&mut inner, &key).ok_or(codes::SHARE_SESSION_NOT_FOUND)?;
            return Ok(ShareFetchSessionUpdate {
                partitions: Vec::new(),
                released: session.partitions.iter().copied().collect(),
                final_request: true,
                incremental: false,
            });
        }

        if epoch == INITIAL_EPOCH {
            if has_acknowledgements {
                return Err(codes::INVALID_REQUEST);
            }
            remove_session(&mut inner, &key);
            if inner.sessions.len() >= self.max {
                return Err(codes::SHARE_SESSION_LIMIT_REACHED);
            }
            let mut ordered = Vec::with_capacity(partitions.requested.len());
            let mut cached = HashMap::with_capacity(partitions.requested.len());
            for requested in partitions.requested {
                if let Entry::Vacant(entry) = cached.entry(*requested) {
                    entry.insert(CachedPartition {
                        requires_update: false,
                    });
                    ordered.push(*requested);
                }
            }
            let fetch = ordered.clone();
            inner
                .connections
                .insert(connection_id.to_string(), key.clone());
            inner.sessions.insert(key, ShareSession {
                epoch: 1,
                partitions: ordered,
                cached,
                connection_id: connection_id.to_string(),
            });
            return Ok(ShareFetchSessionUpdate {
                partitions: fetch,
                released: HashSet::new(),
                final_request: false,
                incremental: false,
            });
        }

        let session = session_at_epoch(&mut inner, &key, epoch)?;
        for requested in partitions.requested {
            if let Entry::Vacant(entry) = session.cached.entry(*requested) {
                entry.insert(CachedPartition {
                    requires_update: true,
                });
                session.partitions.push(*requested);
            }
        }
        session
            .partitions
            .retain(|key| !partitions.forgotten.contains(key));
        for forgotten in partitions.forgotten {
            session.cached.remove(forgotten);
        }
        session.epoch = next_epoch(session.epoch);
        Ok(ShareFetchSessionUpdate {
            partitions: session.partitions.clone(),
            released: HashSet::new(),
            final_request: false,
            incremental: true,
        })
    }

    /// Which rows of an incremental response the response carries, without
    /// changing the session: the selection Kafka's
    /// `ShareSessionContext.responseSize` sizes a response by before it knows
    /// whether a quota throttles it.
    ///
    /// A row with records or an error is always carried, and so is a
    /// partition that the session flagged. A row of a partition that the
    /// session does not hold is carried.
    pub(crate) fn select_response(
        &self,
        group: &str,
        member: &str,
        rows: &[ResponseRow],
    ) -> Vec<bool> {
        let key = SessionKey::new(group, member);
        let inner = self.inner.lock().expect("share-session mutex poisoned");
        let Some(session) = inner.sessions.get(&key) else {
            return vec![true; rows.len()];
        };
        rows.iter()
            .map(|row| {
                session
                    .cached
                    .get(&row.key)
                    .is_none_or(|cached| row.has_records || row.has_error || cached.requires_update)
            })
            .collect()
    }

    /// Kafka's `ShareSessionContext.updateAndGenerateResponseData` for an
    /// incremental response that goes out: updates the session for the rows
    /// that [`Self::select_response`] judged. A throttled response, which
    /// carries no rows, leaves the session as it was.
    ///
    /// An error flags the partition for the next response too, so the client
    /// learns when it clears. A partition that returned records moves to the
    /// end of the session's order.
    pub(crate) fn commit_response(&self, group: &str, member: &str, rows: &[ResponseRow]) {
        let key = SessionKey::new(group, member);
        let mut inner = self.inner.lock().expect("share-session mutex poisoned");
        let Some(session) = inner.sessions.get_mut(&key) else {
            return;
        };
        for row in rows {
            if let Some(cached) = session.cached.get_mut(&row.key) {
                cached.requires_update = row.has_error;
            }
        }

        // Repeatedly removing record-bearing partitions from the middle of
        // the vector makes a wide response quadratic. The original operation
        // leaves them at the end in last-record-occurrence order, which these
        // bounded passes reproduce even when `rows` contains duplicates.
        let mut moved = HashSet::new();
        let mut moved_order = Vec::new();
        for row in rows.iter().rev() {
            if row.has_records && session.cached.contains_key(&row.key) && moved.insert(row.key) {
                moved_order.push(row.key);
            }
        }
        moved_order.reverse();
        session.partitions.retain(|key| !moved.contains(key));
        session.partitions.extend(moved_order);
    }

    /// Validate a `ShareAcknowledge` epoch and advance or close its session.
    pub(crate) fn update_acknowledge(
        &self,
        group: &str,
        member: &str,
        epoch: i32,
    ) -> Result<HashSet<SharePartitionKey>, i16> {
        if epoch == INITIAL_EPOCH {
            return Err(codes::INVALID_SHARE_SESSION_EPOCH);
        }
        let key = SessionKey::new(group, member);
        let mut inner = self.inner.lock().expect("share-session mutex poisoned");
        if epoch == FINAL_EPOCH {
            return remove_session(&mut inner, &key)
                .map(|session| session.partitions.iter().copied().collect())
                .ok_or(codes::SHARE_SESSION_NOT_FOUND);
        }
        let session = session_at_epoch(&mut inner, &key, epoch)?;
        session.epoch = next_epoch(session.epoch);
        Ok(HashSet::new())
    }

    /// Removes every session, as Kafka's `ShareSessionCache.removeAllSessions`.
    pub(crate) fn clear(&self) {
        let mut inner = self.inner.lock().expect("share-session mutex poisoned");
        inner.sessions.clear();
        inner.connections.clear();
    }

    /// Remove the session owned by a closing client connection.
    pub(crate) fn disconnect(&self, connection_id: &str) -> Option<ClosedShareSession> {
        let mut inner = self.inner.lock().expect("share-session mutex poisoned");
        let key = inner.connections.remove(connection_id)?;
        let session = inner.sessions.get(&key)?;
        if session.connection_id != connection_id {
            return None;
        }
        let session = inner.sessions.remove(&key)?;
        Some(ClosedShareSession {
            group: key.group,
            member: key.member,
            partitions: session.partitions.iter().copied().collect(),
        })
    }
}

fn remove_session(inner: &mut Inner, key: &SessionKey) -> Option<ShareSession> {
    let session = inner.sessions.remove(key)?;
    if inner.connections.get(&session.connection_id) == Some(key) {
        inner.connections.remove(&session.connection_id);
    }
    Some(session)
}

/// Find a session and validate its epoch while the caller holds the cache lock.
fn session_at_epoch<'a>(
    inner: &'a mut Inner,
    key: &SessionKey,
    epoch: i32,
) -> Result<&'a mut ShareSession, i16> {
    let session = inner
        .sessions
        .get_mut(key)
        .ok_or(codes::SHARE_SESSION_NOT_FOUND)?;
    if session.epoch != epoch {
        return Err(codes::INVALID_SHARE_SESSION_EPOCH);
    }
    Ok(session)
}

fn next_epoch(epoch: i32) -> i32 { epoch.checked_add(1).unwrap_or(1) }

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        std::{sync::Arc, thread},
    };

    fn partition(id: u8, partition: i32) -> SharePartitionKey {
        (uuid::Uuid::from_bytes([id; 16]), partition)
    }

    fn set(partitions: &[SharePartitionKey]) -> HashSet<SharePartitionKey> {
        partitions.iter().copied().collect()
    }

    fn fetch(
        cache: &ShareSessionCache,
        member: &str,
        epoch: i32,
        requested: &[SharePartitionKey],
        forgotten: &[SharePartitionKey],
    ) -> Result<ShareFetchSessionUpdate, i16> {
        fetch_on(cache, member, "connection-a", epoch, requested, forgotten)
    }

    fn fetch_on(
        cache: &ShareSessionCache,
        member: &str,
        connection: &str,
        epoch: i32,
        requested: &[SharePartitionKey],
        forgotten: &[SharePartitionKey],
    ) -> Result<ShareFetchSessionUpdate, i16> {
        cache.update_fetch(
            ShareFetchSessionRequest::builder()
                .identity(("g", member))
                .connection_id(connection)
                .epoch(epoch)
                .partitions(FetchPartitions {
                    requested,
                    forgotten: &set(forgotten),
                })
                .build(),
        )
    }

    #[test]
    fn open_then_incremental_merges_forgets_and_advances() {
        let cache = ShareSessionCache::new(8);
        let p0 = partition(1, 0);
        let p1 = partition(1, 1);
        let p2 = partition(2, 0);

        let opened = fetch(&cache, "m", 0, &[p1, p0], &[]).expect("open");
        assert!(opened.partitions == vec![p1, p0]);
        assert!(!opened.incremental);

        let updated = fetch(&cache, "m", 1, &[p2], &[p0]).expect("incremental");
        assert!(updated.partitions == vec![p1, p2]);
        assert!(updated.incremental);
        assert!(fetch(&cache, "m", 2, &[], &[]).is_ok());
    }

    #[test]
    fn membership_keeps_first_seen_order_and_existing_state() {
        let cache = ShareSessionCache::new(8);
        let (p0, p1, p2, p3) = (
            partition(1, 0),
            partition(1, 1),
            partition(1, 2),
            partition(1, 3),
        );
        let row = |key, has_error| ResponseRow {
            key,
            has_records: false,
            has_error,
        };

        let opened = fetch(&cache, "m", 0, &[p1, p0, p1, p0], &[]).expect("open");
        assert!(opened.partitions == vec![p1, p0]);
        cache.commit_response("g", "m", &[row(p1, true)]);

        // Existing and repeated partitions do not move or reset their cached
        // state. New partitions append on their first occurrence, before the
        // forgotten set is applied.
        let updated = fetch(&cache, "m", 1, &[p0, p2, p2, p1, p3], &[p0, p3]).expect("incremental");
        assert!(updated.partitions == vec![p1, p2]);
        assert!(
            cache.select_response("g", "m", &[row(p1, false), row(p2, false)]) == vec![true, true]
        );
    }

    #[test]
    fn stale_epoch_is_invalid() {
        let cache = ShareSessionCache::new(8);
        assert!(fetch(&cache, "m", 0, &[], &[]).is_ok());
        // Stored epoch is 1; sending the wrong epoch is rejected.
        assert!(fetch(&cache, "m", 5, &[], &[]) == Err(codes::INVALID_SHARE_SESSION_EPOCH));
    }

    #[test]
    fn unknown_member_non_zero_epoch_not_found() {
        let cache = ShareSessionCache::new(8);
        assert!(fetch(&cache, "ghost", 3, &[], &[]) == Err(codes::SHARE_SESSION_NOT_FOUND));
    }

    /// Kafka's `FinalContext` accepts partitions to add and to forget, and
    /// fetches none of them.
    #[test]
    fn close_removes_session_whatever_the_request_names() {
        let p = partition(1, 0);
        let other = partition(2, 0);
        for (requested, forgotten) in [
            (Vec::new(), Vec::new()),
            (vec![other], Vec::new()),
            (Vec::new(), vec![p]),
        ] {
            let cache = ShareSessionCache::new(8);
            assert!(fetch(&cache, "m", 0, &[p], &[]).is_ok());
            let closed = fetch(&cache, "m", -1, &requested, &forgotten).expect("close");
            assert!(
                closed
                    == ShareFetchSessionUpdate {
                        partitions: Vec::new(),
                        released: set(&[p]),
                        final_request: true,
                        incremental: false,
                    }
            );
            assert!(fetch(&cache, "m", 1, &[], &[]) == Err(codes::SHARE_SESSION_NOT_FOUND));
        }
    }

    #[test]
    fn close_absent_session_is_not_found() {
        let cache = ShareSessionCache::new(8);
        assert!(fetch(&cache, "never", -1, &[], &[]) == Err(codes::SHARE_SESSION_NOT_FOUND));
    }

    /// Kafka refuses an initial fetch with acknowledgements and ignores its
    /// forgotten partitions.
    #[test]
    fn initial_fetch_refuses_acknowledgements_and_ignores_forgotten_partitions() {
        let cache = ShareSessionCache::new(8);
        let p = partition(1, 0);
        let acknowledging = cache.update_fetch(
            ShareFetchSessionRequest::builder()
                .identity(("g", "m"))
                .connection_id("connection-a")
                .epoch(0)
                .partitions(FetchPartitions {
                    requested: &[p],
                    forgotten: &HashSet::new(),
                })
                .has_acknowledgements(true)
                .build(),
        );
        assert!(acknowledging == Err(codes::INVALID_REQUEST));
        let forgetting = fetch(&cache, "m", 0, &[p], &[p]).expect("open");
        assert!(forgetting.partitions == vec![p]);
    }

    #[test]
    fn acknowledge_zero_epoch_is_invalid_and_final_closes() {
        let cache = ShareSessionCache::new(8);
        let p = partition(1, 0);
        assert!(fetch(&cache, "m", 0, &[p], &[]).is_ok());
        assert!(cache.update_acknowledge("g", "m", 0) == Err(codes::INVALID_SHARE_SESSION_EPOCH));
        assert!(cache.update_acknowledge("g", "m", 1).is_ok());
        assert!(cache.update_acknowledge("g", "m", -1) == Ok(set(&[p])));
    }

    /// A full cache refuses a new member. A member that re-opens its own
    /// session frees its old entry first, and gives back no records.
    #[test]
    fn over_capacity_is_limit_reached_and_reopening_releases_nothing() {
        let cache = ShareSessionCache::new(1);
        let p = partition(1, 0);
        assert!(fetch(&cache, "m1", 0, &[p], &[]).is_ok());
        assert!(fetch(&cache, "m2", 0, &[], &[]) == Err(codes::SHARE_SESSION_LIMIT_REACHED));
        let reopened = fetch(&cache, "m1", 0, &[], &[]).expect("reopen");
        assert!(reopened.released.is_empty());
    }

    /// Selects the rows an incremental response carries, and updates the
    /// session for them when the response goes out unthrottled.
    fn respond(cache: &ShareSessionCache, rows: &[ResponseRow], sent: bool) -> Vec<bool> {
        let carried = cache.select_response("g", "m", rows);
        if sent {
            cache.commit_response("g", "m", rows);
        }
        carried
    }

    /// What three incremental responses carry, and the session's order after
    /// them.
    #[derive(Debug, PartialEq, Eq)]
    struct Responses {
        carried: [Vec<bool>; 3],
        order: Vec<SharePartitionKey>,
    }

    /// Kafka's `CachedSharePartition.maybeUpdateResponseData` over three
    /// incremental responses. A throttled first response carries no rows, so
    /// Kafka never calls `updateAndGenerateResponseData` for it and the session
    /// keeps its flags and its order.
    #[yare::parameterized(
        sent = { true, [vec![true, true, true], vec![false, true, false], vec![false, false, false]], &[1, 2, 0] },
        throttled = { false, [vec![true, true, true], vec![false, false, true], vec![false, false, false]], &[0, 1, 2] },
    )]
    fn incremental_responses_carry_only_partitions_with_news(
        first_sent: bool,
        carried: [Vec<bool>; 3],
        order: &[i32],
    ) {
        let cache = ShareSessionCache::new(8);
        let (p0, p1, p2) = (partition(1, 0), partition(1, 1), partition(1, 2));
        let row = |key, has_records, has_error| ResponseRow {
            key,
            has_records,
            has_error,
        };
        let quiet = [
            row(p0, false, false),
            row(p1, false, false),
            row(p2, false, false),
        ];
        fetch(&cache, "m", 0, &[p0, p1], &[]).expect("open");
        // p2 joins at epoch 1, so its first incremental row is carried.
        fetch(&cache, "m", 1, &[p2], &[]).expect("add");
        let first = respond(
            &cache,
            &[
                row(p0, true, false),
                row(p1, false, true),
                row(p2, false, false),
            ],
            first_sent,
        );
        // p1 had an error, so a sent response carries it once more after it
        // clears.
        fetch(&cache, "m", 2, &[], &[]).expect("continue");
        let second = respond(&cache, &quiet, true);
        let after = fetch(&cache, "m", 3, &[], &[]).expect("continue");
        let third = respond(&cache, &quiet, true);

        // A partition that returned records moves to the end of the order.
        assert2::assert!(
            Responses {
                carried: [first, second, third],
                order: after.partitions,
            } == Responses {
                carried,
                order: order.iter().map(|&index| partition(1, index)).collect(),
            }
        );
    }

    #[test]
    fn response_rotation_uses_last_record_occurrence_and_last_error_state() {
        let cache = ShareSessionCache::new(8);
        let (p0, p1, p2, p3, unknown) = (
            partition(1, 0),
            partition(1, 1),
            partition(1, 2),
            partition(1, 3),
            partition(2, 0),
        );
        let row = |key, has_records, has_error| ResponseRow {
            key,
            has_records,
            has_error,
        };
        fetch(&cache, "m", 0, &[p0, p1, p2, p3], &[]).expect("open");

        cache.commit_response("g", "m", &[
            row(p2, true, true),
            row(p0, true, true),
            row(unknown, true, true),
            row(p1, false, true),
            row(p2, true, false),
            row(p1, false, false),
        ]);

        let continued = fetch(&cache, "m", 1, &[], &[]).expect("continue");
        // Moving p2 again puts it after p0, while untouched partitions keep
        // their relative order. An unknown row has no effect.
        assert!(continued.partitions == vec![p1, p3, p0, p2]);
        // A later row without records updates the error flag without moving
        // that partition after a subsequent record-bearing partition.
        cache.commit_response("g", "m", &[
            row(p1, true, true),
            row(p3, true, false),
            row(p1, false, false),
        ]);
        let rotated = fetch(&cache, "m", 2, &[], &[]).expect("continue after repeated rows");
        assert!(rotated.partitions == vec![p0, p2, p1, p3]);
        // The final row for each cached partition determines whether its
        // error needs a clearing response.
        assert!(
            cache.select_response("g", "m", &[
                row(p0, false, false),
                row(p1, false, false),
                row(p2, false, false),
                row(unknown, false, false),
            ],) == vec![true, false, false, true]
        );
    }

    #[test]
    fn wide_and_small_sessions_progress_concurrently() {
        for partition_count in [1_000_usize, 2_000, 4_000] {
            let cache = Arc::new(ShareSessionCache::new(2));
            let wide: Vec<_> = (0..partition_count)
                .map(|index| partition(1, i32::try_from(index).expect("partition index")))
                .collect();
            let new: Vec<_> = (0..partition_count)
                .map(|index| partition(2, i32::try_from(index).expect("partition index")))
                .collect();
            let forgotten: Vec<_> = wide.iter().step_by(2).copied().collect();
            let forgotten_count = forgotten.len();
            let small = partition(3, 0);

            let wide_cache = Arc::clone(&cache);
            let wide_thread = thread::spawn(move || {
                fetch(&wide_cache, "wide", 0, &wide, &[]).expect("open wide");
                let updated = fetch(&wide_cache, "wide", 1, &new, &forgotten).expect("update wide");
                let rows: Vec<_> = updated
                    .partitions
                    .iter()
                    .copied()
                    .map(|key| ResponseRow {
                        key,
                        has_records: true,
                        has_error: false,
                    })
                    .collect();
                assert!(
                    wide_cache
                        .select_response("g", "wide", &rows)
                        .iter()
                        .all(|carried| *carried)
                );
                wide_cache.commit_response("g", "wide", &rows);
                updated.partitions
            });
            let small_cache = Arc::clone(&cache);
            let small_thread = thread::spawn(move || {
                fetch(&small_cache, "small", 0, &[small], &[]).expect("open small");
                fetch(&small_cache, "small", 1, &[], &[]).expect("update small")
            });

            let wide_order = wide_thread.join().expect("wide thread");
            let small_update = small_thread.join().expect("small thread");
            assert!(wide_order.len() == partition_count + forgotten_count);
            assert!(small_update.partitions == vec![small]);
        }
    }

    #[test]
    fn disconnect_removes_only_the_connections_live_session() {
        let cache = ShareSessionCache::new(8);
        let p = partition(1, 0);
        assert!(fetch(&cache, "m", 0, &[p], &[]).is_ok());
        let closed = cache.disconnect("connection-a").expect("session");
        assert!(closed.group == "g");
        assert!(closed.member == "m");
        assert!(closed.partitions == set(&[p]));
        assert!(cache.disconnect("connection-a").is_none());
    }

    #[test]
    fn reopening_does_not_remove_another_sessions_connection_mapping() {
        let cache = ShareSessionCache::new(8);
        let old_partition = partition(1, 0);
        let current_partition = partition(2, 0);
        fetch_on(&cache, "old", "connection-a", 0, &[old_partition], &[])
            .expect("open old session");
        fetch_on(
            &cache,
            "current",
            "connection-a",
            0,
            &[current_partition],
            &[],
        )
        .expect("replace connection mapping");
        fetch_on(&cache, "old", "connection-b", 0, &[], &[])
            .expect("reopen old member on a new connection");

        let closed = cache
            .disconnect("connection-a")
            .expect("current session remains mapped");
        assert!(closed.group == "g");
        assert!(closed.member == "current");
        assert!(closed.partitions == set(&[current_partition]));
    }
}
