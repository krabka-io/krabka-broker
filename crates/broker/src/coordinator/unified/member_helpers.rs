//! Request-level helpers that every group protocol shares: minting the member
//! id of a first join and finding the members whose session has expired.
//!
//! The classic, next-gen, share, and streams paths all call them, and they are
//! pure functions over request fields, so they sit apart from the coordinator
//! that calls them.

use std::time::{Duration, Instant};

/// Evicts expired members through the group's own removal transition.
macro_rules! evict_expired {
    ($(#[$doc:meta])*) => {
        $(#[$doc])*
        pub fn evict_expired(
            &mut self,
            now: std::time::Instant,
            session_timeout: std::time::Duration,
        ) -> Vec<String> {
            $crate::coordinator::unified::member_helpers::evict_expired!(self, now, session_timeout)
        }
    };
    ($group:expr, $now:expr, $timeout:expr) => {{
        let group = $group;
        let expired = $crate::coordinator::unified::expired_member_ids(
            group
                .members
                .iter()
                .map(|(id, member)| (id.as_str(), member.last_seen)),
            $now,
            $timeout,
        );
        for id in &expired {
            group.remove_member(id);
        }
        expired
    }};
}

pub(crate) use evict_expired;

/// Every group refuses the proved successor at exhaustion; only selected protocols mark it dirty.
macro_rules! bump_group_epoch {
    ($(#[$doc:meta])* $group:ident; $($after:tt)*) => {
        $(#[$doc])*
        pub fn bump_epoch(&mut $group) -> bool {
            let Some(epoch) = $crate::metadata_epoch::next_i32($group.group_epoch) else {
                return false;
            };
            $group.group_epoch = epoch;
            $($after)*
            true
        }
    };
}
pub(crate) use bump_group_epoch;

/// The same assignment-delay query on each protocol's stored timestamp.
macro_rules! assignment_delay_method {
    () => {
        /// Whether the previous target assignment still holds the next one back.
        #[must_use]
        pub(crate) fn assignment_delayed(
            &self,
            interval: std::time::Duration,
            now: std::time::Instant,
        ) -> bool {
            $crate::coordinator::unified::member_helpers::assignment_delayed(
                self.assignment_timestamp,
                interval,
                now,
            )
        }
    };
}
pub(crate) use assignment_delay_method;

/// An absent heartbeat field keeps the stored value; allocate only when it changes.
pub(crate) fn update_present<T: Clone + PartialEq>(
    stored: &mut Option<T>,
    incoming: Option<&T>,
) -> bool {
    if let Some(incoming) = incoming
        && stored.as_ref() != Some(incoming)
    {
        match stored {
            Some(value) => value.clone_from(incoming),
            None => *stored = Some(incoming.clone()),
        }
        true
    } else {
        false
    }
}

pub(crate) fn first_join_member_id(request_member_id: &str) -> String {
    if request_member_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        request_member_id.to_string()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ClientIdentity<'a> {
    pub id: &'a str,
    pub host: &'a str,
}

impl ClientIdentity<'_> {
    /// Updates stored client metadata, allocating only for changed fields.
    pub fn update_metadata(self, client_id: &mut String, client_host: &mut String) -> bool {
        let mut changed = false;
        for (stored, incoming) in [(client_id, self.id), (client_host, self.host)] {
            if stored.as_str() != incoming {
                *stored = incoming.to_string();
                changed = true;
            }
        }
        changed
    }
}

pub(crate) fn expired_member_ids<'a>(
    members: impl IntoIterator<Item = (&'a str, Instant)>,
    now: Instant,
    session_timeout: Duration,
) -> Vec<String> {
    members
        .into_iter()
        .filter(|(_, last_seen)| now.duration_since(*last_seen) > session_timeout)
        .map(|(id, _)| id.to_string())
        .collect()
}

/// Kafka's `canComputeNextTargetAssignment`, negated. A zero interval or
/// missing timestamp computes at once; otherwise the prior assignment holds
/// the next one until its interval elapses, including after a backward clock step.
pub(crate) fn assignment_delayed(
    timestamp: Option<Instant>,
    interval: Duration,
    now: Instant,
) -> bool {
    !interval.is_zero() && timestamp.is_some_and(|computed| now < computed + interval)
}

#[cfg(test)]
mod helper_tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn first_join_member_id_preserves_client_supplied_id() {
        assert!(first_join_member_id("member-a") == "member-a");
    }

    #[test]
    fn first_join_member_id_mints_uuid_for_empty_id() {
        let member_id = first_join_member_id("");

        check!(!member_id.is_empty());
        assert!(uuid::Uuid::parse_str(&member_id).is_ok());
    }

    #[test]
    fn expired_member_ids_returns_only_members_past_timeout() {
        let now = Instant::now();
        let session_timeout = Duration::from_secs(10);
        let expired = now
            .checked_sub(Duration::from_secs(11))
            .expect("past instant");
        let active = now
            .checked_sub(Duration::from_secs(10))
            .expect("past instant");
        let future = now
            .checked_add(Duration::from_secs(1))
            .expect("future instant");

        let expired = expired_member_ids(
            [("expired", expired), ("active", active), ("future", future)],
            now,
            session_timeout,
        );

        assert!(expired == vec!["expired".to_string()]);
    }
}
