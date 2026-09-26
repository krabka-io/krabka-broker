//! Request-level helpers that every group protocol shares: minting the member
//! id of a first join and finding the members whose session has expired.
//!
//! The classic, next-gen, share, and streams paths all call them, and they are
//! pure functions over request fields, so they sit apart from the coordinator
//! that calls them.

use std::time::{Duration, Instant};

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
