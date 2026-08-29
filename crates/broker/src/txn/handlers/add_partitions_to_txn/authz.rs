//! The per-topic `Write` ACL sweep that `AddPartitionsToTxn` runs once the
//! whole-transaction `TransactionalId` check has passed.
//!
//! The transactional-id decision covers a whole request entry and stays with
//! the version paths that make it. This module answers only the narrower
//! question of which topic names the authorizer denies, so that the result
//! builders can stamp `TOPIC_AUTHORIZATION_FAILED` on exactly those rows while
//! the remaining topics resolve normally.

use std::net::SocketAddr;

use krabka_metadata::{AclOperation, MetadataImage};
use krabka_protocol::owned::common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic;
use krabka_security::Principal;

use crate::authorizer::{AuthorizationResult, Authorizer, authorize_topics};

/// Builds the set of topic names that the authorizer denies `Write` on
/// `Topic(name)` for this principal and host. The caller uses the set to stamp
/// `TOPIC_AUTHORIZATION_FAILED` on every partition row of a denied topic.
pub(super) fn denied_topics(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    principal: &Principal,
    peer: &SocketAddr,
    topics: &[AddPartitionsToTxnTopic],
) -> std::collections::HashSet<String> {
    let names: Vec<&str> = topics.iter().map(|t| t.name.as_str()).collect();
    let map = authorize_topics(
        authorizer,
        image,
        principal,
        peer,
        AclOperation::Write,
        names,
    );
    map.into_iter()
        .filter_map(|(name, r)| {
            if r == AuthorizationResult::Deny {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect()
}
