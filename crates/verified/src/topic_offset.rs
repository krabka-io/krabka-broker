//! Topic-incarnation guard for committed-offset deletion.

use creusot_std::prelude::*;

/// Select an offset for deletion by topic incarnation.
///
/// Legacy offsets have no UUID and are deleted by name. An identified offset
/// is deleted only by its own incarnation, not a delayed deletion of an older
/// topic with the same name. The host has already matched the topic name.
#[ensures(result == match stored_id {
    None => true,
    Some(own) => own@ == deleted_id@,
})]
#[must_use]
pub fn deleted_topic_offset_matches(stored_id: Option<u128>, deleted_id: u128) -> bool {
    if let Some(own) = stored_id {
        return own == deleted_id;
    }
    true
}
