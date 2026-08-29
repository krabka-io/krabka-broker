//! The `DeleteRecords` ACL preamble: reducing a batch topic authorization to
//! the set of topic names the handler must fail.
//!
//! Authorization is decided once for the whole request, and a denied topic
//! then stamps `TOPIC_AUTHORIZATION_FAILED` on every partition row it asked
//! for. This module holds that reduction and nothing about trimming.

use crate::authorizer::AuthorizationResult;

pub(super) fn denied_topic_names(
    acl_results: &std::collections::HashMap<&str, AuthorizationResult>,
) -> std::collections::HashSet<String> {
    acl_results
        .iter()
        .filter_map(|(name, r)| {
            if *r == AuthorizationResult::Deny {
                Some((*name).to_string())
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn denied_topic_names_keeps_only_denied_decisions() {
        let acl_results = maplit::hashmap! {
        "denied" => AuthorizationResult::Deny,
        "allowed" => AuthorizationResult::Allow};

        let denied = denied_topic_names(&acl_results);

        let expected = maplit::hashset! {"denied".to_string()};
        assert!(denied == expected);
    }
}
