//! Pure `ApiVersions` admission, shared by broker and controller listeners.
//!
//! The host decodes the request, validates KIP-511 strings, and parses/compares
//! cluster identifiers. Socket identity, authentication state and image freshness
//! remain host obligations. This decides only this handshake response; it does
//! not fence subsequent pipelined requests. No feature/default ceiling changes.

#[cfg(creusot)]
use creusot_std::prelude::*;

/// Whether the listener performs the broker-only post-authentication comparison.
#[krabka_macros::model_type]
pub enum IdentityComparison {
    Skip,
    Compare,
}

/// Facts read from one decoded request and its listener.
#[krabka_macros::model_type]
pub struct AdmissionFacts {
    pub version: i16,
    pub client_info_valid: bool,
    pub cluster_present: bool,
    pub node_id: i32,
    pub expected_node_id: Option<i32>,
    pub cluster_matches: bool,
    /// Compare only on an authenticated broker listener, never a controller.
    pub comparison: IdentityComparison,
}

/// Request validity, independent of listener identity comparison.
#[cfg_attr(creusot, ensures(result ==
    ((version@ < 3 || client_info_valid)
    && (version@ < 5 || cluster_present == (node_id@ != -1)))))]
#[must_use]
pub fn request_valid(
    version: i16,
    client_info_valid: bool,
    cluster_present: bool,
    node_id: i32,
) -> bool {
    (version < 3 || client_info_valid) && (version < 5 || cluster_present == (node_id != -1))
}

/// Wire error code: `INVALID_REQUEST` first, then broker-only `REBOOTSTRAP_REQUIRED`.
///
/// Controller and preauthentication callers must set `comparison` to `IdentityComparison::Skip`.
/// The contract is unconditional even for inconsistent host-supplied facts.
#[cfg_attr(creusot, ensures((result@ == 42) ==
    ((facts.version@ >= 3 && !facts.client_info_valid)
    || (facts.version@ >= 5 && facts.cluster_present != (facts.node_id@ != -1)))))]
#[cfg_attr(creusot, ensures((result@ == 129) ==
    ((facts.version@ < 3 || facts.client_info_valid)
    && (facts.version@ < 5 || facts.cluster_present == (facts.node_id@ != -1))
    && facts.version@ >= 5 && facts.comparison == IdentityComparison::Compare && facts.cluster_present
    && (!facts.cluster_matches || Some(facts.node_id) != facts.expected_node_id))))]
#[cfg_attr(creusot, ensures(result@ == 0 || result@ == 42 || result@ == 129))]
#[must_use]
pub fn admission_error(facts: AdmissionFacts) -> i16 {
    if !request_valid(
        facts.version,
        facts.client_info_valid,
        facts.cluster_present,
        facts.node_id,
    ) {
        42
    } else if facts.version >= 5
        && matches!(facts.comparison, IdentityComparison::Compare)
        && facts.cluster_present
        && (!facts.cluster_matches || Some(facts.node_id) != facts.expected_node_id)
    {
        129
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn identity_presence_listener_and_version_boundaries() {
        use IdentityComparison::{Compare, Skip};
        // Expected wire errors are explicit protocol outcomes, not another
        // implementation of the decision under test. Skip represents both
        // controller requests and broker requests before authentication.
        for (
            version,
            client_info_valid,
            cluster_present,
            node_id,
            expected_node_id,
            cluster_matches,
            comparison,
            expected,
        ) in [
            (i16::MIN, false, true, -1, None, false, Compare, 0),
            (2, false, false, 7, None, false, Compare, 0),
            (3, false, false, -1, None, false, Skip, 42),
            (4, true, true, -1, None, false, Compare, 0),
            (5, true, false, -1, None, false, Compare, 0),
            (5, true, false, 0, Some(0), true, Compare, 42),
            (5, true, true, -1, Some(-1), true, Compare, 42),
            (5, true, true, 0, Some(0), true, Compare, 0),
            (5, true, true, 0, Some(0), false, Compare, 129),
            (5, true, true, 0, Some(1), true, Compare, 129),
            (5, true, true, 0, None, true, Compare, 129),
            (5, true, true, 0, None, false, Skip, 0),
            (5, false, true, 0, None, false, Compare, 42),
            (5, true, true, -2, Some(-2), true, Compare, 0),
            (5, true, true, i32::MIN, Some(i32::MIN), true, Compare, 0),
            (
                i16::MAX,
                true,
                true,
                i32::MAX,
                Some(i32::MAX),
                true,
                Compare,
                0,
            ),
            (
                i16::MAX,
                true,
                true,
                i32::MAX,
                Some(i32::MIN),
                true,
                Compare,
                129,
            ),
            (i16::MAX, true, false, i32::MAX, None, false, Skip, 42),
        ] {
            assert!(
                admission_error(AdmissionFacts {
                    version,
                    client_info_valid,
                    cluster_present,
                    node_id,
                    expected_node_id,
                    cluster_matches,
                    comparison,
                }) == expected
            );
        }
    }
}
