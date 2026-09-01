//! Admission decisions for signed WORM manifests and their object sets.

use creusot_std::prelude::*;

/// Why a manifest signature is not an accepted attestation.
#[cfg_attr(creusot, derive(DeepModel))]
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
pub enum WormSignatureDecision {
    Unsigned,
    Untrusted,
    Invalid,
    Admit,
}

/// Requires a present signature, an externally trusted key, and verification
/// against the canonical manifest signing bytes.
#[ensures(!present ==> result == WormSignatureDecision::Unsigned)]
#[ensures(present && !trusted ==> result == WormSignatureDecision::Untrusted)]
#[ensures(present && trusted && !canonical_valid
    ==> result == WormSignatureDecision::Invalid)]
#[ensures(present && trusted && canonical_valid
    ==> result == WormSignatureDecision::Admit)]
#[must_use]
pub fn worm_signature_decision(
    present: bool,
    trusted: bool,
    canonical_valid: bool,
) -> WormSignatureDecision {
    if !present {
        WormSignatureDecision::Unsigned
    } else if !trusted {
        WormSignatureDecision::Untrusted
    } else if !canonical_valid {
        WormSignatureDecision::Invalid
    } else {
        WormSignatureDecision::Admit
    }
}

/// Why the objects named by one manifest are not an exact archive set.
#[cfg_attr(creusot, derive(DeepModel))]
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
pub enum WormObjectSetDecision {
    Empty,
    DuplicateKey,
    CoordinateMismatch,
    MissingObject,
    CountMismatch,
    SizeMismatch,
    DigestMismatch,
    Admit,
}

/// Host observations about the objects belonging to one signed segment.
#[cfg_attr(creusot, derive(DeepModel))]
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
#[allow(clippy::struct_excessive_bools)] // Each field is an independently proved host fact.
pub struct WormObjectSetFacts {
    pub object_count: u64,
    pub listed_count: u64,
    pub unique_keys: bool,
    pub coordinates_match: bool,
    pub all_present: bool,
    pub sizes_match: bool,
    pub require_digests: bool,
    pub digests_match: bool,
}

/// Requires a nonempty, one-to-one object set with exact coordinates, sizes,
/// and, when requested, digests.
#[ensures(facts.object_count@ == 0 ==> result == WormObjectSetDecision::Empty)]
#[ensures(facts.object_count@ > 0 && !facts.unique_keys
    ==> result == WormObjectSetDecision::DuplicateKey)]
#[ensures(facts.object_count@ > 0 && facts.unique_keys && !facts.coordinates_match
    ==> result == WormObjectSetDecision::CoordinateMismatch)]
#[ensures(facts.object_count@ > 0 && facts.unique_keys && facts.coordinates_match && !facts.all_present
    ==> result == WormObjectSetDecision::MissingObject)]
#[ensures(facts.object_count@ > 0 && facts.unique_keys && facts.coordinates_match && facts.all_present
    && facts.object_count@ != facts.listed_count@ ==> result == WormObjectSetDecision::CountMismatch)]
#[ensures(facts.object_count@ > 0 && facts.unique_keys && facts.coordinates_match && facts.all_present
    && facts.object_count@ == facts.listed_count@ && !facts.sizes_match
    ==> result == WormObjectSetDecision::SizeMismatch)]
#[ensures(facts.object_count@ > 0 && facts.unique_keys && facts.coordinates_match && facts.all_present
    && facts.object_count@ == facts.listed_count@ && facts.sizes_match
    && facts.require_digests && !facts.digests_match
    ==> result == WormObjectSetDecision::DigestMismatch)]
#[ensures(result == WormObjectSetDecision::Admit ==> facts.object_count@ > 0
    && facts.unique_keys && facts.coordinates_match && facts.all_present
    && facts.object_count@ == facts.listed_count@ && facts.sizes_match
    && (!facts.require_digests || facts.digests_match))]
#[must_use]
pub fn worm_object_set_decision(facts: WormObjectSetFacts) -> WormObjectSetDecision {
    if facts.object_count == 0 {
        WormObjectSetDecision::Empty
    } else if !facts.unique_keys {
        WormObjectSetDecision::DuplicateKey
    } else if !facts.coordinates_match {
        WormObjectSetDecision::CoordinateMismatch
    } else if !facts.all_present {
        WormObjectSetDecision::MissingObject
    } else if facts.object_count != facts.listed_count {
        WormObjectSetDecision::CountMismatch
    } else if !facts.sizes_match {
        WormObjectSetDecision::SizeMismatch
    } else if facts.require_digests && !facts.digests_match {
        WormObjectSetDecision::DigestMismatch
    } else {
        WormObjectSetDecision::Admit
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn signatures_require_every_attestation_fact() {
        assert!(worm_signature_decision(false, true, true) == WormSignatureDecision::Unsigned);
        assert!(worm_signature_decision(true, false, true) == WormSignatureDecision::Untrusted);
        assert!(worm_signature_decision(true, true, false) == WormSignatureDecision::Invalid);
        assert!(worm_signature_decision(true, true, true) == WormSignatureDecision::Admit);
    }

    #[test]
    fn object_sets_fail_closed_in_diagnostic_order() {
        let cases = [
            (
                (0, 0, true, true, true, true, true, true),
                WormObjectSetDecision::Empty,
            ),
            (
                (2, 2, false, true, true, true, true, true),
                WormObjectSetDecision::DuplicateKey,
            ),
            (
                (1, 1, true, false, true, true, true, true),
                WormObjectSetDecision::CoordinateMismatch,
            ),
            (
                (1, 1, true, true, false, true, true, true),
                WormObjectSetDecision::MissingObject,
            ),
            (
                (1, 2, true, true, true, true, true, true),
                WormObjectSetDecision::CountMismatch,
            ),
            (
                (1, 1, true, true, true, false, true, true),
                WormObjectSetDecision::SizeMismatch,
            ),
            (
                (1, 1, true, true, true, true, true, false),
                WormObjectSetDecision::DigestMismatch,
            ),
            (
                (1, 1, true, true, true, true, false, false),
                WormObjectSetDecision::Admit,
            ),
            (
                (1, 1, true, true, true, true, true, true),
                WormObjectSetDecision::Admit,
            ),
        ];
        for (
            (
                object_count,
                listed_count,
                unique_keys,
                coordinates_match,
                all_present,
                sizes_match,
                require_digests,
                digests_match,
            ),
            expected,
        ) in cases
        {
            let facts = WormObjectSetFacts {
                object_count,
                listed_count,
                unique_keys,
                coordinates_match,
                all_present,
                sizes_match,
                require_digests,
                digests_match,
            };
            assert!(worm_object_set_decision(facts) == expected);
        }
    }
}
