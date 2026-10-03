use super::*;

#[test]
fn fetch_session_basic() {
    // One partition; the core rename/half-identity churn alphabet.
    run(
        FsModel {
            refs: vec![
                Ref::Both(NAME_A, 1),
                Ref::NameOnly(NAME_A),
                Ref::IdOnly(1),
                Ref::Both(NAME_B, 1),
            ],
            partitions: vec![0],
        },
        "fetch_session_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn fetch_session_wide() {
    // One partition, the full identity-churn alphabet: both halves of topic A
    // (id U), its rename to B (same id U), a name-only and id-only form of each,
    // and a second id V (a stale/conflicting identity). forget and merge are both
    // partition-scoped (`k.partition == p`), so a single partition exercises all
    // the shadow logic; the `two_keys` witness still fires via two distinct-
    // identity topics coexisting on the one partition.
    run(
        FsModel {
            refs: vec![
                Ref::Both(NAME_A, 1),
                Ref::NameOnly(NAME_A),
                Ref::IdOnly(1),
                Ref::Both(NAME_B, 1),
                Ref::NameOnly(NAME_B),
                Ref::Both(NAME_A, 2),
                Ref::IdOnly(2),
            ],
            partitions: vec![0],
        },
        "fetch_session_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}
