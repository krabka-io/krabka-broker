use super::*;

#[test]
fn visibility_basic() {
    run(
        VisModel { max_offset: 4 },
        "visibility_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn visibility_wide() {
    run(
        VisModel { max_offset: 7 },
        "visibility_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}
