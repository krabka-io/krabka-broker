use super::*;

#[test]
fn producer_basic() {
    // Six single-record batches fill the five-batch window and evict one.
    run(
        ProducerModel {
            max_epoch: 1,
            max_seq: 6,
        },
        "producer_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn producer_wide() {
    run(
        ProducerModel {
            max_epoch: 3,
            max_seq: 9,
        },
        "producer_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}
