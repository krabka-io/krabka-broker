use super::*;

#[test]
fn isr_safety() {
    run(
        IsrModel::safety(3),
        "isr_safety",
        PINNED_UNIQUE_STATES_SAFETY,
    );
}

#[test]
fn isr_overshoot() {
    run(
        IsrModel::overshoot(3),
        "isr_overshoot",
        PINNED_UNIQUE_STATES_OVERSHOOT,
    );
}
