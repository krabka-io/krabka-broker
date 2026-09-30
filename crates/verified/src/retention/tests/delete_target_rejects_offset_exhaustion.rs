use super::*;

#[test]
fn delete_target_rejects_offset_exhaustion() {
    for (last_offset, expected) in [
        (None, None),
        (Some(9), Some(10)),
        (Some(i64::MAX - 1), Some(i64::MAX)),
        (Some(i64::MAX), None),
    ] {
        check!(retention_delete_target(last_offset) == expected);
    }
}
