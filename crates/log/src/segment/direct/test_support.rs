//! The test helper that the `O_DIRECT` modules' unit tests share.

/// The bytes `0..len` of a pattern that never repeats inside one block,
/// tagged by `seed` so two appends differ.
pub(super) fn pattern(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| {
            seed.wrapping_mul(31)
                .wrapping_add(u8::try_from(i % 251).unwrap())
        })
        .collect()
}
