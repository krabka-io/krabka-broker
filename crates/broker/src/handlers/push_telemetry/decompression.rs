//! The cap on how much a client-telemetry payload may decompress to.
//!
//! KIP-714 lets a client push metrics under any compression codec the broker
//! advertises, so a small request body can expand without bound. The handler
//! applies this ratio-with-floor-and-ceiling policy to pick the output limit it
//! hands the decompressor. The calculation is its own module because it is a
//! pure policy decision, testable without a broker.

use krabka_units::{
    ByteSize, Ratio,
    convert::{ByteSizeExt as _, RatioExt as _},
};

pub(super) fn decompressed_output_bound(
    compressed_len: ByteSize,
    ratio: Ratio,
    floor: ByteSize,
    ceiling: ByteSize,
) -> ByteSize {
    ByteSize::from_bytes_f64(compressed_len.bytes_f64() * ratio.as_f64())
        .max(floor)
        .min(ceiling)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{bytes, fraction, gibibytes};

    use super::*;

    #[test]
    fn decompressed_output_bound_uses_runtime_policy() {
        let cases = [
            ("ratio", bytes(10), 7, bytes(1), bytes(1_000), bytes(70)),
            ("floor", bytes(10), 2, bytes(50), bytes(1_000), bytes(50)),
            ("ceiling", bytes(10), 100, bytes(1), bytes(500), bytes(500)),
            (
                "ceiling clamps a very large product",
                gibibytes(4),
                1_000_000,
                bytes(1),
                gibibytes(1),
                gibibytes(1),
            ),
        ];

        for (name, compressed_len, ratio, floor, ceiling, expected) in cases {
            assert!(
                decompressed_output_bound(
                    compressed_len,
                    fraction(f64::from(ratio)),
                    floor,
                    ceiling
                ) == expected,
                "{name}"
            );
        }
    }
}
