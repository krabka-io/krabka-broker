//! Kafka-compatible hashing helpers.

const MURMUR2_SEED: u32 = 0x9747_b28c;
const MURMUR2_M: u32 = 0x5bd1_e995;
const MURMUR2_R: u32 = 24;

#[must_use]
pub fn murmur2(data: &[u8]) -> u32 {
    let length = data.len();
    let length_bytes = length.to_le_bytes();
    // The low 32 bits of the length, as Java's `int` length holds them.
    let length32 = length_bytes
        .iter()
        .take(4)
        .rev()
        .fold(0_u32, |low, &byte| (low << 8) | u32::from(byte));
    let mut h: u32 = MURMUR2_SEED ^ length32;
    let (chunks, rem) = data.as_chunks::<4>();

    for chunk in chunks {
        let mut k = u32::from_le_bytes(*chunk);
        k = k.wrapping_mul(MURMUR2_M);
        k ^= k >> MURMUR2_R;
        k = k.wrapping_mul(MURMUR2_M);
        h = h.wrapping_mul(MURMUR2_M);
        h ^= k;
    }

    match rem {
        [a] => {
            h ^= u32::from(*a);
            h = h.wrapping_mul(MURMUR2_M);
        }
        [a, b] => {
            h ^= u32::from(*b) << 8;
            h ^= u32::from(*a);
            h = h.wrapping_mul(MURMUR2_M);
        }
        [a, b, c] => {
            h ^= u32::from(*c) << 16;
            h ^= u32::from(*b) << 8;
            h ^= u32::from(*a);
            h = h.wrapping_mul(MURMUR2_M);
        }
        [] => {}
        _ => unreachable!("chunks_exact(4) leaves at most three remainder bytes"),
    }

    h ^= h >> 13;
    h = h.wrapping_mul(MURMUR2_M);
    h ^= h >> 15;
    h
}

/// Apply Kafka's `Utils.abs(murmur2(data)) % numPartitions` convention.
#[must_use]
pub fn murmur2_partition(data: &[u8], num_partitions: i32) -> i32 {
    let hash = murmur2(data).cast_signed();
    let positive = if hash == i32::MIN { 0 } else { hash.abs() };
    positive % num_partitions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    #[rstest_bdd_macros::then(
        "kafka hash tests: murmur2 matches kafka vectors for remainder lengths for {example}"
    )]
    fn bdd_verify_murmur2_matches_kafka_vectors_for_remainder_lengths(example: String) {
        type Example<'a> = (&'a [u8], u32);
        let (input, expected): Example<'_> = match example.as_str() {
            "empty" => (b"", 0x106e_08d9),
            "one byte" => (b"a", 0xa2d0_b27c),
            "two bytes" => (b"ab", 0x12d8_262a),
            "three bytes" => (b"abc", 0x1c94_221b),
            "four bytes" => (b"abcd", 0xb11a_b5f4),
            "five bytes" => (b"abcde", 0x1b89_7edd),
            _ => panic!("unknown BDD example: {example}"),
        };

        assert2::assert!(murmur2(input) == expected);
    }

    #[cfg(test)]
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/kafka_hash.feature",
        name = "tests: murmur2 matches kafka vectors for remainder lengths"
    )]
    fn murmur2_matches_kafka_vectors_for_remainder_lengths(example: String) { let _ = example; }

    #[cfg(test)]
    #[rstest_bdd_macros::then(
        "kafka hash tests: murmur2 partition matches kafka utils abs mod for {example}"
    )]
    fn bdd_verify_murmur2_partition_matches_kafka_utils_abs_mod(example: String) {
        type Example<'a> = (&'a [u8], i32, i32);
        let (input, partitions, expected): Example<'_> = match example.as_str() {
            "transaction id" => (b"my-tid", 50, 43),
            "producer id" => (b"producer-1", 50, 45),
            "orders transaction" => (b"tx-orders-prod", 50, 26),
            "five byte key" => (b"abcde", 7, 4),
            _ => panic!("unknown BDD example: {example}"),
        };

        assert2::assert!(murmur2_partition(input, partitions) == expected);
    }

    #[cfg(test)]
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/kafka_hash.feature",
        name = "tests: murmur2 partition matches kafka utils abs mod"
    )]
    fn murmur2_partition_matches_kafka_utils_abs_mod(example: String) { let _ = example; }
}
