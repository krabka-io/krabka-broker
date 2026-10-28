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
    use {
        super::{murmur2, murmur2_partition},
        assert2::assert,
        rstest::fixture,
        rstest_bdd_macros::{given, then, when},
    };

    #[derive(Default)]
    struct HashScenario {
        key: Vec<u8>,
        partitions: i32,
        hash: Option<u32>,
        partition: Option<i32>,
    }

    #[fixture]
    fn hashing() -> HashScenario {
        // Keep hashing inputs and observations fresh for each scenario.
        HashScenario::default()
    }

    #[given("the Kafka hash key is {key}")]
    fn key_bytes(hashing: &mut HashScenario, key: String) {
        hashing.key = if key == "(empty)" {
            Vec::new()
        } else {
            key.into_bytes()
        };
    }

    #[given("the destination topic has {partitions} partitions")]
    fn partition_count(hashing: &mut HashScenario, partitions: i32) {
        hashing.partitions = partitions;
    }

    #[when("the key is hashed with Kafka Murmur2")]
    fn hash_key(hashing: &mut HashScenario) { hashing.hash = Some(murmur2(&hashing.key)); }

    #[when("Kafka's absolute-hash partition rule is applied")]
    fn choose_partition(hashing: &mut HashScenario) {
        hashing.partition = Some(murmur2_partition(&hashing.key, hashing.partitions));
    }

    #[then("the unsigned hash is {expected}")]
    fn hash_matches(hashing: &HashScenario, expected: u32) {
        assert!(hashing.hash == Some(expected));
    }

    #[then("the destination partition is {expected}")]
    fn partition_matches(hashing: &HashScenario, expected: i32) {
        assert!(hashing.partition == Some(expected));
    }

    rstest_bdd_macros::scenarios!("tests/features/migrated/src/kafka_hash.feature", fixtures = [hashing: HashScenario]);
}
