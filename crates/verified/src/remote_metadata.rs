//! KIP-405 remote-log metadata topic partitioning.

use creusot_std::prelude::*;

const MURMUR2_SEED: u32 = 0x9747_b28c;
const MURMUR2_M: u32 = 0x5bd1_e995;

#[cfg(creusot)]
#[logic]
fn java_long_hash_model(bits: u64) -> i32 {
    pearlite! { (bits ^ (bits >> 32u32)) as i32 }
}

#[cfg_attr(creusot, ensures(result == java_long_hash_model(bits)))]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    reason = "Java Long.hashCode deliberately folds the two 32-bit halves"
)]
fn java_long_hash(bits: u64) -> i32 {
    (bits ^ (bits >> 32)) as i32
}

#[cfg(creusot)]
#[logic]
fn java_objects_hash_model(topic_id: u128, user_partition: i32) -> i32 {
    pearlite! {
        let most_significant_bits = (topic_id >> 64u32) as u64;
        let least_significant_bits = topic_id as u64;
        let first = 1i32 * 31i32 + java_long_hash_model(least_significant_bits);
        let second = first * 31i32 + java_long_hash_model(most_significant_bits);
        second * 31i32 + user_partition
    }
}

#[cfg_attr(
    creusot,
    ensures(result == java_objects_hash_model(topic_id, user_partition))
)]
#[allow(
    clippy::cast_possible_truncation,
    reason = "a UUID's two u64 halves are selected by the shifts before casting"
)]
fn java_objects_hash(topic_id: u128, user_partition: i32) -> i32 {
    let most_significant_bits = (topic_id >> 64) as u64;
    let least_significant_bits = topic_id as u64;

    let mut hash = 1_i32;
    hash = hash
        .wrapping_mul(31)
        .wrapping_add(java_long_hash(least_significant_bits));
    hash = hash
        .wrapping_mul(31)
        .wrapping_add(java_long_hash(most_significant_bits));
    hash.wrapping_mul(31).wrapping_add(user_partition)
}

#[cfg(creusot)]
#[logic]
fn reverse_bytes_model(value: u32) -> u32 {
    pearlite! {
        (value >> 24u32)
            | ((value >> 8u32) & 0x0000_ff00u32)
            | ((value << 8u32) & 0x00ff_0000u32)
            | (value << 24u32)
    }
}

#[cfg_attr(creusot, ensures(result == reverse_bytes_model(value)))]
fn reverse_bytes(value: u32) -> u32 {
    (value >> 24) | ((value >> 8) & 0x0000_ff00) | ((value << 8) & 0x00ff_0000) | (value << 24)
}

#[cfg(creusot)]
#[logic]
fn kafka_murmur2_i32_model(value: i32) -> u32 {
    pearlite! {
        let chunk0 = reverse_bytes_model(value as u32);
        let chunk1 = chunk0 * 0x5bd1_e995u32;
        let chunk2 = (chunk1 ^ (chunk1 >> 24u32)) * 0x5bd1_e995u32;
        let hash0 = (0x9747_b28cu32 ^ 4u32) * 0x5bd1_e995u32;
        let hash1 = hash0 ^ chunk2;
        let hash2 = (hash1 ^ (hash1 >> 13u32)) * 0x5bd1_e995u32;
        hash2 ^ (hash2 >> 15u32)
    }
}

/// Kafka Murmur2 over the four big-endian bytes of a Java `int`.
#[cfg_attr(creusot, ensures(result == kafka_murmur2_i32_model(value)))]
#[allow(
    clippy::cast_sign_loss,
    reason = "Murmur2 consumes the Java int's unsigned bit image"
)]
fn kafka_murmur2_i32(value: i32) -> u32 {
    let mut hash = MURMUR2_SEED ^ 4;
    let mut chunk = reverse_bytes(value as u32);
    chunk = chunk.wrapping_mul(MURMUR2_M);
    chunk ^= chunk >> 24;
    chunk = chunk.wrapping_mul(MURMUR2_M);
    hash = hash.wrapping_mul(MURMUR2_M);
    hash ^= chunk;
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(MURMUR2_M);
    hash ^ (hash >> 15)
}

#[cfg(creusot)]
#[logic]
fn kafka_to_positive_model(hash: u32) -> Int {
    pearlite! { hash@ % 2_147_483_648 }
}

#[cfg_attr(creusot, ensures(result@ == kafka_to_positive_model(hash)))]
#[ensures(0 <= result@)]
#[ensures(result@ <= i32::MAX@)]
#[allow(
    clippy::cast_possible_wrap,
    reason = "modulo 2^31 clears the sign bit before the Kafka int conversion"
)]
fn kafka_to_positive(hash: u32) -> i32 {
    (hash % 0x8000_0000) as i32
}

#[cfg(creusot)]
#[logic]
fn remote_metadata_partition_model(
    topic_id: u128,
    user_partition: i32,
    metadata_partition_count: i32,
) -> Int {
    pearlite! {
        if metadata_partition_count <= 0i32 {
            0
        } else {
            kafka_to_positive_model(kafka_murmur2_i32_model(
                java_objects_hash_model(topic_id, user_partition)
            )) % metadata_partition_count@
        }
    }
}

/// Map a user topic-id partition to its KIP-405 metadata topic partition.
///
/// This is byte-for-byte compatible with Kafka's
/// `RemoteLogMetadataTopicPartitioner`: Java `Objects.hash` receives the
/// UUID's least-significant bits, most-significant bits, and user partition;
/// its big-endian `int` bytes are passed through Kafka Murmur2 and
/// `Utils.toPositive` before the metadata partition modulo.
#[cfg_attr(
    creusot,
    ensures(metadata_partition_count@ > 0 ==>
        exists<partition: i32> result == Some(partition)
            && partition@ == remote_metadata_partition_model(
                topic_id,
                user_partition,
                metadata_partition_count
            ))
)]
#[ensures((result == None) == (metadata_partition_count@ <= 0))]
#[ensures(forall<partition: i32> result == Some(partition) ==>
    0 <= partition@ && partition@ < metadata_partition_count@)]
#[must_use]
pub fn remote_metadata_partition(
    topic_id: u128,
    user_partition: i32,
    metadata_partition_count: i32,
) -> Option<i32> {
    if metadata_partition_count <= 0 {
        return None;
    }

    let objects_hash = java_objects_hash(topic_id, user_partition);
    let murmur2 = kafka_murmur2_i32(objects_hash);
    Some(kafka_to_positive(murmur2) % metadata_partition_count)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn matches_jvm_remote_metadata_partitioner_goldens() {
        let topic_id = 0x1234_5678_9abc_def0_0011_2233_4455_6677;
        for (partition, expected) in [(0, 6), (1, 36), (3, 2), (7, 26)] {
            check!(remote_metadata_partition(topic_id, partition, 50) == Some(expected));
        }
    }

    #[test]
    fn matches_java_signed_uuid_and_partition_extremes() {
        for (topic_id, partition, expected) in [
            (0_u128, i32::MIN, 96),
            (u128::MAX, i32::MAX, 5),
            (0xffff_ffff_ffff_ffff_8000_0000_0000_0000, i32::MIN, 45),
        ] {
            check!(remote_metadata_partition(topic_id, partition, 97) == Some(expected));
        }
    }

    #[test]
    fn rejects_nonpositive_metadata_partition_counts() {
        check!(remote_metadata_partition(0, 0, 0) == None);
        check!(remote_metadata_partition(0, 0, -1) == None);
    }
}
