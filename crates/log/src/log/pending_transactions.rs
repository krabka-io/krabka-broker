//! Producer lookup and ordered bounds of the same live transaction starts.
//!
//! Keeping mutation here ensures that a non-minimal corrupt start is still
//! visible to LSO validation without walking every producer on each append.

use {
    krabka_ids::{Offset, ProducerId},
    std::collections::{BTreeMap, HashMap, hash_map},
};

#[derive(Debug, Default)]
pub(super) struct PendingTransactions {
    producers: HashMap<ProducerId, Offset>,
    starts: BTreeMap<Offset, usize>,
}

impl PendingTransactions {
    pub(super) fn get(&self, producer: &ProducerId) -> Option<&Offset> {
        self.producers.get(producer)
    }

    pub(super) fn contains_key(&self, producer: &ProducerId) -> bool {
        self.producers.contains_key(producer)
    }

    pub(super) fn is_empty(&self) -> bool { self.producers.is_empty() }

    pub(super) fn values(&self) -> hash_map::Values<'_, ProducerId, Offset> {
        self.producers.values()
    }

    pub(super) fn iter(&self) -> hash_map::Iter<'_, ProducerId, Offset> { self.producers.iter() }

    pub(super) fn bounds(&self) -> Option<(Offset, Offset)> {
        Some((
            *self.starts.first_key_value()?.0,
            *self.starts.last_key_value()?.0,
        ))
    }

    pub(super) fn insert_first(&mut self, producer: ProducerId, start: Offset) {
        if let hash_map::Entry::Vacant(entry) = self.producers.entry(producer) {
            entry.insert(start);
            *self.starts.entry(start).or_default() += 1;
        }
    }

    pub(super) fn insert(&mut self, producer: ProducerId, start: Offset) {
        if let Some(previous) = self.producers.insert(producer, start) {
            self.remove_start(previous);
        }
        *self.starts.entry(start).or_default() += 1;
    }

    pub(super) fn remove(&mut self, producer: &ProducerId) -> Option<Offset> {
        let start = self.producers.remove(producer)?;
        self.remove_start(start);
        Some(start)
    }

    fn remove_start(&mut self, start: Offset) {
        let entry = self
            .starts
            .get_mut(&start)
            .expect("every pending producer has an indexed start");
        *entry -= 1;
        if *entry == 0 {
            self.starts.remove(&start);
        }
    }

    pub(super) fn clear(&mut self) {
        self.producers.clear();
        self.starts.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_starts_remain_until_every_producer_is_removed() {
        let mut pending = PendingTransactions::default();
        pending.insert(ProducerId(1), Offset(3));
        pending.insert(ProducerId(2), Offset(3));
        pending.insert(ProducerId(3), Offset(9));
        pending.remove(&ProducerId(1));
        assert2::assert!(pending.bounds() == Some((Offset(3), Offset(9))));
        pending.insert(ProducerId(2), Offset(9));
        assert2::assert!(pending.bounds() == Some((Offset(9), Offset(9))));
        pending.remove(&ProducerId(2));
        assert2::assert!(pending.bounds() == Some((Offset(9), Offset(9))));
        pending.remove(&ProducerId(3));
        assert2::assert!(pending.bounds().is_none());
    }

    #[test]
    fn mutations_preserve_all_start_bounds() {
        let mut indexed = PendingTransactions::default();
        let mut oracle = HashMap::new();
        let mut random = 17_u64;
        for step in 0..10_000 {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let producer = ProducerId(i64::try_from(random % 64).unwrap());
            let start = Offset(i64::try_from((random >> 8) % 100).unwrap() - 5);
            match step % 7 {
                0 | 1 => {
                    indexed.insert(producer, start);
                    oracle.insert(producer, start);
                }
                2 | 3 => {
                    indexed.insert_first(producer, start);
                    oracle.entry(producer).or_insert(start);
                }
                4 | 5 => {
                    assert2::assert!(indexed.remove(&producer) == oracle.remove(&producer));
                }
                _ if step % 97 == 0 => {
                    indexed.clear();
                    oracle.clear();
                }
                _ => {}
            }
            let bounds = oracle
                .values()
                .min()
                .copied()
                .zip(oracle.values().max().copied());
            assert2::assert!(indexed.bounds() == bounds);
            let starts: Vec<_> = oracle.values().map(|start| start.0).collect();
            for end in [0, 25, 50, 100] {
                let compact = indexed
                    .bounds()
                    .map_or([end; 2], |(first, last)| [first.0, last.0]);
                assert2::assert!(
                    krabka_verified::first_unstable_offset(&compact, end)
                        == krabka_verified::first_unstable_offset(&starts, end)
                );
            }
        }
    }
}
