//! The registry of in-progress log-directory moves.
//!
//! A move removes only its own entry: it compares the entry it finds with
//! `Arc::ptr_eq`, because a later move of the same partition may already have
//! replaced it.

use {super::FutureLogState, dashmap::DashMap, krabka_ids::PartitionIndex, std::sync::Arc};

/// KIP-113: the in-progress intra-broker log-dir moves, one entry per
/// `(topic, partition)` that the broker currently copies to another log.dir.
#[derive(Debug, Default)]
pub struct FutureLogRegistry(DashMap<(String, PartitionIndex), Arc<FutureLogState>>);

impl FutureLogRegistry {
    /// The move of `key`, if one is in progress.
    #[must_use]
    pub fn get(&self, key: &(String, PartitionIndex)) -> Option<Arc<FutureLogState>> {
        self.0.get(key).map(|entry| Arc::clone(entry.value()))
    }

    /// Register `state` as the move of `key`, in place of any earlier entry.
    pub fn insert(&self, key: (String, PartitionIndex), state: Arc<FutureLogState>) {
        self.0.insert(key, state);
    }

    /// Remove the entry of `key` if it is `state` itself, and leave an entry
    /// that a later move put there.
    pub fn remove_if_same(&self, key: &(String, PartitionIndex), state: &Arc<FutureLogState>) {
        self.0.remove_if(key, |_, entry| Arc::ptr_eq(entry, state));
    }

    /// Every in-progress move with its key.
    #[must_use]
    pub fn entries(&self) -> Vec<((String, PartitionIndex), Arc<FutureLogState>)> {
        self.0
            .iter()
            .map(|entry| (entry.key().clone(), Arc::clone(entry.value())))
            .collect()
    }

    /// Remove every move and return them.
    pub(super) fn take_all(&self) -> Vec<Arc<FutureLogState>> {
        let states: Vec<_> = self
            .0
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        self.0.clear();
        states
    }

    #[cfg(test)]
    #[must_use]
    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    #[cfg(test)]
    #[must_use]
    pub fn len(&self) -> usize { self.0.len() }
}
