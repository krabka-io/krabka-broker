//! Broker-scoped reference-plugin data and solicitation windows.
//!
//! Kafka owns these in its broker-wide topology manager, not its reloaded
//! coordinator shard. The per-group handle survives actor replacement, but a
//! new broker/coordinator instance starts with empty runtime state.
use {
    super::{Jitter, SolicitationBackoff, StoredDescription, TopologyDescription},
    std::{
        sync::{Mutex, PoisonError},
        time::Instant,
    },
};

#[derive(Debug, Default)]
struct State {
    description: Option<StoredDescription>,
    backoff: SolicitationBackoff,
}

/// One group's broker-local plugin and retry state, shared by replacement actors.
#[derive(Debug, Default)]
pub struct DescriptionRuntime {
    state: Mutex<State>,
}

impl DescriptionRuntime {
    fn with<R>(&self, operation: impl FnOnce(&mut State) -> R) -> R {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        operation(&mut state)
    }

    pub fn store(&self, epoch: i32, description: TopologyDescription) {
        self.with(|state| {
            state.description = Some(StoredDescription {
                topology_epoch: epoch,
                description,
            });
        });
    }

    #[must_use]
    pub fn get(&self, epoch: i32) -> Option<TopologyDescription> {
        self.with(|state| {
            state
                .description
                .as_ref()
                .filter(|stored| stored.topology_epoch == epoch)
                .map(|stored| stored.description.clone())
        })
    }

    pub fn delete(&self) { self.with(|state| state.description = None); }

    pub fn arm_if_not_active(&self, epoch: i32, now: Instant, jitter: Jitter) -> bool {
        self.with(|state| state.backoff.arm_if_not_active(epoch, now, jitter))
    }

    pub fn arm_or_extend(&self, epoch: i32, now: Instant, jitter: Jitter) {
        self.with(|state| state.backoff.arm_or_extend(epoch, now, jitter));
    }

    pub fn clear(&self, epoch: i32) { self.with(|state| state.backoff.clear(epoch)); }

    pub fn reset_backoff(&self) {
        self.with(|state| state.backoff = SolicitationBackoff::default());
    }
}
