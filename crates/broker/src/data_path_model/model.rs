//! The model configuration and checker interface. Enabled actions, transitions
//! and properties live in child modules; transitions drive the production seams.

use std::time::Instant;

use krabka_log::Offset;
use krabka_metadata::{MetadataImage, MetadataRecord, PartitionOffsetAdvanceRecord};
use stateright::{Model, Property};

use super::{
    bounds::{MAX_EPOCH, MAX_LEN, NB, NB_U8, has, model_index, model_offset},
    election::do_failover,
    elr,
    hwm::{consistent_leo, real_hwm, real_wal_hwm},
    state::{Act, DpState, ELR_BEAT_LONGER_LOG, ELR_DROPPED_COMMITTED, ELR_ELECTED, isr_eligible},
    truncation::real_truncation_offset,
};
use crate::handlers::fetch::{FetchWatermarks, compute_visibility_window};

pub(super) struct DpModel {
    pub(super) base: Instant,
    pub(super) unclean: bool,  // false in DPC-1/2 (clean), true in DPC-3
    pub(super) diskless: bool, // true drives the WAL durability path instead of ISR-HWM
    /// The metadata image the real controller rules read. It carries the
    /// topic's `min.insync.replicas`, which is what decides both when the ELR
    /// rule clears the set and when the leader's high watermark stops.
    image: MetadataImage,
    /// `min.insync.replicas` as [`effective_min_insync_replicas`] resolves it
    /// out of [`Self::image`], not a second copy of the number.
    ///
    /// [`effective_min_insync_replicas`]: crate::config_keys::effective_min_insync_replicas
    min_isr: usize,
    /// The longest log this configuration lets a broker reach, at most
    /// [`MAX_LEN`]. Each extra record multiplies the reachable states, and the
    /// ELR configuration carries more per-state than the others, so it buys
    /// its ELR bookkeeping back out of its log length.
    max_len: usize,
}

impl DpModel {
    /// A configuration of the modelled cluster. `min_isr` is the topic's
    /// `min.insync.replicas`: at 1 no partition can ever have a non-empty ELR,
    /// because the rule clears the set as soon as the ISR meets the threshold
    /// and an ISR that reached zero has no partition record left to reach it
    /// with, so only a configuration above 1 exercises the ELR rule.
    pub(super) fn config(
        base: Instant,
        unclean: bool,
        diskless: bool,
        min_isr: usize,
        max_len: usize,
    ) -> Self {
        assert2::assert!(
            max_len <= MAX_LEN,
            "the cast helpers are bounded by MAX_LEN"
        );
        assert2::assert!(
            min_isr == 1 || (unclean && !diskless),
            "only the unclean replicated configuration is configured with an ELR"
        );
        let image = elr::image(min_isr);
        let min_isr = elr::min_insync_replicas(&image);
        Self {
            base,
            unclean,
            diskless,
            image,
            min_isr,
            max_len,
        }
    }

    /// Whether this configuration maintains and checks KIP-966 ELR state.
    ///
    /// At Kafka's default `min.insync.replicas` of 1 the rule clears the set
    /// on every change a live partition can make, so the other configurations
    /// would carry an always-empty set through every state they enumerate --
    /// state identity they pay for in the search and get nothing back from.
    /// They leave it empty instead, and the ELR properties are stated only
    /// here, where a `sometimes` property has states that can witness it.
    fn tracks_elr(&self) -> bool {
        self.min_isr > 1
    }

    /// Reserve `count` diskless offsets the way the controller does, over a
    /// partition whose committed next offset is `committed_next`.
    ///
    /// The controller's `submit_change` takes the base from the image's
    /// `partition_next_offset`, reserves the range with the proved
    /// [`reserve_offsets`](krabka_verified::reserve_offsets), and commits a
    /// `PartitionOffsetAdvance` whose replay bumps the image's next offset.
    /// This runs those three production pieces in that order and returns the
    /// reserved base and the next offset the image holds afterwards, so
    /// `offsets_contiguous_and_unique` checks what they compute rather than
    /// what the model wrote down.
    ///
    /// The image the state stands for is rebuilt from its one number: every
    /// advance adds to the same counter, so a single advance of
    /// `committed_next` replays to the same image as the history that reached
    /// it.
    fn reserve(&self, committed_next: i64, count: i64) -> (i64, i64) {
        let advance = |count| {
            MetadataRecord::V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord {
                topic: elr::TOPIC.to_string(),
                partition: elr::PARTITION,
                count,
            })
        };
        let mut image = self.image.clone();
        if committed_next > 0 {
            image.apply(&advance(committed_next));
        }
        let next_offset = image
            .partition_next_offset(elr::TOPIC, elr::PARTITION)
            .unwrap_or(0);
        let (base, _) = krabka_verified::reserve_offsets(next_offset, count)
            .expect("a bounded model reservation is representable");
        image.apply(&advance(count));
        let next = image
            .partition_next_offset(elr::TOPIC, elr::PARTITION)
            .expect("the advance just applied");
        (base, next)
    }
}

impl Model for DpModel {
    type State = DpState;
    type Action = Act;

    fn init_states(&self) -> Vec<Self::State> {
        self.model_init_states()
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        self.model_actions(s, acts);
    }

    fn next_state(&self, last: &Self::State, a: Self::Action) -> Option<Self::State> {
        self.model_next_state(last, a)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        self.model_properties()
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.log.iter().all(|l| l.len() <= self.max_len) && s.leader_epoch <= MAX_EPOCH + 1
    }
}

#[path = "model/actions.rs"]
mod actions;

#[path = "model/transitions.rs"]
mod transitions;

#[path = "model/invariants.rs"]
mod invariants;
