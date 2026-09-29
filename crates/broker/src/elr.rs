//! KIP-966 eligible leader replicas: the controller state and the projection
//! `DescribeTopicPartitions` reads back out of it.
//!
//! ELR is the set of replicas that left the ISR while the partition still had
//! `min.insync.replicas` members, so their logs are known to hold every
//! committed record and the controller may elect one of them without
//! accepting data loss. Last-known ELR, in Kafka 4.3.1, is the single replica
//! that led the partition when it lost its leader, and it stays empty while
//! the partition has one. When that replica comes back, the controller elects
//! it as an unclean leader: Kafka's `canElectLastKnownLeader`. Kafka keeps both
//! on `PartitionRegistration` and reports them on
//! `DescribeTopicPartitionsResponsePartition`; `kafka-topics --describe`
//! prints them as the `Elr:` and `LastKnownElr:` columns.
//!
//! Only `DescribeTopicPartitions` carries them. `MetadataResponsePartition`
//! has no ELR field in any version of Kafka's schema, so the Metadata API
//! answers with `error_code`, `leader`, `replicas`, `isr` and
//! `offline_replicas` and nothing more; there is no encoding on that API for
//! a broker to get wrong.
//!
//! ## Where the state lives
//!
//! Krabka publishes ELR as `V1PartitionElr`, which translates to Kafka's
//! standard `PartitionChangeRecord` fields. Publishing it through the
//! metadata log lets every node answer with the same columns and makes the
//! state survive replay, snapshots, and mixed Kafka/Krabka controller logs.
//!
//! ## The two halves
//!
//! [`state`] projects one partition's two lists. [`maintain`] is the
//! controller half: every path that changes a
//! partition's ISR or leader hands its emitted records to an
//! [`ElrPublisher`], which recomputes the affected partitions and appends the
//! `V1PartitionElr` records that carry the new state.
//!
//! The columns are not the only reader. KIP-966's point is that the set is
//! *elected from*, and two paths do. `unclean_recovery`'s
//! [`select_leader`](crate::unclean_recovery::select_leader) reads this
//! projection and elects a surviving ELR member ahead of any longer log that
//! is not one, because only the ELR member is known to hold every committed
//! record. The failover scans read it first, through
//! [`failover_one`](crate::leader_election::failover_one): a partition whose
//! live ISR has emptied elects a surviving ELR member outright and cleanly,
//! without consulting `unclean.leader.election.enable` or
//! `unclean.recovery.strategy`, which is Apache Kafka's
//! `PartitionChangeBuilder.electAnyLeader`.
//!
//! ## A partition without a leader
//!
//! When nothing can be elected Kafka writes `leader = -1`. A krabka partition
//! record always names a leader, so the scans leave it as it is and publish
//! the last leader as the last-known ELR through [`ElrPublisher::leaderless`];
//! [`state::is_leaderless`] reads that one-member set back, and `Metadata` and
//! `DescribeTopicPartitions` report the partition as `leader = -1` with an ISR
//! that has lost the last leader. The election that gives it a leader again is
//! [`compute_unfence_changes`](crate::leader_election::compute_unfence_changes),
//! which the `BrokerHeartbeat` that unfences a broker runs, as Kafka's
//! `handleBrokerUnfenced` runs its election over `partitionsWithNoLeader`.
//!
//! That is also why the state has to be withdrawn when it stops being true.
//! The one event that ends a membership without any partition changing is a
//! broker coming back from a stop it cannot prove was clean, whose current log
//! need not be the log that made it eligible. [`unclean_restart`] holds that
//! rule and withdraws the published half of it. The published half is not the
//! whole of it, though: the next eligibility is derived from the ISRs the
//! image still holds, so
//! [`compute_unclean_restart_changes`](crate::leader_election::compute_unclean_restart_changes)
//! -- what the registration handler calls once the proof fails -- wraps the
//! withdrawal with the matching ISR removals and runs [`ElrPublisher`] over
//! the whole batch with the broker excluded, so the batch cannot re-derive
//! what it just withdrew. The withdrawal takes the broker out of the eligible
//! sets and nothing else: it does not move into the last-known ELR, which
//! belongs to the last leader of a partition that has none. A broker that
//! *can* prove it -- the clean-shutdown record [`crate::clean_shutdown`]
//! keeps, offered back as `previousBrokerEpoch` -- keeps its membership,
//! because its log is still the log the claim was about.

pub(crate) mod maintain;
pub(crate) mod state;
pub(crate) mod unclean_restart;

#[cfg(test)]
mod tests;

pub(crate) use self::{
    maintain::ElrPublisher,
    state::TopicElr,
    unclean_restart::{clear_published_elr, withdraw_elr_membership},
};
