//! The marker fan-out of one injection.
//!
//! The coordinator freezes the target set, and then writes one marker into
//! every partition of that set. A partition this broker leads takes
//! [`Partition::produce_control_batch`](crate::partition::Partition::produce_control_batch),
//! which applies no compression rewrite and keeps the control batch
//! byte-exact. A partition another broker leads takes the
//! [`RemoteMarkerWriter`] seam.
//!
//! The fan-out collects the offset that each append returned, because those
//! offsets are the cut. It retries the partitions that carry no marker until
//! its deadline runs out. A leader that is down or mid-election is the common
//! failure, and it usually resolves inside the deadline.
//!
//! One concern per module: `plan` holds the pure decisions the fan-out makes
//! before it writes anything, `transport` holds the seam that carries a marker
//! to another broker, `append` holds the single local append that both the
//! fan-out and the `WriteBarrierMarkers` handler go through, and `fanout`
//! drives the retry loop over all three.

mod append;
mod fanout;
mod plan;
mod transport;

#[cfg(test)]
mod test_support;

#[cfg(test)]
pub(crate) use self::transport::MockRemoteMarkerWriter;
pub(crate) use self::{
    append::{MarkerAppendError, append_marker},
    fanout::MarkerFanout,
    plan::{backoff_for, freeze_targets, group_by_leader},
    transport::{MarkerPlacement, RemoteMarkerWriter},
};
