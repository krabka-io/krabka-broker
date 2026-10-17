//! The broker epoch this process is registered at.
//!
//! A UUID, the `incarnation_id`, identifies one broker process in its
//! registration. Every process draws a new one at startup, as Kafka's
//! `BrokerLifecycleManager` does with `Uuid.randomUuid()`, so the controller can
//! tell a restart from this process registering again. The broker epoch, not
//! the incarnation, fences the previous process, and the clean-shutdown proof
//! (see `crate::clean_shutdown`), not the incarnation, decides whether a
//! restart was clean.

use {
    krabka_metadata::{MetadataImage, NodeId},
    uuid::Uuid,
};

/// The broker epoch this process is registered at: `registered_at`, the epoch
/// its startup registration was first seen committed at, or the epoch of a
/// newer registration of `node_id` under `incarnation_id` that `image` holds.
///
/// A startup registration whose submit fails can still commit: the controller
/// leader that appended it may lose the leadership before it answers, and the
/// next leader keep the record. The retry then commits a second registration
/// under the same incarnation at a newer epoch, and the controller refuses
/// every heartbeat, producer-id allocation and clean-shutdown proof that names
/// the first one with `STALE_BROKER_EPOCH`. Kafka's broker takes its epoch from
/// the answer to its last `BrokerRegistration`; krabka's registration is a
/// metadata write, and the newest registration under this incarnation is that
/// answer. A registration at an epoch older than `registered_at` never wins.
pub(crate) fn own_broker_epoch(
    image: &MetadataImage,
    node_id: NodeId,
    incarnation_id: Uuid,
    registered_at: i64,
) -> i64 {
    image
        .broker(node_id)
        .filter(|registration| registration.incarnation_id == incarnation_id)
        .map_or(registered_at, |registration| {
            registration.broker_epoch.max(registered_at)
        })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::broker_registration,
        krabka_metadata::{BrokerRegistrationRecord, MetadataRecord},
    };

    /// krabka-io/krabka-broker#1458: the epoch a broker names is the newest
    /// registration of its own incarnation, never one of another incarnation
    /// and never one older than the registration it started at.
    #[yare::parameterized(
        no_registration = { None, 20 },
        the_startup_registration = { Some((OURS, 20)), 20 },
        a_retried_registration = { Some((OURS, 35)), 35 },
        an_older_registration = { Some((OURS, 7)), 20 },
        another_incarnation = { Some((THEIRS, 50)), 20 },
    )]
    fn a_broker_names_its_newest_own_registration(registration: Option<(Uuid, i64)>, want: i64) {
        let mut image = MetadataImage::new(Uuid::nil());
        if let Some((incarnation_id, broker_epoch)) = registration {
            image.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    incarnation_id,
                    broker_epoch,
                    ..broker_registration(NodeId(4))
                },
            ));
        }
        assert2::assert!(own_broker_epoch(&image, NodeId(4), OURS, 20) == want);
    }

    const OURS: Uuid = Uuid::from_u128(0x0a);
    const THEIRS: Uuid = Uuid::from_u128(0x0b);
}
