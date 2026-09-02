//! Broker registration and heartbeat admission decisions.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::DeepModel;
use creusot_std::prelude::ensures;

/// Action selected for a broker registration after parsing its wire fields.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum BrokerRegistrationDecision {
    RejectCompatibility,
    Register,
    Idempotent(i64),
    DuplicateIncarnation,
}

/// Admit a new compatible broker, preserve the exact epoch for an idempotent
/// retry of the same incarnation, and reject a competing incarnation.
#[ensures((result == BrokerRegistrationDecision::RejectCompatibility) == (
    !compatibility.0
        || !compatibility.1
        || !compatibility.2
        || !compatibility.3
        || !compatibility.4
))]
#[ensures((result == BrokerRegistrationDecision::Register) == (
    compatibility.0
        && compatibility.1
        && compatibility.2
        && compatibility.3
        && compatibility.4
        && existing_epoch == None
))]
#[ensures(forall<epoch: i64> result == BrokerRegistrationDecision::Idempotent(epoch) == (
    compatibility.0
        && compatibility.1
        && compatibility.2
        && compatibility.3
        && compatibility.4
        && existing_epoch == Some(epoch)
        && incarnation_matches
))]
#[ensures((result == BrokerRegistrationDecision::DuplicateIncarnation) == (
    compatibility.0
        && compatibility.1
        && compatibility.2
        && compatibility.3
        && compatibility.4
        && existing_epoch != None
        && !incarnation_matches
))]
#[must_use]
pub fn broker_registration_decision(
    compatibility: (bool, bool, bool, bool, bool),
    existing_epoch: Option<i64>,
    incarnation_matches: bool,
) -> BrokerRegistrationDecision {
    let (identity_valid, cluster_matches, migration_allowed, listeners_valid, features_compatible) =
        compatibility;
    if !identity_valid
        || !cluster_matches
        || !migration_allowed
        || !listeners_valid
        || !features_compatible
    {
        BrokerRegistrationDecision::RejectCompatibility
    } else {
        match existing_epoch {
            None => BrokerRegistrationDecision::Register,
            Some(epoch) if incarnation_matches => BrokerRegistrationDecision::Idempotent(epoch),
            Some(_) => BrokerRegistrationDecision::DuplicateIncarnation,
        }
    }
}

/// Registration state established by one broker heartbeat.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum BrokerHeartbeatRegistration {
    Missing,
    Stale,
    Current,
}

/// Exact response state for one broker heartbeat.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct BrokerHeartbeatDecision {
    pub registration: BrokerHeartbeatRegistration,
    pub caught_up: bool,
    pub fenced: bool,
    pub should_shut_down: bool,
}

/// Fence an absent or stale registration and otherwise derive caught-up,
/// explicit-fence, and shutdown state from the exact registered epoch.
#[ensures((result.registration == BrokerHeartbeatRegistration::Missing)
    == (registered_epoch == None))]
#[ensures((result.registration == BrokerHeartbeatRegistration::Stale)
    == exists<epoch: i64> registered_epoch == Some(epoch) && request_epoch != epoch)]
#[ensures((result.registration == BrokerHeartbeatRegistration::Current)
    == exists<epoch: i64> registered_epoch == Some(epoch) && request_epoch == epoch)]
#[ensures(result.caught_up == exists<epoch: i64>
    registered_epoch == Some(epoch) && request_epoch == epoch && metadata_offset >= epoch)]
#[ensures(result.fenced == (
    result.registration != BrokerHeartbeatRegistration::Current
        || exists<epoch: i64> registered_epoch == Some(epoch)
            && request_epoch == epoch
            && (want_fence || metadata_offset < epoch)
))]
#[ensures(result.should_shut_down == (
    result.registration == BrokerHeartbeatRegistration::Current && want_shut_down
))]
#[must_use]
pub fn broker_heartbeat_decision(
    registered_epoch: Option<i64>,
    request_epoch: i64,
    metadata_offset: i64,
    want_fence: bool,
    want_shut_down: bool,
) -> BrokerHeartbeatDecision {
    match registered_epoch {
        None => BrokerHeartbeatDecision {
            registration: BrokerHeartbeatRegistration::Missing,
            caught_up: false,
            fenced: true,
            should_shut_down: false,
        },
        Some(epoch) if request_epoch != epoch => BrokerHeartbeatDecision {
            registration: BrokerHeartbeatRegistration::Stale,
            caught_up: false,
            fenced: true,
            should_shut_down: false,
        },
        Some(epoch) => {
            let caught_up = metadata_offset >= epoch;
            BrokerHeartbeatDecision {
                registration: BrokerHeartbeatRegistration::Current,
                caught_up,
                fenced: want_fence || !caught_up,
                should_shut_down: want_shut_down,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{
        BrokerHeartbeatRegistration, BrokerRegistrationDecision, broker_heartbeat_decision,
        broker_registration_decision,
    };

    #[test]
    fn registration_is_compatible_idempotent_and_incarnation_fenced() {
        use BrokerRegistrationDecision::{
            DuplicateIncarnation, Idempotent, Register, RejectCompatibility,
        };

        check!(
            broker_registration_decision((true, true, true, true, true), None, false) == Register
        );
        check!(
            broker_registration_decision((true, true, true, true, true), Some(17), true)
                == Idempotent(17)
        );
        check!(
            broker_registration_decision((true, true, true, true, true), Some(17), false)
                == DuplicateIncarnation
        );
        check!(
            broker_registration_decision((true, false, true, true, true), None, false)
                == RejectCompatibility
        );
    }

    #[test]
    fn heartbeat_fences_exact_epochs_and_preserves_shutdown() {
        let absent = broker_heartbeat_decision(None, 7, 7, false, true);
        check!(absent.registration == BrokerHeartbeatRegistration::Missing);
        check!(absent.fenced && !absent.should_shut_down);

        let stale = broker_heartbeat_decision(Some(7), 6, i64::MAX, false, true);
        check!(stale.registration == BrokerHeartbeatRegistration::Stale);
        check!(stale.fenced && !stale.should_shut_down);

        let behind = broker_heartbeat_decision(Some(7), 7, 6, false, false);
        check!(behind.registration == BrokerHeartbeatRegistration::Current);
        check!(!behind.caught_up && behind.fenced);

        let shutdown = broker_heartbeat_decision(Some(7), 7, 7, false, true);
        check!(shutdown.caught_up && !shutdown.fenced && shutdown.should_shut_down);
    }
}
