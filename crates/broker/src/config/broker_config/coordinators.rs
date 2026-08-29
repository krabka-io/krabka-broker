//! The protocol feature gates and the coordinator subsystems behind them:
//! the idle-transaction reaper, and the nested configuration of the
//! consumer-group (KIP-848), share-group (KIP-932), streams-group (KIP-1071),
//! and share-coordinator subsystems.

// Link 5 of the `BrokerConfig` field chain: it appends this group to
// the fields collected so far and forwards them to `operations_fields`.
macro_rules! coordinators_fields {
    ($($collected:tt)*) => {
        $crate::config::broker_config::operations::operations_fields! {
            $($collected)*
            /// Independent compatibility and protocol feature gates.
            pub features: BrokerFeatureFlags,

            /// KIP-98 / KIP-939: how often the idle-transaction reaper scans for
            /// `Ongoing` transactions whose timeout has elapsed and aborts them. The
            /// reaper never reaps 2PC transactions. Mirrors Kafka's
            /// `transaction.abort.timed.out.transaction.cleanup.interval.ms` (10s).
            /// A zero interval disables the reaper entirely and spawns no background
            /// task. Zero is the default in `for_tests`, so a background abort does
            /// not disturb unit and integration tests. Tests that exercise the reaper
            /// set this value low explicitly.
            pub txn_abort_cleanup_interval: Time,

            /// KIP-848 next-gen consumer group protocol configuration. It controls
            /// which rebalance protocols the broker advertises, the session and
            /// heartbeat timeout bounds, and the set of enabled server-side
            /// assignors.
            pub next_gen_consumer_group: Box<crate::coordinator::unified::config::NextGenConfig>,

            /// KIP-932 share-group configuration.
            pub share_group: Box<crate::coordinator::unified::share::config::ShareGroupConfig>,

            /// KIP-1071 streams-group (Streams rebalance protocol) configuration.
            pub streams_group: Box<crate::coordinator::unified::streams::config::StreamsGroupConfig>,

            /// KIP-932 share-coordinator (persister) configuration. It controls the
            /// `__share_group_state` internal topic geometry and snapshot folding.
            pub share_coordinator: Box<crate::share_coordinator::config::ShareCoordinatorConfig>,
        }
    };
}

pub(crate) use coordinators_fields;
