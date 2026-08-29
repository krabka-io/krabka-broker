//! The [`BrokerConfig`] struct: every knob the broker reads at construction
//! time.
//!
//! A struct is one item, so it cannot be cut across modules the way a set of
//! functions can. Each child module holds one contiguous run of the fields
//! inside a `macro_rules!` that appends its run to the fields collected so
//! far and forwards them to the next child. The last child calls
//! `define_broker_config`, which emits the struct here. The order of the
//! chain is the declaration order of the struct, so every field keeps the
//! position it has always had.

use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};

use krabka_log::LogConfig;
use krabka_raft::{
    BootstrapMode, ControllerFetchMissLimit, MetadataRaftCommandQueueCapacity,
    MetadataRaftFetchMax, NodeId,
};
use krabka_security::{SaslMechanism, TlsConfig};
use krabka_units::{ByteSize, Ratio, Time};

use crate::{
    config::{
        BreakGlassConfig, BrokerFeatureFlags, FreezeConfig, InterBrokerCredentials, ListenerSpec,
        NodeRole, RemoteStorageBackend, ReplicationRuntimeConfig, RlmmKind, StretchProfile,
    },
    operator_keys::OperatorKeys,
};

mod audit;
mod coordinators;
mod delegation_tokens;
mod identity;
mod operations;
mod quorum;
mod remote_storage;
mod security;
mod tuning;

// The terminal link of the field chain: it emits the struct from every
// field group the chain collected.
macro_rules! define_broker_config {
    ($($field:tt)*) => {
        #[derive(Debug, Clone)]
        // a broad config struct; flags are independent knobs
        pub struct BrokerConfig {
            $($field)*
        }
    };
}

pub(crate) use define_broker_config;

// Head of the field chain. It runs through every child module in turn and
// ends in `define_broker_config`.
self::tuning::tuning_fields! {}
