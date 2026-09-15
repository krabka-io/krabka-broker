//! Registration table for the apis whose handler needs no per-request context:
//! the dispatcher passes the raw body straight to the `handle` function.

use krabka_protocol::api_key::ApiKey;

use super::{DispatchEntry, DispatchRegistry};

plain_dispatches!(register_plain_dispatches;
    (AssignReplicasToDirs, assign_replicas_to_dirs_request, crate::handlers::assign_replicas_to_dirs::handle),
);
