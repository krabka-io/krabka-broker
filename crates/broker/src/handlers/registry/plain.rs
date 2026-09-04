//! Registration table for the apis whose handler needs no per-request context:
//! the dispatcher passes the raw body straight to the `handle` function.

use krabka_protocol::api_key::ApiKey;

use super::{DispatchEntry, DispatchRegistry};

plain_dispatches!(register_plain_dispatches;
    (AllocateProducerIds, allocate_producer_ids_request, crate::handlers::allocate_producer_ids::handle),
    (AddOffsetsToTxn, add_offsets_to_txn_request, crate::txn::handlers::add_offset_commits_to_txn::handle),
    (WriteTxnMarkers, write_txn_markers_request, crate::txn::handlers::write_txn_markers::handle),
    (FetchSnapshot, fetch_snapshot_request, crate::handlers::fetch_snapshot::handle),
    (AssignReplicasToDirs, assign_replicas_to_dirs_request, crate::handlers::assign_replicas_to_dirs::handle),
    (InitializeShareGroupState, initialize_share_group_state_request, crate::share_coordinator::handlers::initialize::handle),
    (ReadShareGroupState, read_share_group_state_request, crate::share_coordinator::handlers::read::handle),
    (WriteShareGroupState, write_share_group_state_request, crate::share_coordinator::handlers::write::handle),
    (DeleteShareGroupState, delete_share_group_state_request, crate::share_coordinator::handlers::delete::handle),
    (ReadShareGroupStateSummary, read_share_group_state_summary_request, crate::share_coordinator::handlers::read_summary::handle),
);
