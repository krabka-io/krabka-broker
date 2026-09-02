//! The serializable recording the simulator emits.
//!
//! A [`ScenarioTrace`] holds the ordered [`TraceStep`] values of one curated
//! scenario, each with a [`NodeRole`] snapshot of every node. The same types
//! back the live [`SimSnapshot`] the browser playground reads after every
//! control action, so the recorded timeline and the interactive view share one
//! vocabulary.

use crate::event::Event;

/// A complete recording of one curated failure scenario.
///
/// The recording holds the scenario identity, the invariant it shows, and the
/// ordered sequence of steps the simulator took. Each step carries a snapshot
/// of every node's role.
#[derive(serde::Serialize, Clone)]
pub struct ScenarioTrace {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub invariant: String,
    pub nodes: Vec<u64>,
    pub steps: Vec<TraceStep>,
    pub outcome: String,
}

/// One step in a scenario.
///
/// A step holds the action that occurred, a short human note, and a snapshot of
/// every node's role. The simulator takes the snapshot immediately after the
/// action.
#[derive(serde::Serialize, Clone)]
pub struct TraceStep {
    pub index: usize,
    pub clock_ms: u64,
    pub action: TraceAction,
    pub note: String,
    pub roles: Vec<NodeRole>,
}

/// The kind of action a [`TraceStep`] records.
#[derive(serde::Serialize, Clone)]
#[serde(tag = "kind")]
pub enum TraceAction {
    Deliver {
        src: u64,
        dst: u64,
        event: String,
    },
    Partition {
        node: u64,
    },
    Heal {
        node: u64,
    },
    Timeout {
        node: u64,
        #[serde(rename = "timer_kind")]
        kind: String,
    },
    Elected {
        node: u64,
        epoch: u64,
    },
    Append {
        node: u64,
        count: usize,
    },
    /// The operator deliberately discarded an in-flight message instead of
    /// delivering it. This is the interactive playground's "drop" fault.
    Drop {
        src: u64,
        dst: u64,
        event: String,
    },
}

/// A single node's observable state at a point in time.
#[derive(serde::Serialize, Clone)]
pub struct NodeRole {
    pub id: u64,
    pub role: String,
    pub epoch: u64,
    pub log_len: usize,
    pub hwm: i64,
    pub partitioned: bool,
}

/// One message sitting on the in-memory bus, waiting to be delivered.
#[derive(serde::Serialize, Clone)]
pub struct InFlight {
    pub src: u64,
    pub dst: u64,
    pub event: String,
}

/// A full, serializable snapshot of the simulation.
///
/// The browser UI reads this snapshot back after every interactive control
/// action.
#[derive(serde::Serialize, Clone)]
pub struct SimSnapshot {
    /// Logical clock in milliseconds.
    pub clock_ms: u64,
    /// Every node's observable role, epoch, and log state, ascending by id.
    pub nodes: Vec<NodeRole>,
    /// Messages currently queued on the bus, next-to-deliver first.
    pub in_flight: Vec<InFlight>,
    /// The ids of every node that currently believes it is leader.
    pub leaders: Vec<u64>,
    /// How many timeline steps the simulator has recorded so far.
    pub step_count: usize,
}

/// A short, stable label for an [`Event`] used in the rendered sequence diagram.
pub(super) fn event_label(event: &Event) -> String {
    match event {
        Event::ElectionTimeout => "ElectionTimeout".to_string(),
        Event::FetchTimeout => "FetchTimeout".to_string(),
        Event::CheckQuorumTimeout => "CheckQuorumTimeout".to_string(),
        Event::ReceiveVoteRequest { pre_vote, .. } => {
            if *pre_vote {
                "PreVoteRequest".to_string()
            } else {
                "VoteRequest".to_string()
            }
        }
        Event::ReceiveVoteResponse { vote_granted, .. } => {
            if *vote_granted {
                "VoteResponse(granted)".to_string()
            } else {
                "VoteResponse(denied)".to_string()
            }
        }
        Event::ReceiveBeginQuorumEpoch { .. } => "BeginQuorumEpoch".to_string(),
        Event::ReceiveEndQuorumEpoch { .. } => "EndQuorumEpoch".to_string(),
        Event::ReceiveFetch { .. } => "Fetch".to_string(),
        Event::ReceiveFetchSnapshot { .. } => "FetchSnapshot".to_string(),
        Event::ReceiveFetchResponse { .. } => "FetchResponse".to_string(),
    }
}
