//! Shared fixtures for the quorum state machine unit tests: two [`LogView`]
//! stubs and the constructors that build a machine over a bootstrap voter
//! set.
//!
//! The tests live in one module per event family, and every one of them
//! needs these builders, so they sit here rather than in any single family.

use krabka_units::prelude::{Time, secs};

use super::QuorumStateMachine;
use crate::types::{Epoch, LogView, NodeId, QuorumState};

pub struct FakeLog {
    pub end: i64,
    pub last_epoch: Epoch,
}
impl LogView for FakeLog {
    fn end_offset(&self) -> i64 {
        self.end
    }
    fn last_epoch(&self) -> Epoch {
        self.last_epoch
    }
    fn end_offset_for_epoch(&self, epoch: Epoch) -> Option<i64> {
        if epoch <= self.last_epoch {
            Some(self.end)
        } else {
            None
        }
    }
}
/// A `LogView` whose `end_offset` can change between calls.
///
/// A test can then model a leader that is promoted at a small log end, that
/// is, a low `epoch_start_offset`, and whose log grows before followers
/// fetch.
pub struct CellLog {
    pub end: std::cell::Cell<i64>,
    pub last_epoch: Epoch,
}
impl LogView for CellLog {
    fn end_offset(&self) -> i64 {
        self.end.get()
    }
    fn last_epoch(&self) -> Epoch {
        self.last_epoch
    }
    fn end_offset_for_epoch(&self, epoch: Epoch) -> Option<i64> {
        if epoch <= self.last_epoch {
            Some(self.end.get())
        } else {
            None
        }
    }
}
pub fn voters(ids: &[NodeId]) -> krabka_voters::VoterSet {
    krabka_voters::VoterSet::from_voters(ids.iter().map(|&id| krabka_voters::Voter {
        id,
        directory_id: uuid::Uuid::nil(),
        endpoints: vec![],
        kraft_version: krabka_voters::KRaftVersionRange::default(),
    }))
}
pub fn machine(me: NodeId, ids: &[NodeId]) -> QuorumStateMachine {
    QuorumStateMachine::new(
        me,
        QuorumState::bootstrap(uuid::Uuid::nil(), voters(ids)),
        TEST_ELECTION_TIMEOUT,
    )
}

/// The base election timeout for every test machine.
pub const TEST_ELECTION_TIMEOUT: Time = secs(1);
