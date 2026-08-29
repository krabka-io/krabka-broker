//! Sequential reference spec of the committed log, plus the identity domains
//! the linearizability tester keys its history by. It is its own file because
//! it is the model's correctness *oracle*, independent of how the consensus
//! core reaches a commit.

use stateright::semantics::SequentialSpec;

/// Identifies a client request thread for the linearizability tester. Each
/// append uses a fresh id, so every "thread" has exactly one in-flight
/// operation: one invoke and one return.
pub type ClientId = u64;

/// Stateless appenders are exchangeable in this model. Keeping a separate,
/// tiny identity domain avoids accidentally treating an appender as the WAL
/// voter with the same id.
pub type AppenderId = u8;
pub(super) const APPENDER_COUNT: AppenderId = 2;

/// Sequential reference model of the committed log. An append returns the
/// assigned offset, and a read returns the committed value sequence.
///
/// A committed Kafka log is an append-only sequence and not a single-value
/// register, so this module defines its own `SequentialSpec` instead of a reuse
/// of the built-in register. A regular `KRaft` append linearizes when the high
/// watermark passes its offset; a diskless append linearizes when a WAL
/// majority has durably stored it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct KraftLogSpec {
    committed: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogOp {
    Append(u64),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogRet {
    /// The committed client-value prefix, in commit order, up to and including
    /// this append, observed at the moment it committed.
    ///
    /// The value prefix avoids the leader-change control records that occupy
    /// raft offsets, which a physical offset would not. It also makes a lost or
    /// reordered committed entry produce an unserializable history.
    Committed(Vec<u64>),
}

impl SequentialSpec for KraftLogSpec {
    type Op = LogOp;
    type Ret = LogRet;

    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        let LogOp::Append(v) = op;
        self.committed.push(*v);
        LogRet::Committed(self.committed.clone())
    }
}
