//! The two shapes a `SetTopicFreeze` request takes once the checks have run.
//!
//! Every phase after the checks reads one of them. The raft append needs the
//! records an accepted request became, and the response needs the code and the
//! text that a refused one carries.

use krabka_metadata::{MetadataRecord, TopicFreezeRecord};

/// A request the broker did not accept, and the text that says why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Refusal {
    pub(super) code: i16,
    pub(super) message: String,
    /// Whether a signature was present and verified before the refusal. It is
    /// `false` for every refusal that a signature check itself produced.
    pub(super) signature_verified: bool,
}

impl Refusal {
    pub(super) fn new(code: i16, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            signature_verified: false,
        }
    }
}

/// A request the broker accepted, with everything the raft append needs.
pub(super) struct Accepted {
    /// The registry record the append writes.
    pub(super) record: TopicFreezeRecord,
    /// The break-glass proposal the append spends, on a thaw.
    pub(super) consumed_proposal: Option<MetadataRecord>,
    /// Whether the broker verified a signature on the record.
    pub(super) signature_verified: bool,
}
