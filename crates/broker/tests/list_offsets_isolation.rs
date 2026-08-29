//! Every `ListOffsets` answer against the bound Kafka measures it by, proved
//! over the wire against a live broker with a real transaction open.
//!
//! # Why this suite exists
//!
//! `read_committed` is the isolation level a consumer picks when it must not
//! see the records of a transaction that has not resolved. `ListOffsets` with
//! the `LATEST` sentinel is how such a consumer finds the end of a partition --
//! it is what `seekToEnd`, `endOffsets` and every lag calculation are built on.
//! A broker that answers `LATEST` from the log end offset whatever the request
//! asked for hands a `read_committed` consumer a position past records it is
//! not allowed to read, which is precisely the distinction the isolation level
//! exists to draw. Kafka answers such a request with the partition's last
//! stable offset instead, and this suite is the tier that says so on a real
//! socket through the real Kafka codecs.
//!
//! # Both isolation levels are asked on both sides of the resolution
//!
//! Every case reads `LATEST` at `read_uncommitted` *and* at `read_committed`,
//! while the transaction is open and again after it ends. The pair has to
//! disagree in the first reading and agree in the second. A case that compared
//! them only after the commit would pass against a broker that ignored
//! `isolation_level` entirely, because a resolved transaction pins nothing --
//! the disagreement while the transaction is open is the whole assertion.
//!
//! # One bound, three sentinels
//!
//! `Partition.fetchOffsetForTimestamp` picks `lastFetchableOffset` once per
//! request -- the last stable offset for a `read_committed` client, the high
//! watermark for a `read_uncommitted` one -- and then uses it twice over.
//! LATEST *is* that offset. `MAX_TIMESTAMP` and a positive-timestamp lookup
//! resolve against record data and are refused with `UNKNOWN_OFFSET` when the
//! record they matched sits at or above it. The refusal carries no error code,
//! which is what separates it from a partition that is unavailable.
//!
//! The cases below drive all three through one open transaction, because a
//! bound that is right for LATEST and ignored by the other two would let a
//! `read_committed` consumer read past its own end of partition simply by
//! asking a different way.
//!
//! # Records precede the transaction
//!
//! Each case writes two ordinary records before it opens the transaction. The
//! `read_committed` answer is then the offset the transaction started at rather
//! than zero, so a broker that answered the log start offset, or a constant, is
//! caught here too.
//!
//! # The cases
//!
//! - [`open_transaction`] -- `LATEST` on either side of a commit and an abort.
//! - [`high_watermark`] -- `LATEST` against an unacknowledged tail.
//! - [`timestamp_sentinels`] -- `MAX_TIMESTAMP` and a lookup by timestamp.
//! - [`wire`] -- the requests the cases send and the rows they expect back.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `list_offsets_isolation/` directory, which keeps the parts out of `tests/`
// where every `.rs` file would become another test binary.
#[path = "list_offsets_isolation/high_watermark.rs"]
mod high_watermark;
#[path = "list_offsets_isolation/open_transaction.rs"]
mod open_transaction;
#[path = "list_offsets_isolation/timestamp_sentinels.rs"]
mod timestamp_sentinels;
#[path = "list_offsets_isolation/wire.rs"]
mod wire;
