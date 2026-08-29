//! The `AuditLog` handle and the background `AuditWriter`.
//!
//! `AuditLog::emit` is synchronous and does not block. The `AuditWriter` drains
//! events into a sink.

mod handle;
mod spool_mode;
mod writer;

#[cfg(test)]
mod test_support;

pub use self::{
    handle::AuditLog,
    writer::{AuditWriter, AuditWriterParams},
};
