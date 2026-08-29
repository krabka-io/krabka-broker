//! The two sparse per-segment indexes: the `.index` file, which maps a
//! relative offset to a byte position in the segment's log, and the
//! `.timeindex` file, which maps a timestamp to a relative offset.
//!
//! One submodule per on-disk artifact. `offset` holds the 8-byte `.index`
//! entry layout, its loader and its floor lookup; `time` holds the 12-byte
//! `.timeindex` entry layout, its loader and its timestamp lookup. Both
//! layouts are byte-compatible with Kafka's, so each entry's encode, decode
//! and binary-search paths stay beside the layout they read.

mod offset;
mod time;

pub use self::{offset::OffsetIndex, time::TimeIndex};
