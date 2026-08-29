//! The bounded shape of the modelled cluster and of the producer client: the
//! broker count, the single producer identity, and the caps that keep the
//! search finite.
//!
//! Every bound is decided before the search begins and only read afterwards,
//! so they sit apart from the state that the search moves.

pub const NB: usize = 3;
pub const INITIAL_LEADER: usize = 0;
pub const PRODUCER_ID: i64 = 1;
pub const PRODUCER_EPOCH: i16 = 0;
pub const BASE_SEQUENCE: i32 = 0;
pub const BASE_OFFSET: i64 = 0;
pub const MAX_LOG_LEN: usize = 1;
pub const MAX_HWM: u8 = 1;
pub const MAX_SEND_ATTEMPTS: u8 = 4;
pub const MAX_METADATA_REFRESHES: u8 = 4;
