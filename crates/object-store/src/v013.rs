//! Compatibility exports for consumers of the former object-store 0.13 interface.
//!
//! Both interfaces now use object-store 0.14, so construction, read bounds,
//! and error classification share the canonical implementation.

pub use crate::{ObjectStoreError, build_object_store, read_capped};
