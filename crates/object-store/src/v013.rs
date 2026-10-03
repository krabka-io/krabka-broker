//! Object-store 0.13 interfaces for DataFusion and Parquet consumers.
//!
//! Construction, read bounds, and error classification use the same
//! implementations as the current object-store interface.

use object_store_013 as object_store_api;

#[path = "build.rs"]
mod build;
#[path = "error.rs"]
mod error;
#[path = "read.rs"]
mod read;

pub use build::build_object_store;
pub use error::ObjectStoreError;
pub use read::read_capped;
