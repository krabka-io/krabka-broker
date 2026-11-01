//! Native KIP-1331 storage SPI. This is a Rust embedding interface, not a JVM
//! class loader or a stable dynamic-library ABI. Implementations must be safe
//! to share across groups, cancellation-safe, idempotent on retries, and scoped
//! to the Kafka cluster. The coordinator serializes operations for each group.
use {
    super::TopologyDescription,
    std::{fmt::Debug, future::Future, pin::Pin, ptr, sync::Arc},
};

/// An asynchronous storage operation, borrowing its backend and arguments.
pub type BackendFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, StorageFailure>> + Send + 'a>>;

/// Kafka distinguishes permanent rejection of a topology from retryable backend
/// failure. Messages are deliberately not forwarded to clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFailure {
    Transient,
    Permanent,
}

/// A backend supplied through `StreamsGroupConfig::topology_description_backend`.
/// `get` must return `None` for a missing or mismatched epoch. `delete` must
/// succeed when the group is already absent. After success, `set` must be visible
/// to `get`, including after coordinator movement when the backend is shared.
/// Implementations own connection pools and release them in `Drop`; futures
/// must complete in bounded time (including their own IO timeout).
pub trait TopologyDescriptionBackend: Debug + Send + Sync {
    fn set<'a>(
        &'a self,
        group_id: &'a str,
        epoch: i32,
        description: &'a TopologyDescription,
    ) -> BackendFuture<'a, ()>;
    fn get<'a>(
        &'a self,
        group_id: &'a str,
        epoch: i32,
    ) -> BackendFuture<'a, Option<TopologyDescription>>;
    fn delete<'a>(&'a self, group_id: &'a str) -> BackendFuture<'a, ()>;
}

/// Optional native backend installed by an embedding application.
pub type NativeBackend = Option<Arc<dyn TopologyDescriptionBackend>>;

// Configuration equality compares backend identity, not mutable stored data.
impl PartialEq for dyn TopologyDescriptionBackend {
    fn eq(&self, other: &Self) -> bool { ptr::eq(self, other) }
}
impl Eq for dyn TopologyDescriptionBackend {}
