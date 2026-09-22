//! Bridges the blocking [`RemoteLogMetadataManager`] mutation SPI onto the
//! Tokio blocking pool, so no metadata write runs on a runtime worker thread.

use std::sync::Arc;

use krabka_remote_storage::RemoteLogMetadataManager;

/// Run one blocking [`RemoteLogMetadataManager`] mutation on the blocking
/// pool. The topic-backed manager's synchronous SPI methods bridge to a
/// Tokio runtime with `block_on`, which panics on a runtime worker thread.
/// `spawn_blocking` gives them a thread that is allowed to block. For the
/// in-memory manager the closure is a cheap no-op there.
/// This mirrors the `spawn_blocking` wrapping that this module already uses
/// for the blocking
/// [`RemoteStorageManager`](krabka_remote_storage::RemoteStorageManager) SPI.
pub(super) async fn rlmm_mutate<F>(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    op: F,
) -> Result<(), krabka_remote_storage::RemoteStorageError>
where
    F: FnOnce(
            &dyn RemoteLogMetadataManager,
        ) -> Result<(), krabka_remote_storage::RemoteStorageError>
        + Send
        + 'static,
{
    let rlmm = Arc::clone(rlmm);
    match tokio::task::spawn_blocking(move || op(rlmm.as_ref())).await {
        Ok(res) => res,
        Err(e) => Err(krabka_remote_storage::RemoteStorageError::Backend(format!(
            "RLMM mutation task panicked: {e}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use krabka_remote_storage::InmemoryRemoteLogMetadataManager;

    use super::*;

    #[tokio::test]
    async fn rlmm_mutate_runs_op_and_propagates_error() {
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = Arc::clone(&called);

        let res = rlmm_mutate(&rlmm, move |_| {
            called_clone.store(true, Ordering::SeqCst);
            Err(krabka_remote_storage::RemoteStorageError::Backend(
                "injected error".into(),
            ))
        })
        .await;

        assert2::check!(called.load(Ordering::SeqCst));
        assert2::check!(res.is_err());
    }
}
