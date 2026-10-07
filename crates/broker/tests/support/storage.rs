//! Log-directory fixtures shared by JBOD integration suites.
use std::net::SocketAddr;

use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use tempfile::TempDir;

/// Starts a broker with two fixture log directories.
///
/// # Panics
///
/// Panics if either temporary directory cannot be created or the broker cannot start.
pub fn start_two_dir_broker()
-> impl std::future::Future<Output = (BrokerHandle, TempDir, TempDir, SocketAddr)> {
    let primary = tempfile::tempdir().unwrap();
    let extra = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(primary.path().to_path_buf());
    cfg.extra_log_dirs = vec![extra.path().to_path_buf()];
    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("broker start");
        let addr = handle.listen_addr();
        (handle, primary, extra, addr)
    })
}
