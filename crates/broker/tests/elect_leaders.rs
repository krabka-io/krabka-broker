#![cfg(test)]

//! Broker-side integration tests for the operator-triggered `ElectLeaders` RPC.
//!
//! The tests drive the wire path end-to-end with a Rust PLAINTEXT client. They
//! then read the resulting partition state through `BrokerHandle` test
//! accessors.
//!
//! Both tests use a **3-broker PLAINTEXT cluster** and not a 2-broker SASL
//! cluster, for two reasons:
//!
//! * A 2-broker raft cluster cannot form a quorum (2/2) when one broker is
//!   dead. The automatic partition-leader election and the metadata commits
//!   that these tests need thus never succeed. A 3-broker cluster keeps quorum
//!   (2/3) with one dead node, which is enough for both test scenarios.
//!
//! * The compatibility shim of the authorizer maps an empty `super_users` list
//!   and zero ACLs to Allow. The test can thus exercise the full `ElectLeaders`
//!   wire path without a SASL handshake, which keeps the test helpers simpler.
//!
//! These tests are gated to non-Windows to match the multi-broker test
//! convention established by the existing integration suites. The openraft `debug_assert!` races on the
//! hosted Windows task scheduler are unrelated to the protocol under test.

mod support;

use support::cluster_lock;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `elect_leaders/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "elect_leaders/authorization.rs"]
mod authorization;
#[path = "elect_leaders/auto_rebalance.rs"]
mod auto_rebalance;
mod kafka_wire;
#[path = "elect_leaders/preferred.rs"]
mod preferred;
#[path = "elect_leaders/sasl.rs"]
mod sasl;
#[path = "elect_leaders/unclean.rs"]
mod unclean;
#[path = "elect_leaders/wait.rs"]
mod wait;
#[path = "elect_leaders/wire.rs"]
mod wire;

#[derive(Default)]
struct OperatorElectionScenario {
    lock: Option<MutexGuard<'static, ()>>,
    cluster: Vec<(
        krabka_broker::BrokerHandle,
        krabka_broker::BrokerConfig,
        tempfile::TempDir,
    )>,
    address: Option<SocketAddr>,
    revived: Option<krabka_broker::BrokerHandle>,
    retired_directory: Option<tempfile::TempDir>,
    result: Option<Vec<(i32, i16)>>,
    all_result: Option<Vec<(String, i32, i16)>>,
}
#[rstest::fixture]
fn operator_election() -> OperatorElectionScenario {
    let lock = None;
    OperatorElectionScenario {
        lock,
        ..Default::default()
    }
}
impl OperatorElectionScenario {
    fn address(&self) -> SocketAddr { self.address.expect("election endpoint") }

    async fn shutdown(&mut self) {
        if let Some(revived) = self.revived.take() {
            revived.shutdown().await;
        }
        support::shutdown_cluster(mem::take(&mut self.cluster)).await;
    }
}
#[rstest_bdd_macros::given("three plaintext brokers are registered for operator leader elections")]
async fn registered_operator_election_cluster(operator_election: &mut OperatorElectionScenario) {
    operator_election.lock = Some(cluster_lock().lock().await);
    operator_election.cluster = support::registered_cluster(3).await;
}
#[rstest_bdd_macros::then("ElectLeaders returns partition 0 with success code 0")]
fn successful_operator_election(operator_election: &OperatorElectionScenario) {
    assert2::assert!(
        operator_election
            .result
            .as_ref()
            .expect("election response")
            == &[(0, 0)]
    );
}

use {
    std::{mem, net::SocketAddr},
    tokio::sync::MutexGuard,
};
