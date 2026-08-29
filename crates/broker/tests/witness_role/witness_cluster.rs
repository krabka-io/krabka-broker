//! Booting the three-site stretch cluster the witness tests run on, and the
//! client and shutdown helpers that bracket each of them.
//!
//! The cluster is what makes the role observable at all: two data sites, one
//! witness site carrying `NodeRole::Witness`, `min.insync.replicas=2`, and the
//! rack-aware replica selector that the KIP-392 redirect check needs. Every
//! test in this suite starts here, so the boot lives in its own file rather
//! than beside any one of them.

use std::time::Duration;

use krabka_broker::{
    BrokerConfig, BrokerHandle, NodeId,
    config::{NodeRole, StretchProfile},
    replica_selector::ReplicaSelectorKind,
};
use krabka_client_core::Client;
use tempfile::TempDir;

use crate::{
    BROKER_WITNESS, SITE_A, SITE_C, SITES, STRETCH_PREFERRED_LEADER_SITE, support, within,
};

fn stretch_profile() -> StretchProfile {
    StretchProfile {
        sites: SITES.iter().map(|site| (*site).to_string()).collect(),
        witness_site: SITE_C.to_string(),
        preferred_leader_site: SITE_A.to_string(),
    }
}

/// Boot the three-site cluster: two data sites and one witness site, with
/// `min.insync.replicas=2` (the only value a stretch profile accepts) and the
/// rack-aware replica selector, which is what makes the KIP-392 redirect check
/// meaningful.
///
/// Retries like `support::start_n_node_with_retry`, which cannot be reused here
/// because it takes no per-broker customizer.
pub(crate) async fn start_stretch_cluster() -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let mut last_err = None;
    for attempt in 1..=3 {
        let started = support::start_n_node_with(3, |i, cfg| {
            cfg.rack = Some(SITES[i].to_string());
            cfg.stretch = Some(stretch_profile());
            cfg.default_min_insync_replicas = 2;
            cfg.replica_selector = ReplicaSelectorKind::RackAware;
            if SITES[i] == SITE_C {
                cfg.roles.push(NodeRole::Witness);
            }
        })
        .await;
        match started {
            Ok(cluster) => {
                support::wait_for_all_brokers_registered(&cluster, 3).await;
                // Placement and the produce / fetch gates read the role and the
                // preferred site out of the metadata image, so wait until both
                // records have reached every node before a topic is created.
                for (handle, _, _) in &cluster {
                    within(
                        "witness role and preferred site in the image",
                        handle.wait_for_image(|img| {
                            img.broker_config(NodeId(3))
                                .and_then(|configs| configs.get(BROKER_WITNESS))
                                .map(String::as_str)
                                == Some("true")
                                && img
                                    .default_broker_config()
                                    .and_then(|configs| configs.get(STRETCH_PREFERRED_LEADER_SITE))
                                    .map(String::as_str)
                                    == Some(SITE_A)
                        }),
                    )
                    .await;
                }
                return cluster;
            }
            Err(error) => {
                tracing::warn!(attempt, %error, "stretch cluster start failed; retrying");
                last_err = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("stretch cluster start failed after 3 attempts; last error: {last_err:?}");
}

pub(crate) async fn client_at(addr: &str) -> Client {
    Client::builder()
        .bootstrap(addr.to_string())
        .client_id("witness-role-test")
        .build()
        .await
        .expect("client build")
}

pub(crate) async fn shutdown(cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>) {
    for (handle, _, _) in cluster {
        handle.shutdown().await;
    }
}
