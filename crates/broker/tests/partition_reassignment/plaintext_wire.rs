//! PLAINTEXT wire helpers for the partition-reassignment suite: the client id
//! every request carries, and the `CreateTopics` call that provisions a topic
//! before a test drives a reassignment.
//!
//! The authorizer compatibility shim allows every request when there are no
//! `super_users` and no ACLs, so nothing here performs a SASL handshake.

use std::net::SocketAddr;

use tokio::net::TcpStream;

use crate::kafka_wire;

/// The client id every request header in this suite carries.
pub const CLIENT_ID: &str = "krabka-reassign-test";

/// Creates a topic over PLAINTEXT. The authorizer's compat shim, with no
/// `super_users` and no ACLs, lets the request through. Copied from
/// `elect_leaders.rs`.
pub async fn create_topic_plaintext(
    addr: SocketAddr,
    name: &str,
    partitions: i32,
    replication_factor: i16,
) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    kafka_wire::create_topic_on(
        &mut stream,
        CLIENT_ID,
        kafka_wire::topic(name, partitions, replication_factor),
    )
    .await;
}
