//! Peer identity and distinct-rack placement for native diskless WAL clusters.

/// Preserve the lazy ordered peer credential construction and checked node ids.
///
/// # Panics
/// Panics if the original peer index arithmetic or checked node id is out of range.
pub fn peer_credentials<'a>(
    voters: usize,
    password: &'a str,
    principal: impl Fn(u64) -> String + 'a,
) -> impl Iterator<Item = (String, String)> + 'a {
    (0..voters).map(move |peer| {
        (
            principal(u64::try_from(peer + 1).expect("small cluster")),
            password.to_owned(),
        )
    })
}

/// Set the broker's authenticated identity before its distinct rack and replica count.
/// `select_voters` picks one broker per unused rack; a shared rack would yield
/// two voters and make the WAL reconcile loop refuse the three-voter placement.
///
/// # Panics
/// Panics if the rack index does not fit the original byte arithmetic.
pub fn configure_identity(
    config: &mut krabka_broker::BrokerConfig,
    index: usize,
    node: u64,
    voters: usize,
    password: &str,
    principal: impl FnOnce(u64) -> String,
) {
    config.inter_broker_credentials = Some(krabka_broker::config::InterBrokerCredentials::Plain {
        username: principal(node),
        password: password.to_owned(),
    });
    config.rack = Some(format!(
        "rack-{}",
        char::from(b'a' + u8::try_from(index).expect("small cluster"))
    ));
    config.diskless_wal_local_replica_count = voters;
}
