use {
    super::*,
    assert2::{assert, check},
    krabka_security::ListenerProtocol::{self, Plaintext, SaslPlaintext, Ssl},
    std::io::ErrorKind,
};

fn endpoint(name: &str, host: Option<&str>, port: i32, protocol: ListenerProtocol) -> Endpoint {
    Endpoint {
        name: name.to_owned(),
        protocol,
        host: host.map(str::to_owned),
        port,
    }
}

fn plaintext(name: &str, port: i32) -> Endpoint {
    endpoint(name, Some("127.0.0.1"), port, Plaintext)
}

fn names(names: &[&str]) -> Vec<String> { names.iter().map(|name| (*name).to_owned()).collect() }

/// Which listeners a change starts and stops, against what runs and what
/// failed before.
#[yare::parameterized(
    unchanged = {
        &["PLAINTEXT"], &[], vec![plaintext("PLAINTEXT", 9092)],
        ListenerChanges::default(),
    },
    added = {
        &["PLAINTEXT"], &[], vec![plaintext("PLAINTEXT", 9092), plaintext("EXTRA", 9093)],
        ListenerChanges { start: vec![plaintext("EXTRA", 9093)], stop: vec![] },
    },
    removed = {
        &["PLAINTEXT", "EXTRA"], &[], vec![plaintext("PLAINTEXT", 9092)],
        ListenerChanges { start: vec![], stop: names(&["EXTRA"]) },
    },
    renamed = {
        &["PLAINTEXT", "EXTRA"], &[], vec![plaintext("PLAINTEXT", 9092), plaintext("OTHER", 9093)],
        ListenerChanges { start: vec![plaintext("OTHER", 9093)], stop: names(&["EXTRA"]) },
    },
    a_kept_name_stays_on_a_new_endpoint = {
        &["PLAINTEXT", "EXTRA"], &[], vec![plaintext("PLAINTEXT", 9092), plaintext("EXTRA", 9999)],
        ListenerChanges::default(),
    },
    a_failure_is_not_retried_on_the_same_endpoint = {
        &["PLAINTEXT"], &[plaintext("EXTRA", 9093)],
        vec![plaintext("PLAINTEXT", 9092), plaintext("EXTRA", 9093)],
        ListenerChanges::default(),
    },
    a_failure_is_retried_on_a_new_endpoint = {
        &["PLAINTEXT"], &[plaintext("EXTRA", 9093)],
        vec![plaintext("PLAINTEXT", 9092), plaintext("EXTRA", 9094)],
        ListenerChanges { start: vec![plaintext("EXTRA", 9094)], stop: vec![] },
    },
    every_listener_removed = {
        &["PLAINTEXT", "EXTRA"], &[], vec![],
        ListenerChanges { start: vec![], stop: names(&["PLAINTEXT", "EXTRA"]) },
    },
)]
fn listener_changes_diff_by_name(
    running: &[&str],
    failed: &[Endpoint],
    desired: Vec<Endpoint>,
    expected: ListenerChanges,
) {
    let failed = failed
        .iter()
        .map(|ep| (ep.name.clone(), ep.clone()))
        .collect();
    check!(listener_changes(&names(running), &failed, &desired) == expected);
}

/// The address a listener binds, or the kind of error it fails with.
#[yare::parameterized(
    wildcard = { None, 9092, Ok("0.0.0.0:9092") },
    ipv4 = { Some("127.0.0.1"), 9092, Ok("127.0.0.1:9092") },
    ipv6 = { Some("::1"), 9092, Ok("[::1]:9092") },
    port_too_large = { Some("127.0.0.1"), 65_536, Err(ErrorKind::InvalidInput) },
    negative_port = { None, -1, Err(ErrorKind::InvalidInput) },
)]
#[test_macro(tokio::test)]
async fn bind_address_reads_the_endpoint(
    host: Option<&str>,
    port: i32,
    expected: Result<&str, ErrorKind>,
) {
    let bound = bind_address(&endpoint("EXTRA", host, port, Plaintext))
        .await
        .map_err(|error| error.kind());
    check!(bound == expected.map(|addr| addr.parse::<SocketAddr>().expect("literal")));
}

/// The parts of a built [`ListenerSpec`] a test compares.
#[derive(Debug, PartialEq, Eq)]
struct Built {
    name: String,
    bind_addr: SocketAddr,
    advertised: String,
    protocol: ListenerProtocol,
    own_tls: bool,
    sasl_mechanisms: Option<Vec<SaslMechanism>>,
}

impl From<ListenerSpec> for Built {
    fn from(spec: ListenerSpec) -> Self {
        Self {
            name: spec.name,
            bind_addr: spec.bind_addr,
            advertised: spec.advertised,
            protocol: spec.protocol,
            own_tls: spec.tls_config.is_some(),
            sasl_mechanisms: spec.sasl_mechanisms,
        }
    }
}

/// An added listener's spec: its endpoint's name and protocol, the address it
/// binds, its endpoint as its advertised address, no TLS material of its own,
/// and the mechanisms of the config of the moment.
#[yare::parameterized(
    plaintext = {
        endpoint("EXTRA", Some("localhost"), 9093, Plaintext), "127.0.0.1:9093", vec![],
        Built {
            name: "EXTRA".to_owned(),
            bind_addr: "127.0.0.1:9093".parse().expect("literal"),
            advertised: "localhost:9093".to_owned(),
            protocol: Plaintext,
            own_tls: false,
            sasl_mechanisms: Some(vec![]),
        },
    },
    sasl = {
        endpoint("SASL", Some("127.0.0.1"), 9094, SaslPlaintext), "127.0.0.1:9094",
        vec![SaslMechanism::ScramSha256],
        Built {
            name: "SASL".to_owned(),
            bind_addr: "127.0.0.1:9094".parse().expect("literal"),
            advertised: "127.0.0.1:9094".to_owned(),
            protocol: SaslPlaintext,
            own_tls: false,
            sasl_mechanisms: Some(vec![SaslMechanism::ScramSha256]),
        },
    },
    wildcard = {
        endpoint("SSL", None, 9095, Ssl), "0.0.0.0:9095", vec![],
        Built {
            name: "SSL".to_owned(),
            bind_addr: "0.0.0.0:9095".parse().expect("literal"),
            advertised: "0.0.0.0:9095".to_owned(),
            protocol: Ssl,
            own_tls: false,
            sasl_mechanisms: Some(vec![]),
        },
    },
    ipv6 = {
        endpoint("V6", Some("::1"), 9096, Plaintext), "[::1]:9096", vec![],
        Built {
            name: "V6".to_owned(),
            bind_addr: "[::1]:9096".parse().expect("literal"),
            advertised: "[::1]:9096".to_owned(),
            protocol: Plaintext,
            own_tls: false,
            sasl_mechanisms: Some(vec![]),
        },
    },
)]
fn added_listener_spec(
    endpoint: Endpoint,
    bind_addr: &str,
    sasl_mechanisms: Vec<SaslMechanism>,
    expected: Built,
) {
    let spec = AddedListener::builder()
        .endpoint(&endpoint)
        .bind_addr(bind_addr.parse().expect("literal"))
        .sasl_mechanisms(sasl_mechanisms)
        .principal_mapper(SslPrincipalMapper::default())
        .build()
        .spec();
    check!(Built::from(spec) == expected);
}

/// A set running a listener of each of `names`, each on an idle task.
fn running(names: &[&str]) -> DataPlaneListeners {
    let listeners = DataPlaneListeners::new(CancellationToken::new());
    listeners.state().running = names
        .iter()
        .map(|name| Running {
            spec: ListenerSpec {
                name: (*name).to_owned(),
                bind_addr: "127.0.0.1:0".parse().expect("literal"),
                advertised: "127.0.0.1:0".to_owned(),
                protocol: Plaintext,
                tls_config: None,
                sasl_mechanisms: None,
                principal_mapper: SslPrincipalMapper::default(),
            },
            stop: listeners.shutdown.child_token(),
            task: tokio::spawn(async {}),
        })
        .collect();
    listeners
}

/// Kafka's `protectedListener`: the inter-broker listener, matched without
/// case, while more than one listener runs.
#[yare::parameterized(
    alone = { &["INTERNAL"], "INTERNAL", false },
    with_another = { &["INTERNAL", "EXTERNAL"], "INTERNAL", true },
    lower_case = { &["INTERNAL", "EXTERNAL"], "internal", true },
    not_inter_broker = { &["INTERNAL", "EXTERNAL"], "EXTERNAL", false },
)]
#[test_macro(tokio::test)]
async fn the_inter_broker_listener_is_protected_beside_another(
    names: &[&str],
    listener: &str,
    protected: bool,
) {
    check!(running(names).is_protected(listener, "INTERNAL") == protected);
}

/// Stopping by name cancels those listeners alone; closing hands back the
/// rest and opens nothing after.
#[tokio::test]
async fn stop_cancels_the_named_listeners_and_close_takes_the_rest() {
    let listeners = running(&["INTERNAL", "EXTRA", "EXTERNAL"]);
    let tokens: Vec<CancellationToken> = listeners
        .state()
        .running
        .iter()
        .map(|running| running.stop.clone())
        .collect();

    let stopped = listeners.stop(&names(&["EXTRA"]));

    assert!(stopped.len() == 1);
    assert!(
        tokens
            .iter()
            .map(CancellationToken::is_cancelled)
            .collect::<Vec<_>>()
            == vec![false, true, false]
    );
    assert!(listeners.names() == names(&["INTERNAL", "EXTERNAL"]));
    assert!(listeners.close().len() == 2);
    assert!(listeners.names().is_empty());
    assert!(listeners.state().closed);
}
