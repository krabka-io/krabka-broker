use {
    super::*,
    assert2::check,
    krabka_security::ListenerProtocol::{Plaintext, SaslPlaintext, SaslSsl, Ssl},
    rstest_bdd::datatable::Rows,
};

fn protocols() -> BTreeMap<String, ListenerProtocol> {
    BTreeMap::from([
        ("PLAINTEXT".to_owned(), Plaintext),
        ("SSL".to_owned(), Ssl),
        ("INTERNAL".to_owned(), SaslPlaintext),
    ])
}

fn endpoint(name: &str, protocol: ListenerProtocol, host: Option<&str>, port: i32) -> Endpoint {
    Endpoint {
        name: name.to_owned(),
        protocol,
        host: host.map(str::to_owned),
        port,
    }
}

#[yare::parameterized(
    blank = { "  ", &[] },
    one = { "PLAINTEXT://:9092", &["PLAINTEXT://:9092"] },
    spaced = { " A://:1 ,  B://:2 ", &["A://:1", "B://:2"] },
    empty_entry_kept = { "A://:1,,B://:2", &["A://:1", "", "B://:2"] },
)]
fn a_list_value_splits_as_config_def_splits_it(value: &str, expected: &[&str]) {
    check!(config_list(value) == expected);
}

#[test]
fn advertised_listeners_may_share_a_port() {
    let listeners = config_list("PLAINTEXT://a:9092,SSL://b:9092");
    check!(
        listener_endpoints(&listeners, &protocols(), false)
            == Ok(vec![
                endpoint("PLAINTEXT", Plaintext, Some("a"), 9092),
                endpoint("SSL", Ssl, Some("b"), 9092),
            ])
    );
}

fn view(per_broker: &[(&str, &str)]) -> ListenerView {
    let static_config = BTreeMap::from([
        (
            LISTENERS.to_owned(),
            "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093".to_owned(),
        ),
        (
            LISTENER_SECURITY_PROTOCOL_MAP.to_owned(),
            "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT".to_owned(),
        ),
        (
            ADVERTISED_LISTENERS.to_owned(),
            "PLAINTEXT://localhost:9092".to_owned(),
        ),
        (
            CONTROLLER_LISTENER_NAMES.to_owned(),
            "CONTROLLER".to_owned(),
        ),
        (SASL_ENABLED_MECHANISMS.to_owned(), "PLAIN".to_owned()),
    ]);
    let default = BTreeMap::from([(
        SASL_ENABLED_MECHANISMS.to_owned(),
        "SCRAM-SHA-256".to_owned(),
    )]);
    let per_broker = per_broker
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect();
    ListenerView::new(&static_config, Some(&default), &per_broker)
}

#[test]
fn the_view_layers_static_default_and_per_broker_configs() {
    let view = view(&[
        (
            LISTENERS,
            "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,SSL://:9094",
        ),
        (
            LISTENER_SECURITY_PROTOCOL_MAP,
            "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,SSL:SSL",
        ),
        ("listener.name.ssl.sasl.enabled.mechanisms", "OAUTHBEARER"),
    ]);
    check!(
        view.data_plane_listeners()
            == Ok(vec![
                endpoint("PLAINTEXT", Plaintext, Some("0.0.0.0"), 9092),
                endpoint("SSL", Ssl, None, 9094),
            ])
    );
    check!(
        view.advertised_broker_listeners()
            == Ok(vec![endpoint(
                "PLAINTEXT",
                Plaintext,
                Some("localhost"),
                9092
            )])
    );
    check!(view.sasl_enabled_mechanisms("SSL") == vec![SaslMechanism::OAuthBearer]);
    check!(view.sasl_enabled_mechanisms("PLAINTEXT") == vec![SaslMechanism::ScramSha256]);
    check!(
        view.with_prefix("listener.name.ssl.")
            == BTreeMap::from([(SASL_ENABLED_MECHANISMS.to_owned(), "OAUTHBEARER".to_owned())])
    );
}

#[derive(Default)]
struct ListenerConfiguration {
    input: String,
    endpoints: Option<Result<Vec<Endpoint>, String>>,
    mapping: Option<Result<BTreeMap<String, ListenerProtocol>, String>>,
}

#[derive(rstest_bdd_macros::DataTableRow)]
struct ExpectedListenerEndpoint {
    name: String,
    protocol: String,
    host: String,
    port: i32,
}

#[derive(rstest_bdd_macros::DataTableRow)]
struct ExpectedListenerProtocol {
    name: String,
    protocol: String,
}

#[rstest::fixture]
fn listener_configuration() -> ListenerConfiguration {
    // Parsing every independent configuration starts without a prior result.
    ListenerConfiguration::default()
}

fn expected_listener_protocol(protocol: &str) -> ListenerProtocol {
    match protocol {
        "Plaintext" => Plaintext,
        "Ssl" => Ssl,
        "SaslPlaintext" => SaslPlaintext,
        "SaslSsl" => SaslSsl,
        _ => panic!("unsupported expected protocol {protocol}"),
    }
}

#[rstest_bdd_macros::given("listener addresses are JSON {value}")]
#[rstest_bdd_macros::given("the protocol mapping is JSON {value}")]
fn listener_configuration_input(listener_configuration: &mut ListenerConfiguration, value: String) {
    listener_configuration.input =
        serde_json::from_str(&value).expect("JSON preserves configuration whitespace");
}

#[rstest_bdd_macros::when(
    "broker listener addresses are resolved against the configured protocols"
)]
fn resolve_configured_listeners(listener_configuration: &mut ListenerConfiguration) {
    listener_configuration.endpoints = Some(listener_endpoints(
        &config_list(&listener_configuration.input),
        &protocols(),
        true,
    ));
}

#[rstest_bdd_macros::when("the listener protocol mapping is parsed")]
fn parse_listener_protocols(listener_configuration: &mut ListenerConfiguration) {
    listener_configuration.mapping = Some(protocol_map(&listener_configuration.input));
}

#[rstest_bdd_macros::then("listener resolution fails with {message}")]
fn refused_listener_addresses(listener_configuration: &ListenerConfiguration, message: String) {
    check!(listener_configuration.endpoints == Some(Err(message)));
}

#[rstest_bdd_macros::then("protocol mapping fails with {message}")]
fn refused_listener_protocols(listener_configuration: &ListenerConfiguration, message: String) {
    check!(listener_configuration.mapping == Some(Err(message)));
}

#[rstest_bdd_macros::then("the resolved listener endpoints are:")]
fn resolved_listener_addresses(
    listener_configuration: &ListenerConfiguration,
    #[datatable] rows: Rows<ExpectedListenerEndpoint>,
) {
    let expected = rows
        .into_iter()
        .map(|row| {
            endpoint(
                &row.name,
                expected_listener_protocol(&row.protocol),
                if row.host == "wildcard" {
                    None
                } else {
                    Some(row.host.as_str())
                },
                row.port,
            )
        })
        .collect();
    check!(listener_configuration.endpoints == Some(Ok(expected)));
}

#[rstest_bdd_macros::then("the normalized listener protocol mappings are:")]
fn resolved_listener_protocols(
    listener_configuration: &ListenerConfiguration,
    #[datatable] rows: Rows<ExpectedListenerProtocol>,
) {
    let expected = rows
        .into_iter()
        .map(|row| (row.name, expected_listener_protocol(&row.protocol)))
        .collect();
    check!(listener_configuration.mapping == Some(Ok(expected)));
}

#[rstest_bdd_macros::then("the listener protocol mapping is empty")]
fn empty_listener_protocols(listener_configuration: &ListenerConfiguration) {
    check!(listener_configuration.mapping == Some(Ok(BTreeMap::new())));
}

rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/config_keys/listener_endpoints/tests",
    fixtures = [listener_configuration: ListenerConfiguration]
);
