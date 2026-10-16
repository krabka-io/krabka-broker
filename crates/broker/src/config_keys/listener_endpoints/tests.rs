use {
    super::*,
    assert2::check,
    krabka_security::ListenerProtocol::{Plaintext, SaslPlaintext, SaslSsl, Ssl},
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

#[yare::parameterized(
    wildcard = {
        "PLAINTEXT://:9092",
        Ok(vec![endpoint("PLAINTEXT", Plaintext, None, 9092)])
    },
    lower_case_name = {
        "internal://127.0.0.1:9094",
        Ok(vec![endpoint("INTERNAL", SaslPlaintext, Some("127.0.0.1"), 9094)])
    },
    bracketed_ipv6 = {
        "SSL://[::1]:9093",
        Ok(vec![endpoint("SSL", Ssl, Some("::1"), 9093)])
    },
    two = {
        "PLAINTEXT://localhost:9092,SSL://localhost:9093",
        Ok(vec![
            endpoint("PLAINTEXT", Plaintext, Some("localhost"), 9092),
            endpoint("SSL", Ssl, Some("localhost"), 9093),
        ])
    },
    unparsable = {
        "PLAINTEXT:9092",
        Err("Error creating broker listeners from '[PLAINTEXT:9092]': Unable to parse \
             PLAINTEXT:9092 to a broker endpoint".to_owned())
    },
    port_overflow = {
        "PLAINTEXT://:99999999999",
        Err("Error creating broker listeners from '[PLAINTEXT://:99999999999]': For input \
             string: \"99999999999\"".to_owned())
    },
    unmapped = {
        "EXTERNAL://:9095",
        Err("Error creating broker listeners from '[EXTERNAL://:9095]': No security protocol \
             defined for listener EXTERNAL".to_owned())
    },
    repeated_name = {
        "PLAINTEXT://:9092,plaintext://:9093",
        Err("Error creating broker listeners from '[PLAINTEXT://:9092, plaintext://:9093]': \
             Each listener must have a different name, listeners: [PLAINTEXT://:9092, \
             plaintext://:9093]".to_owned())
    },
    repeated_port = {
        "PLAINTEXT://:9092,SSL://:9092",
        Err("Error creating broker listeners from '[PLAINTEXT://:9092, SSL://:9092]': Each \
             listener must have a different port, listeners: [PLAINTEXT://:9092, \
             SSL://:9092]".to_owned())
    },
    port_zero_may_repeat = {
        "PLAINTEXT://:0,SSL://:0",
        Ok(vec![
            endpoint("PLAINTEXT", Plaintext, None, 0),
            endpoint("SSL", Ssl, None, 0),
        ])
    },
    ipv4_and_ipv6_share_a_port = {
        "PLAINTEXT://127.0.0.1:9092,SSL://[::1]:9092",
        Ok(vec![
            endpoint("PLAINTEXT", Plaintext, Some("127.0.0.1"), 9092),
            endpoint("SSL", Ssl, Some("::1"), 9092),
        ])
    },
    two_ipv4_share_a_port = {
        "PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.2:9092",
        Err("Error creating broker listeners from '[PLAINTEXT://127.0.0.1:9092, \
             SSL://127.0.0.2:9092]': If you have two listeners on the same port then one needs \
             to be IPv4 and the other IPv6, listeners: [PLAINTEXT://127.0.0.1:9092, \
             SSL://127.0.0.2:9092], port: 9092".to_owned())
    },
    three_share_a_port = {
        "PLAINTEXT://127.0.0.1:9092,SSL://[::1]:9092,INTERNAL://127.0.0.2:9092",
        Err("Error creating broker listeners from '[PLAINTEXT://127.0.0.1:9092, \
             SSL://[::1]:9092, INTERNAL://127.0.0.2:9092]': Each listener must have a \
             different port unless exactly one listener has an IPv4 address and the other \
             IPv6 address, listeners: [PLAINTEXT://127.0.0.1:9092, SSL://[::1]:9092, \
             INTERNAL://127.0.0.2:9092], port: 9092".to_owned())
    },
)]
fn listeners_read_as_kafka_reads_them(value: &str, expected: Result<Vec<Endpoint>, String>) {
    check!(listener_endpoints(&config_list(value), &protocols(), true) == expected);
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

#[yare::parameterized(
    empty = { "", Ok(BTreeMap::new()) },
    lower_case = {
        "internal:sasl_ssl, PLAINTEXT:PLAINTEXT,",
        Ok(BTreeMap::from([
            ("INTERNAL".to_owned(), SaslSsl),
            ("PLAINTEXT".to_owned(), Plaintext),
        ]))
    },
    later_entry_wins = {
        "A:SSL,A:PLAINTEXT",
        Ok(BTreeMap::from([("A".to_owned(), Plaintext)]))
    },
    no_colon = {
        "PLAINTEXT",
        Err("Error parsing configuration property 'listener.security.protocol.map': begin 0, \
             end -1, length 9".to_owned())
    },
    unknown_protocol = {
        "A:TLS",
        Err("Invalid security protocol `TLS` defined in listener.security.protocol.map"
            .to_owned())
    },
    names_meet_once_normalised = {
        "a:SSL,A:SSL",
        Err("Duplicate key A (attempted merging values SSL and SSL)".to_owned())
    },
)]
fn the_protocol_map_reads_as_kafka_reads_it(
    value: &str,
    expected: Result<BTreeMap<String, ListenerProtocol>, String>,
) {
    check!(protocol_map(value) == expected);
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
        (CONTROLLER_LISTENER_NAMES.to_owned(), "CONTROLLER".to_owned()),
        (SASL_ENABLED_MECHANISMS.to_owned(), "PLAIN".to_owned()),
    ]);
    let default = BTreeMap::from([(SASL_ENABLED_MECHANISMS.to_owned(), "SCRAM-SHA-256".to_owned())]);
    let per_broker = per_broker
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect();
    ListenerView::new(&static_config, Some(&default), &per_broker)
}

#[test]
fn the_view_layers_static_default_and_per_broker_configs() {
    let view = view(&[
        (LISTENERS, "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,SSL://:9094"),
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
            == Ok(vec![endpoint("PLAINTEXT", Plaintext, Some("localhost"), 9092)])
    );
    check!(view.sasl_enabled_mechanisms("SSL") == vec![SaslMechanism::OAuthBearer]);
    check!(view.sasl_enabled_mechanisms("PLAINTEXT") == vec![SaslMechanism::ScramSha256]);
    check!(
        view.with_prefix("listener.name.ssl.")
            == BTreeMap::from([(
                SASL_ENABLED_MECHANISMS.to_owned(),
                "OAUTHBEARER".to_owned()
            )])
    );
}
