//! Listener fixtures with explicit security protocols and advertised addresses.

use {
    krabka_broker::{BrokerConfig, SslPrincipalMapper, config::ListenerSpec},
    krabka_security::ListenerProtocol,
    std::net::SocketAddr,
};

pub fn configure_sasl_plaintext(
    config: &mut BrokerConfig,
    addr: SocketAddr,
    controller_protocol: ListenerProtocol,
) {
    config.listeners = vec![listener(
        "SASL_PLAINTEXT",
        addr,
        ListenerProtocol::SaslPlaintext,
    )];
    config.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    config.controller_listener_protocol = controller_protocol;
}

pub fn listener(name: &str, addr: SocketAddr, protocol: ListenerProtocol) -> ListenerSpec {
    ListenerSpec {
        name: name.to_owned(),
        bind_addr: addr,
        advertised: addr.to_string(),
        protocol,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: SslPrincipalMapper::default(),
    }
}

pub fn loopback_listener(name: &str, protocol: ListenerProtocol) -> ListenerSpec {
    listener(name, "127.0.0.1:0".parse().unwrap(), protocol)
}
