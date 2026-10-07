//! Validation shared by broker and controller registration listeners.

/// Retain the caller's errors while applying Kafka's ordered listener checks.
/// Both wire listener types carry the same name, host, port and protocol fields.
macro_rules! decode {
    ($listeners:expr, $empty:expr, $invalid:expr, $protocol:expr) => {{
        let listeners = $listeners;
        if listeners.is_empty() {
            return Err($empty);
        }
        let mut names = std::collections::HashSet::with_capacity(listeners.len());
        listeners
            .iter()
            .map(|listener| {
                if listener.name.is_empty()
                    || listener.host.is_empty()
                    || listener.port == 0
                    || !names.insert(listener.name.clone())
                {
                    return Err($invalid);
                }
                let protocol = match listener.security_protocol {
                    0 => krabka_security::ListenerProtocol::Plaintext,
                    1 => krabka_security::ListenerProtocol::Ssl,
                    2 => krabka_security::ListenerProtocol::SaslPlaintext,
                    3 => krabka_security::ListenerProtocol::SaslSsl,
                    _ => return Err($protocol),
                };
                Ok(krabka_metadata::BrokerEndpoint {
                    name: listener.name.clone(),
                    host: listener.host.clone(),
                    port: listener.port,
                    protocol,
                })
            })
            .collect()
    }};
}

pub(super) use decode;
