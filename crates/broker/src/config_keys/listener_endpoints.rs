//! Kafka's `listeners` and `listener.security.protocol.map`, read as Kafka
//! 4.3.1's `AbstractKafkaConfig` reads them, over the config a node runs with:
//! its static config, under the cluster default, under its own dynamic
//! configs.
//!
//! Kafka keeps no separate listener type for a node. It reads the listeners
//! out of the merged config map each time it needs them, and each read can
//! fail. [`ListenerView`] is that map, and its readers return Kafka's message
//! when Kafka's reader would throw: `SocketServerConfigs.listenerListToEndPoints`
//! for a list entry, `AbstractKafkaConfig.validate` for duplicate names and
//! ports, and `effectiveListenerSecurityProtocolMap` for the protocol map.

use {
    crate::{
        config::BrokerConfig, controller_endpoint::CONTROLLER_LISTENER_NAME,
        handlers::describe_configs::static_settings,
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
    std::{
        collections::BTreeMap,
        net::{Ipv4Addr, Ipv6Addr},
        sync::LazyLock,
    },
};

/// `listeners`.
pub(crate) const LISTENERS: &str = "listeners";
/// `listener.security.protocol.map`.
pub(crate) const LISTENER_SECURITY_PROTOCOL_MAP: &str = "listener.security.protocol.map";
/// `advertised.listeners`.
pub(crate) const ADVERTISED_LISTENERS: &str = "advertised.listeners";
/// `controller.listener.names`.
pub(crate) const CONTROLLER_LISTENER_NAMES: &str = "controller.listener.names";
/// `inter.broker.listener.name`.
pub(crate) const INTER_BROKER_LISTENER_NAME: &str = "inter.broker.listener.name";
/// `sasl.enabled.mechanisms`.
pub(crate) const SASL_ENABLED_MECHANISMS: &str = "sasl.enabled.mechanisms";
/// `sasl.mechanism.inter.broker.protocol`.
pub(crate) const SASL_MECHANISM_INTER_BROKER_PROTOCOL: &str =
    "sasl.mechanism.inter.broker.protocol";
/// `process.roles`.
pub(crate) const PROCESS_ROLES: &str = "process.roles";

/// Kafka's `ListenerName.normalised`: a listener name is upper case.
pub(crate) fn normalised(name: &str) -> String { name.to_uppercase() }

/// Kafka's `ListenerName.configPrefix`: `listener.name.` and the name in
/// lower case.
pub(crate) fn config_prefix(listener: &str) -> String {
    format!("listener.name.{}.", listener.to_lowercase())
}

/// Kafka's `SecurityProtocol` name of a listener protocol.
pub(crate) const fn security_protocol_name(protocol: ListenerProtocol) -> &'static str {
    match protocol {
        ListenerProtocol::Plaintext => "PLAINTEXT",
        ListenerProtocol::Ssl => "SSL",
        ListenerProtocol::SaslPlaintext => "SASL_PLAINTEXT",
        ListenerProtocol::SaslSsl => "SASL_SSL",
    }
}

/// Kafka's `SecurityProtocol.forName`, which ignores case.
fn security_protocol(name: &str) -> Option<ListenerProtocol> {
    match name.to_uppercase().as_str() {
        "PLAINTEXT" => Some(ListenerProtocol::Plaintext),
        "SSL" => Some(ListenerProtocol::Ssl),
        "SASL_PLAINTEXT" => Some(ListenerProtocol::SaslPlaintext),
        "SASL_SSL" => Some(ListenerProtocol::SaslSsl),
        _ => None,
    }
}

/// One endpoint of a listener list: Kafka's `Endpoint`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Endpoint {
    /// The normalised listener name.
    pub(crate) name: String,
    pub(crate) protocol: ListenerProtocol,
    /// `None` for an empty host, which binds the wildcard address.
    pub(crate) host: Option<String>,
    pub(crate) port: i32,
}

/// Kafka's `ConfigDef` parse of a `LIST` value: empty when blank, otherwise
/// the trimmed value split at each comma and the blanks around it, keeping
/// empty entries.
pub(crate) fn config_list(value: &str) -> Vec<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    trimmed
        .split(',')
        .map(|entry| entry.trim().to_owned())
        .collect()
}

/// A Java `List.toString`: `[a, b]`.
fn java_list(entries: &[String]) -> String { format!("[{}]", entries.join(", ")) }

/// Kafka's `URI_PARSE_REGEXP`, anchored as `Matcher.matches` anchors it.
static URI_PARSE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"^(.*)://\[?([0-9a-zA-Z\-%._:]*)\]?:(-?[0-9]+)$")
        .unwrap_or_else(|error| unreachable!("the listener pattern compiles: {error}"))
});

/// Kafka's `Integer.parseInt` of the port, with Java's message.
fn parse_port(digits: &str) -> Result<i32, String> {
    digits
        .parse()
        .map_err(|_| format!("For input string: \"{digits}\""))
}

/// Kafka's `SocketServerConfigs.listenerListToEndPoints` and
/// `AbstractKafkaConfig.validate`, wrapped as
/// `AbstractKafkaConfig.listenerListToEndPoints` wraps their message.
///
/// # Errors
/// Kafka's message for an entry that does not parse, a listener without a
/// protocol, a repeated name, or, with `require_distinct_ports`, a repeated
/// port.
pub(crate) fn listener_endpoints(
    listeners: &[String],
    protocols: &BTreeMap<String, ListenerProtocol>,
    require_distinct_ports: bool,
) -> Result<Vec<Endpoint>, String> {
    let endpoints = parse_endpoints(listeners, protocols)
        .and_then(|endpoints| {
            check_endpoints(&endpoints, listeners, require_distinct_ports)?;
            Ok(endpoints)
        })
        .map_err(|cause| {
            format!(
                "Error creating broker listeners from '{}': {cause}",
                java_list(listeners)
            )
        })?;
    Ok(endpoints)
}

fn parse_endpoints(
    listeners: &[String],
    protocols: &BTreeMap<String, ListenerProtocol>,
) -> Result<Vec<Endpoint>, String> {
    listeners
        .iter()
        .map(|entry| {
            let groups = URI_PARSE
                .captures(entry)
                .ok_or_else(|| format!("Unable to parse {entry} to a broker endpoint"))?;
            let name = normalised(&groups[1]);
            let host = Some(groups[2].to_owned()).filter(|host| !host.is_empty());
            let port = parse_port(&groups[3])?;
            let protocol = *protocols
                .get(&name)
                .ok_or_else(|| format!("No security protocol defined for listener {name}"))?;
            Ok(Endpoint {
                name,
                protocol,
                host,
                port,
            })
        })
        .collect()
}

/// Which IP stack a host literal belongs to, as Apache Commons'
/// `InetAddressValidator` tells them apart.
fn ip_stack(host: Option<&str>) -> Option<bool> {
    let host = host?;
    let address = host.split_once('%').map_or(host, |(address, _)| address);
    if address.parse::<Ipv4Addr>().is_ok() {
        Some(false)
    } else if address.parse::<Ipv6Addr>().is_ok() {
        Some(true)
    } else {
        None
    }
}

/// Kafka's `AbstractKafkaConfig.validate`.
fn check_endpoints(
    endpoints: &[Endpoint],
    listeners: &[String],
    require_distinct_ports: bool,
) -> Result<(), String> {
    let mut names: Vec<&str> = endpoints.iter().map(|ep| ep.name.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    if names.len() != endpoints.len() {
        return Err(format!(
            "Each listener must have a different name, listeners: {}",
            java_list(listeners)
        ));
    }
    if !require_distinct_ports {
        return Ok(());
    }
    let mut by_port: BTreeMap<i32, Vec<&Endpoint>> = BTreeMap::new();
    for endpoint in endpoints.iter().filter(|ep| ep.port != 0) {
        by_port.entry(endpoint.port).or_default().push(endpoint);
    }
    for (port, shared) in by_port.into_iter().filter(|(_, eps)| eps.len() > 1) {
        let (with_ip, without_ip): (Vec<&Endpoint>, Vec<&Endpoint>) = shared
            .into_iter()
            .partition(|ep| ip_stack(ep.host.as_deref()).is_some());
        if without_ip.len() > 1 {
            return Err(format!(
                "Each listener must have a different port, listeners: {}",
                java_list(listeners)
            ));
        }
        match with_ip.as_slice() {
            [] => {}
            [first, second] => {
                let stacks = (
                    ip_stack(first.host.as_deref()),
                    ip_stack(second.host.as_deref()),
                );
                if stacks.0 == stacks.1 || !without_ip.is_empty() {
                    return Err(format!(
                        "If you have two listeners on the same port then one needs to be IPv4 \
                         and the other IPv6, listeners: {}, port: {port}",
                        java_list(listeners)
                    ));
                }
            }
            _ => {
                return Err(format!(
                    "Each listener must have a different port unless exactly one listener has \
                     an IPv4 address and the other IPv6 address, listeners: {}, port: {port}",
                    java_list(listeners)
                ));
            }
        }
    }
    Ok(())
}

/// Kafka's `Csv.parseCsvMap` of `listener.security.protocol.map`, then
/// `effectiveListenerSecurityProtocolMap`: each name normalised and each
/// protocol read by `SecurityProtocol.forName`.
///
/// krabka always states the map of its static listeners, so Kafka's default
/// map, and the `PLAINTEXT` controller mapping it adds to that default, never
/// apply.
///
/// # Errors
/// Kafka's message for an entry without a colon, a protocol that is not one,
/// or two entries that name the same listener.
pub(crate) fn protocol_map(value: &str) -> Result<BTreeMap<String, ListenerProtocol>, String> {
    let mut map = BTreeMap::new();
    if value.is_empty() {
        return Ok(map);
    }
    let mut entries: Vec<&str> = value.split(',').map(str::trim).collect();
    // Java's `String.split` drops the trailing empty strings.
    while entries.last().is_some_and(|entry| entry.is_empty()) {
        entries.pop();
    }
    let mut raw: Vec<(String, String)> = Vec::new();
    for entry in entries {
        let colon = entry.rfind(':').ok_or_else(|| {
            format!(
                "Error parsing configuration property '{LISTENER_SECURITY_PROTOCOL_MAP}': begin \
                 0, end -1, length {}",
                entry.encode_utf16().count()
            )
        })?;
        let name = entry[..colon].trim().to_owned();
        let protocol = entry[colon + 1..].trim().to_owned();
        // `parseCsvMap` fills a map, so a later entry for a name replaces an
        // earlier one before the names are normalised.
        if let Some(existing) = raw.iter_mut().find(|(seen, _)| *seen == name) {
            existing.1 = protocol;
        } else {
            raw.push((name, protocol));
        }
    }
    for (name, protocol_name) in raw {
        let protocol = security_protocol(&protocol_name).ok_or_else(|| {
            format!(
                "Invalid security protocol `{protocol_name}` defined in \
                 {LISTENER_SECURITY_PROTOCOL_MAP}"
            )
        })?;
        let name = normalised(&name);
        if let Some(previous) = map.insert(name.clone(), protocol) {
            return Err(format!(
                "Duplicate key {name} (attempted merging values {} and {})",
                security_protocol_name(previous),
                security_protocol_name(protocol)
            ));
        }
    }
    Ok(map)
}

/// The config map a node runs with, keyed as Kafka keys it: its static
/// config, then the cluster default, then its own dynamic configs, each layer
/// replacing the one before, as `DynamicBrokerConfig.validatedKafkaProps`
/// builds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListenerView {
    originals: BTreeMap<String, String>,
}

impl ListenerView {
    /// The merged map of `static_config` under `cluster_default` under
    /// `per_broker`.
    pub(crate) fn new(
        static_config: &BTreeMap<String, String>,
        cluster_default: Option<&BTreeMap<String, String>>,
        per_broker: &BTreeMap<String, String>,
    ) -> Self {
        let mut originals = static_config.clone();
        for layer in cluster_default.into_iter().chain([per_broker]) {
            originals.extend(
                layer
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone())),
            );
        }
        Self { originals }
    }

    /// The value of `name`, if the node has one.
    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        self.originals.get(name).map(String::as_str)
    }

    /// Kafka's `originalsWithPrefix(prefix, true)`.
    pub(crate) fn with_prefix(&self, prefix: &str) -> BTreeMap<String, String> {
        self.originals
            .iter()
            .filter_map(|(name, value)| {
                name.strip_prefix(prefix)
                    .map(|key| (key.to_owned(), value.clone()))
            })
            .collect()
    }

    /// Kafka's `valuesWithPrefixOverride(listener.configPrefix).get(name)`:
    /// the listener's own value of `name`, else the broker-wide one.
    pub(crate) fn listener_value(&self, listener: &str, name: &str) -> Option<&str> {
        self.get(&format!("{}{name}", config_prefix(listener)))
            .or_else(|| self.get(name))
    }

    /// Kafka's `effectiveListenerSecurityProtocolMap`.
    ///
    /// # Errors
    /// See [`protocol_map`].
    pub(crate) fn protocol_map(&self) -> Result<BTreeMap<String, ListenerProtocol>, String> {
        protocol_map(self.get(LISTENER_SECURITY_PROTOCOL_MAP).unwrap_or_default())
    }

    /// The `listeners` entries, as Kafka's `getList` reads them.
    pub(crate) fn listener_list(&self) -> Vec<String> {
        config_list(self.get(LISTENERS).unwrap_or_default())
    }

    /// Kafka's `KafkaConfig.listeners`.
    ///
    /// # Errors
    /// See [`protocol_map`] and [`listener_endpoints`].
    pub(crate) fn listeners(&self) -> Result<Vec<Endpoint>, String> {
        listener_endpoints(&self.listener_list(), &self.protocol_map()?, true)
    }

    /// Kafka's `controllerListenerNames`.
    pub(crate) fn controller_listener_names(&self) -> Vec<String> {
        config_list(self.get(CONTROLLER_LISTENER_NAMES).unwrap_or_default())
    }

    /// Kafka's `effectiveAdvertisedBrokerListeners`: `advertised.listeners`
    /// when the node names it, else `listeners`, less the controller
    /// listeners.
    ///
    /// # Errors
    /// See [`protocol_map`] and [`listener_endpoints`].
    pub(crate) fn advertised_broker_listeners(&self) -> Result<Vec<Endpoint>, String> {
        let advertised = match self.get(ADVERTISED_LISTENERS) {
            Some(value) => listener_endpoints(&config_list(value), &self.protocol_map()?, false)?,
            None => self.listeners()?,
        };
        let controllers = self.controller_listener_names();
        Ok(advertised
            .into_iter()
            .filter(|ep| !controllers.contains(&ep.name))
            .collect())
    }

    /// Kafka's `dataPlaneListeners`: the listeners that are not controller
    /// listeners. These are the listeners a node with the broker role opens.
    ///
    /// # Errors
    /// See [`ListenerView::listeners`].
    pub(crate) fn data_plane_listeners(&self) -> Result<Vec<Endpoint>, String> {
        let controllers = self.controller_listener_names();
        Ok(self
            .listeners()?
            .into_iter()
            .filter(|ep| !controllers.contains(&ep.name))
            .collect())
    }

    /// The SASL mechanism names `listener` enables, as Kafka's
    /// `saslEnabledMechanisms(listenerName)` reads them.
    pub(crate) fn sasl_enabled_mechanism_names(&self, listener: &str) -> Vec<String> {
        config_list(
            self.listener_value(listener, SASL_ENABLED_MECHANISMS)
                .unwrap_or_default(),
        )
    }

    /// The SASL mechanisms `listener` enables. A name krabka does not know is
    /// left out.
    pub(crate) fn sasl_enabled_mechanisms(&self, listener: &str) -> Vec<SaslMechanism> {
        self.sasl_enabled_mechanism_names(listener)
            .iter()
            .filter_map(|name| SaslMechanism::from_wire(name))
            .collect()
    }
}

/// The static config of `config` as Kafka would hold it in
/// `server.properties`: what `DescribeConfigs` reports at
/// `STATIC_BROKER_CONFIG`, and the SASL settings krabka's own config sets,
/// spelt as the Kafka keys they stand for.
pub(crate) fn static_originals(config: &BrokerConfig) -> BTreeMap<String, String> {
    let mut originals: BTreeMap<String, String> = static_settings(config)
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
    originals
        .entry(CONTROLLER_LISTENER_NAMES.to_owned())
        .or_insert_with(|| CONTROLLER_LISTENER_NAME.to_owned());
    // A krabka broker always runs with a named inter-broker listener, where
    // Kafka derives `PLAINTEXT` from `security.inter.broker.protocol` when the
    // key is unset; the named one is the listener either way.
    if config.is_broker() {
        originals
            .entry(INTER_BROKER_LISTENER_NAME.to_owned())
            .or_insert_with(|| config.inter_broker_listener_name.clone());
    }
    let wire = |mechanisms: &[SaslMechanism]| {
        mechanisms
            .iter()
            .map(|mechanism| mechanism.wire_name())
            .collect::<Vec<_>>()
            .join(",")
    };
    originals.insert(
        SASL_ENABLED_MECHANISMS.to_owned(),
        wire(&config.enabled_sasl_mechanisms),
    );
    for listener in config.effective_listeners() {
        if let Some(mechanisms) = &listener.sasl_mechanisms {
            originals.insert(
                format!("{}{SASL_ENABLED_MECHANISMS}", config_prefix(&listener.name)),
                wire(mechanisms),
            );
        }
    }
    if let Some(credentials) = &config.inter_broker_credentials {
        originals.insert(
            SASL_MECHANISM_INTER_BROKER_PROTOCOL.to_owned(),
            credentials.mechanism().wire_name().to_owned(),
        );
    }
    originals
}

#[cfg(test)]
mod tests;
