//! Kafka 4.3.1's checks of a node's new listener config, run on the node an
//! alter of its own `BROKER` resource is addressed to.
//!
//! `DynamicBrokerConfig.processReconfiguration` first builds a whole new
//! `KafkaConfig`, whose `validateValues` reads the listeners, the protocol
//! map, the advertised listeners and the inter-broker SASL mechanism. A
//! failure there reaches the client as `INVALID_REQUEST` with the reader's
//! own message. It then runs `DynamicListenerConfig.validateReconfiguration`
//! when a key that object listens for changed, and a failure there is wrapped
//! as `Invalid dynamic configuration`.

use {
    super::{
        broker_dynamic::DYNAMIC_SECURITY_CONFIGS,
        listener_endpoints::{
            CONTROLLER_LISTENER_NAMES, Endpoint, INTER_BROKER_LISTENER_NAME,
            LISTENER_SECURITY_PROTOCOL_MAP, LISTENERS, ListenerView, PROCESS_ROLES,
            SASL_ENABLED_MECHANISMS, SASL_MECHANISM_INTER_BROKER_PROTOCOL, config_list,
            config_prefix, normalised,
        },
    },
    crate::{codes, listener_tls::ListenerTlsInputs},
    krabka_metadata::MetadataImage,
    krabka_security::ListenerProtocol,
    std::collections::BTreeMap,
};

/// Kafka's `DynamicListenerConfig.RECONFIGURABLE_CONFIGS`: a change to one of
/// these runs `DynamicListenerConfig.validateReconfiguration`.
const DYNAMIC_LISTENER_CONFIGS: &[&str] = &[
    LISTENERS,
    LISTENER_SECURITY_PROTOCOL_MAP,
    "principal.builder.class",
    "ssl.protocol",
    "ssl.provider",
    "ssl.cipher.suites",
    "ssl.enabled.protocols",
    "ssl.keystore.type",
    "ssl.keystore.location",
    "ssl.keystore.password",
    "ssl.key.password",
    "ssl.truststore.type",
    "ssl.truststore.location",
    "ssl.truststore.password",
    "ssl.keymanager.algorithm",
    "ssl.trustmanager.algorithm",
    "ssl.endpoint.identification.algorithm",
    "ssl.secure.random.implementation",
    "ssl.client.auth",
    "ssl.engine.factory.class",
    SASL_MECHANISM_INTER_BROKER_PROTOCOL,
    "sasl.jaas.config",
    SASL_ENABLED_MECHANISMS,
    "sasl.kerberos.service.name",
    "sasl.kerberos.kinit.cmd",
    "sasl.kerberos.ticket.renew.window.factor",
    "sasl.kerberos.ticket.renew.jitter",
    "sasl.kerberos.min.time.before.relogin",
    "sasl.kerberos.principal.to.local.rules",
    "sasl.login.refresh.window.factor",
    "sasl.login.refresh.window.jitter",
    "sasl.login.refresh.min.period.seconds",
    "sasl.login.refresh.buffer.seconds",
    "max.connections",
    "max.connection.creation.rate",
    "num.network.threads",
];

/// The listener keys an existing listener may change: Kafka's
/// `DYNAMIC_SECURITY_CONFIGS`, `SocketServer.LISTENER_RECONFIGURABLE_CONFIGS`
/// and `DataPlaneAcceptor.ListenerReconfigurableConfigs`.
fn reconfigurable_listener_key(key: &str) -> bool {
    DYNAMIC_SECURITY_CONFIGS.contains(&key)
        || matches!(
            key,
            "max.connections" | "max.connection.creation.rate" | "num.network.threads"
        )
}

/// Kafka's `SecurityProtocol` of the inter-broker listener when nothing names
/// a mechanism: `sasl.mechanism.inter.broker.protocol` defaults to GSSAPI.
const DEFAULT_SASL_MECHANISM: &str = "GSSAPI";

/// A Scala `require` failure, as `IllegalArgumentException.getMessage` gives
/// it.
fn requirement(message: &str) -> String { format!("requirement failed: {message}") }

/// Kafka's `ListenerName.toString`.
fn listener_name(name: &str) -> String { format!("ListenerName({name})") }

/// A `DynamicListenerConfig` refusal, as `processReconfiguration` wraps it.
fn invalid_dynamic_configuration(cause: &str) -> String {
    format!(
        "Invalid value org.apache.kafka.common.config.ConfigException: {cause} for configuration \
         Invalid dynamic configuration"
    )
}

/// The names of `endpoints`, once each, in their order: a Scala `Set` built
/// from a `Seq` keeps that order up to four entries.
fn names(endpoints: &[Endpoint]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for endpoint in endpoints {
        if !names.contains(&endpoint.name) {
            names.push(endpoint.name.clone());
        }
    }
    names
}

/// The node's roles, from `process.roles`.
struct Roles {
    broker: bool,
    controller: bool,
}

impl Roles {
    fn of(view: &ListenerView) -> Self {
        let roles = config_list(view.get(PROCESS_ROLES).unwrap_or_default());
        Self {
            broker: roles.iter().any(|role| role == "broker"),
            controller: roles.iter().any(|role| role == "controller"),
        }
    }
}

/// Whether a key `validateValues` reads about listeners differs between the
/// two views. Only such a change can make a node's listener config fail, so
/// the checks run only then: the static config they start from passed them
/// when the node started.
fn listener_values_changed(current: &ListenerView, proposed: &ListenerView) -> bool {
    let read = |view: &ListenerView| -> BTreeMap<String, String> {
        [
            LISTENERS,
            LISTENER_SECURITY_PROTOCOL_MAP,
            SASL_ENABLED_MECHANISMS,
            SASL_MECHANISM_INTER_BROKER_PROTOCOL,
        ]
        .iter()
        .filter_map(|key| {
            view.get(key)
                .map(|value| ((*key).to_owned(), value.to_owned()))
        })
        .chain(
            view.with_prefix("listener.name.")
                .into_iter()
                .filter(|(key, _)| key.ends_with(&format!(".{SASL_ENABLED_MECHANISMS}"))),
        )
        .collect()
    };
    read(current) != read(proposed)
}

/// Kafka's `effectiveAdvertisedControllerListeners`: each controller
/// listener's advertised endpoint, else its `listeners` endpoint. krabka's
/// `advertised.listeners` names no controller listener, so each comes from
/// `listeners`.
fn advertised_controller_listeners(view: &ListenerView) -> Result<Vec<Endpoint>, String> {
    let listeners = view.listeners()?;
    Ok(view
        .controller_listener_names()
        .iter()
        .filter_map(|name| {
            let name = normalised(name);
            listeners
                .iter()
                .find(|endpoint| endpoint.name == name)
                .cloned()
        })
        .collect())
}

/// The controller-role checks of Kafka's `validateValues`.
fn check_controller_listeners(view: &ListenerView, roles: &Roles) -> Result<(), String> {
    let controller_names = view.controller_listener_names();
    let listeners = view.listeners()?;
    let controller_advertised = advertised_controller_listeners(view)?;
    if !roles.broker && controller_advertised.len() != listeners.len() {
        return Err(requirement(&format!(
            "The {LISTENERS} config must only contain KRaft controller listeners from \
             {CONTROLLER_LISTENER_NAMES} when {PROCESS_ROLES}=controller"
        )));
    }
    if controller_advertised.is_empty() {
        return Err(requirement(&format!(
            "{CONTROLLER_LISTENER_NAMES} must contain at least one value appearing in the \
             '{LISTENERS}' configuration when running the KRaft controller role"
        )));
    }
    let listener_names: Vec<&str> = listeners.iter().map(|ep| ep.name.as_str()).collect();
    if !controller_names
        .iter()
        .all(|name| listener_names.contains(&name.as_str()))
    {
        return Err(requirement(&format!(
            "{CONTROLLER_LISTENER_NAMES} must only contain values appearing in the \
             '{LISTENERS}' configuration when running the KRaft controller role"
        )));
    }
    Ok(())
}

/// The broker-only checks of Kafka's `validateValues`.
fn check_broker_only_listeners(
    view: &ListenerView,
    protocols: &BTreeMap<String, ListenerProtocol>,
) -> Result<(), String> {
    let controller_names = view.controller_listener_names();
    if view
        .listeners()?
        .iter()
        .any(|ep| controller_names.contains(&ep.name))
    {
        return Err(requirement(&format!(
            "{CONTROLLER_LISTENER_NAMES} must not contain a value appearing in the \
             '{LISTENERS}' configuration when running KRaft with just the broker role"
        )));
    }
    if let Some(missing) = controller_names
        .iter()
        .map(|name| normalised(name))
        .find(|name| !protocols.contains_key(name))
    {
        return Err(format!(
            "Controller listener with name {missing} defined in {CONTROLLER_LISTENER_NAMES} not \
             found in {LISTENER_SECURITY_PROTOCOL_MAP}  (an explicit security mapping for each \
             controller listener is required if {LISTENER_SECURITY_PROTOCOL_MAP} is non-empty, \
             or if there are security protocols other than PLAINTEXT in use)"
        ));
    }
    Ok(())
}

/// The broker-role checks of Kafka's `validateValues`: an advertised
/// listener, the inter-broker listener among them, all of them opened, and
/// the inter-broker SASL mechanism enabled.
fn check_broker_listeners(
    view: &ListenerView,
    advertised: &[Endpoint],
    protocols: &BTreeMap<String, ListenerProtocol>,
) -> Result<(), String> {
    let listener_names = names(&view.listeners()?);
    let advertised_names = names(advertised);
    if advertised_names.is_empty() {
        return Err(requirement(&format!(
            "There must be at least one broker advertised listener. Perhaps all listeners \
             appear in {CONTROLLER_LISTENER_NAMES}?"
        )));
    }
    let inter_broker = normalised(view.get(INTER_BROKER_LISTENER_NAME).unwrap_or("PLAINTEXT"));
    let inter_broker_protocol = *protocols.get(&inter_broker).ok_or_else(|| {
        format!(
            "Listener with name {inter_broker} defined in {INTER_BROKER_LISTENER_NAME} not found \
             in {LISTENER_SECURITY_PROTOCOL_MAP}."
        )
    })?;
    if !advertised_names.contains(&inter_broker) {
        return Err(requirement(&format!(
            "{INTER_BROKER_LISTENER_NAME} must be a listener name defined in \
             advertised.listeners. The valid options based on currently configured listeners \
             are {}",
            advertised_names.join(",")
        )));
    }
    if !advertised_names
        .iter()
        .all(|name| listener_names.contains(name))
    {
        return Err(requirement(&format!(
            "advertised.listeners listener names must be equal to or a subset of the ones \
             defined in {LISTENERS}. Found {}. The valid options based on the current \
             configuration are {}",
            advertised_names.join(","),
            listener_names.join(",")
        )));
    }
    if inter_broker_protocol.requires_sasl() {
        let mechanism = view
            .get(SASL_MECHANISM_INTER_BROKER_PROTOCOL)
            .unwrap_or(DEFAULT_SASL_MECHANISM);
        if !view
            .sasl_enabled_mechanism_names(&inter_broker)
            .iter()
            .any(|enabled| enabled == mechanism)
        {
            return Err(requirement(&format!(
                "{SASL_MECHANISM_INTER_BROKER_PROTOCOL} must be included in \
                 {SASL_ENABLED_MECHANISMS} when SASL is used for inter-broker communication"
            )));
        }
    }
    Ok(())
}

/// The listener checks of Kafka's `KafkaConfig.validateValues`, in its
/// order, over the config the node would run with.
fn validate_values(view: &ListenerView) -> Result<(), String> {
    let roles = Roles::of(view);
    let advertised = view.advertised_broker_listeners()?;
    let protocols = view.protocol_map()?;
    if roles.broker && !roles.controller {
        check_broker_only_listeners(view, &protocols)?;
    } else if roles.controller {
        check_controller_listeners(view, &roles)?;
    }
    view.listeners()?;
    if roles.broker {
        check_broker_listeners(view, &advertised, &protocols)?;
    }
    Ok(())
}

/// Whether a key `DynamicListenerConfig` listens for differs between the two
/// views, as `needsReconfiguration` compares the changed and deleted keys.
fn listener_reconfiguration_needed(current: &ListenerView, proposed: &ListenerView) -> bool {
    DYNAMIC_LISTENER_CONFIGS
        .iter()
        .any(|key| current.get(key) != proposed.get(key))
}

/// Kafka's `DynamicListenerConfig.validateReconfiguration`, for the listeners
/// the node keeps: their own keys, other than the ones an existing listener
/// reconfigures, and their protocol, must stay as they are.
///
/// Its first two checks cannot fail here: krabka always states
/// `advertised.listeners`, which no alter changes, so `validate_values`
/// already refused a proposal that drops one, and it refused a listener
/// without a protocol when it read `listeners`.
fn validate_reconfiguration(current: &ListenerView, proposed: &ListenerView) -> Result<(), String> {
    let old = current.listeners()?;
    let new = proposed.listeners()?;
    for endpoint in new
        .iter()
        .filter(|endpoint| old.iter().any(|ep| ep.name == endpoint.name))
    {
        let immutable = |view: &ListenerView| -> BTreeMap<String, String> {
            view.with_prefix(&config_prefix(&endpoint.name))
                .into_iter()
                .filter(|(key, _)| !reconfigurable_listener_key(key))
                .collect()
        };
        if immutable(proposed) != immutable(current) {
            return Err(format!(
                "Configs cannot be updated dynamically for existing listener {}, restart broker \
                 or create a new listener for update",
                listener_name(&endpoint.name)
            ));
        }
        let protocol = |view: &ListenerView| {
            view.protocol_map()
                .ok()
                .and_then(|map| map.get(&endpoint.name).copied())
        };
        if protocol(current) != protocol(proposed) {
            return Err(format!(
                "Security protocol cannot be updated for existing listener {}",
                listener_name(&endpoint.name)
            ));
        }
    }
    Ok(())
}

/// A node's listener config before and after an alter of its own resource.
struct ListenerChange {
    current: ListenerView,
    proposed: ListenerView,
}

impl ListenerChange {
    /// The node's config with its `current` and its `proposed` per-broker
    /// dynamic configs, over its `static_config` and the cluster default
    /// `image` holds.
    fn new(
        static_config: &BTreeMap<String, String>,
        image: &MetadataImage,
        current: &BTreeMap<String, String>,
        proposed: &BTreeMap<String, String>,
    ) -> Self {
        let default = image.default_broker_config();
        Self {
            current: ListenerView::new(static_config, default, current),
            proposed: ListenerView::new(static_config, default, proposed),
        }
    }

    /// The new config's listener checks that Kafka runs before any
    /// reconfigurable object sees it: `validateValues` over the whole new
    /// `KafkaConfig`.
    ///
    /// # Errors
    /// `INVALID_REQUEST` with the message of the reader that failed.
    fn check_values(&self) -> Result<(), (i16, String)> {
        if !listener_values_changed(&self.current, &self.proposed) {
            return Ok(());
        }
        validate_values(&self.proposed).map_err(|message| (codes::INVALID_REQUEST, message))
    }

    /// Kafka's `DynamicListenerConfig.validateReconfiguration`, which runs
    /// after the listeners' own `SslFactory` checks.
    ///
    /// # Errors
    /// `INVALID_REQUEST`, wrapped as `Invalid dynamic configuration`.
    fn check_reconfiguration(&self) -> Result<(), (i16, String)> {
        if !listener_reconfiguration_needed(&self.current, &self.proposed) {
            return Ok(());
        }
        validate_reconfiguration(&self.current, &self.proposed).map_err(|cause| {
            (
                codes::INVALID_REQUEST,
                invalid_dynamic_configuration(&cause),
            )
        })
    }
}

/// What the reconfiguration checks of an alter of this node's own `BROKER`
/// resource need of the node: its running listeners for their `SslFactory`
/// checks, and its static config as Kafka keys.
pub(crate) struct OwnBrokerChecks<'a> {
    pub(crate) listener_tls: ListenerTlsInputs<'a>,
    pub(crate) static_config: BTreeMap<String, String>,
}

impl OwnBrokerChecks<'_> {
    /// Kafka's checks of the node's `proposed` per-broker configs against its
    /// `current` ones, in `processReconfiguration`'s order: the new config's
    /// listener values, then the TLS listeners' new keystores and
    /// truststores, then the listeners it keeps.
    ///
    /// # Errors
    /// `INVALID_REQUEST` with Kafka's message for the first check that fails.
    pub(crate) fn check(
        self,
        image: &MetadataImage,
        current: &BTreeMap<String, String>,
        proposed: &BTreeMap<String, String>,
    ) -> Result<(), (i16, String)> {
        let listeners = ListenerChange::new(&self.static_config, image, current, proposed);
        listeners.check_values()?;
        self.listener_tls.check_reconfiguration(current, proposed)?;
        listeners.check_reconfiguration()
    }
}

#[cfg(test)]
mod tests;
