use {super::*, assert2::check};

fn map(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect()
}

/// A broker-only node with a client listener and a SASL inter-broker one.
fn broker_only() -> BTreeMap<String, String> {
    map(&[
        (PROCESS_ROLES, "broker"),
        (
            LISTENERS,
            "PLAINTEXT://0.0.0.0:9092,INTERNAL://0.0.0.0:9094",
        ),
        (
            LISTENER_SECURITY_PROTOCOL_MAP,
            "PLAINTEXT:PLAINTEXT,INTERNAL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
        ),
        (
            "advertised.listeners",
            "PLAINTEXT://localhost:9092,INTERNAL://localhost:9094",
        ),
        (CONTROLLER_LISTENER_NAMES, "CONTROLLER"),
        (INTER_BROKER_LISTENER_NAME, "INTERNAL"),
        (SASL_ENABLED_MECHANISMS, "SCRAM-SHA-512"),
        (SASL_MECHANISM_INTER_BROKER_PROTOCOL, "SCRAM-SHA-512"),
    ])
}

/// A combined node with one data-plane listener.
fn combined() -> BTreeMap<String, String> {
    map(&[
        (PROCESS_ROLES, "broker,controller"),
        (
            LISTENERS,
            "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
        ),
        (
            LISTENER_SECURITY_PROTOCOL_MAP,
            "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        ),
        ("advertised.listeners", "PLAINTEXT://localhost:9092"),
        (CONTROLLER_LISTENER_NAMES, "CONTROLLER"),
        (INTER_BROKER_LISTENER_NAME, "PLAINTEXT"),
        (SASL_ENABLED_MECHANISMS, "GSSAPI"),
    ])
}

/// Kafka's `validateValues` and then `validateReconfiguration`, as the alter
/// paths run them around the `SslFactory` checks.
fn check(
    static_config: &BTreeMap<String, String>,
    current: &[(&str, &str)],
    proposed: &[(&str, &str)],
) -> Result<(), (i16, String)> {
    let change = ListenerChange {
        current: ListenerView::new(static_config, None, &map(current)),
        proposed: ListenerView::new(static_config, None, &map(proposed)),
    };
    change
        .check_values()
        .and_then(|()| change.check_reconfiguration())
}

fn refused(message: &str) -> Result<(), (i16, String)> {
    Err((codes::INVALID_REQUEST, message.to_owned()))
}

fn reconfiguration_refused(cause: &str) -> Result<(), (i16, String)> {
    refused(&format!(
        "Invalid value org.apache.kafka.common.config.ConfigException: {cause} for configuration \
         Invalid dynamic configuration"
    ))
}

const ADDED: (&str, &str) = (
    LISTENERS,
    "PLAINTEXT://0.0.0.0:9092,INTERNAL://0.0.0.0:9094,EXTRA://:9095",
);
const ADDED_MAP: (&str, &str) = (
    LISTENER_SECURITY_PROTOCOL_MAP,
    "PLAINTEXT:PLAINTEXT,INTERNAL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT,EXTRA:SSL",
);

#[yare::parameterized(
    a_new_listener = { &[], &[ADDED, ADDED_MAP], Ok(()) },
    removing_the_new_listener = { &[ADDED, ADDED_MAP], &[], Ok(()) },
    a_listener_without_a_protocol = {
        &[],
        &[ADDED],
        refused(
            "Error creating broker listeners from '[PLAINTEXT://0.0.0.0:9092, \
             INTERNAL://0.0.0.0:9094, EXTRA://:9095]': No security protocol defined for listener \
             EXTRA",
        )
    },
    an_advertised_listener_dropped = {
        &[],
        &[(LISTENERS, "INTERNAL://0.0.0.0:9094")],
        refused(
            "requirement failed: advertised.listeners listener names must be equal to or a \
             subset of the ones defined in listeners. Found PLAINTEXT,INTERNAL. The valid options \
             based on the current configuration are INTERNAL",
        )
    },
    an_advertised_listener_unmapped = {
        &[],
        &[(LISTENER_SECURITY_PROTOCOL_MAP, "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT")],
        refused(
            "Error creating broker listeners from '[PLAINTEXT://localhost:9092, \
             INTERNAL://localhost:9094]': No security protocol defined for listener INTERNAL",
        )
    },
    the_controller_unmapped = {
        &[],
        &[(LISTENER_SECURITY_PROTOCOL_MAP, "PLAINTEXT:PLAINTEXT,INTERNAL:SASL_PLAINTEXT")],
        refused(
            "Controller listener with name CONTROLLER defined in controller.listener.names not \
             found in listener.security.protocol.map  (an explicit security mapping for each \
             controller listener is required if listener.security.protocol.map is non-empty, or \
             if there are security protocols other than PLAINTEXT in use)",
        )
    },
    the_controller_opened_on_a_broker = {
        &[],
        &[(
            LISTENERS,
            "PLAINTEXT://0.0.0.0:9092,INTERNAL://0.0.0.0:9094,CONTROLLER://:9093",
        )],
        refused(
            "requirement failed: controller.listener.names must not contain a value appearing in \
             the 'listeners' configuration when running KRaft with just the broker role",
        )
    },
    a_shared_port = {
        &[],
        &[(LISTENERS, "PLAINTEXT://0.0.0.0:9092,INTERNAL://0.0.0.0:9092")],
        refused(
            "Error creating broker listeners from '[PLAINTEXT://0.0.0.0:9092, \
             INTERNAL://0.0.0.0:9092]': If you have two listeners on the same port then one \
             needs to be IPv4 and the other IPv6, listeners: [PLAINTEXT://0.0.0.0:9092, \
             INTERNAL://0.0.0.0:9092], port: 9092",
        )
    },
    the_inter_broker_mechanism_disabled = {
        &[],
        &[(SASL_ENABLED_MECHANISMS, "PLAIN")],
        refused(
            "requirement failed: sasl.mechanism.inter.broker.protocol must be included in \
             sasl.enabled.mechanisms when SASL is used for inter-broker communication",
        )
    },
    the_inter_broker_listener_keeps_its_mechanism = {
        &[("listener.name.internal.sasl.enabled.mechanisms", "SCRAM-SHA-512")],
        &[
            (SASL_ENABLED_MECHANISMS, "PLAIN"),
            ("listener.name.internal.sasl.enabled.mechanisms", "SCRAM-SHA-512"),
        ],
        Ok(())
    },
    an_existing_listener_gains_a_key_beside_a_broker_wide_change = {
        &[],
        &[
            (SASL_ENABLED_MECHANISMS, "PLAIN"),
            ("listener.name.internal.sasl.enabled.mechanisms", "SCRAM-SHA-512"),
        ],
        reconfiguration_refused(
            "Configs cannot be updated dynamically for existing listener \
             ListenerName(INTERNAL), restart broker or create a new listener for update",
        )
    },
    an_existing_listener_changes_protocol = {
        &[],
        &[(
            LISTENER_SECURITY_PROTOCOL_MAP,
            "PLAINTEXT:SSL,INTERNAL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
        )],
        reconfiguration_refused(
            "Security protocol cannot be updated for existing listener ListenerName(PLAINTEXT)",
        )
    },
    an_existing_listener_changes_its_own_key = {
        &[],
        &[ADDED, ADDED_MAP, ("listener.name.plaintext.sasl.enabled.mechanisms", "PLAIN")],
        reconfiguration_refused(
            "Configs cannot be updated dynamically for existing listener \
             ListenerName(PLAINTEXT), restart broker or create a new listener for update",
        )
    },
    an_existing_listener_changes_a_key_alone = {
        &[],
        &[("listener.name.plaintext.sasl.jaas.config", "x required;")],
        Ok(())
    },
    an_existing_listener_changes_its_keystore = {
        &[],
        &[ADDED, ADDED_MAP, ("listener.name.plaintext.ssl.keystore.location", "/k.pem")],
        Ok(())
    },
    a_broker_wide_sasl_key = {
        &[("listener.name.plaintext.sasl.jaas.config", "x required;")],
        &[
            ("listener.name.plaintext.sasl.jaas.config", "x required;"),
            ("sasl.jaas.config", "y required;"),
        ],
        Ok(())
    },
)]
fn a_broker_only_listener_change(
    current: &[(&str, &str)],
    proposed: &[(&str, &str)],
    expected: Result<(), (i16, String)>,
) {
    check!(check(&broker_only(), current, proposed) == expected);
}

#[yare::parameterized(
    a_new_listener = {
        &[
            (LISTENERS, "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTRA://:9095"),
            (LISTENER_SECURITY_PROTOCOL_MAP, "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTRA:PLAINTEXT"),
        ],
        Ok(())
    },
    the_controller_dropped = {
        &[(LISTENERS, "PLAINTEXT://0.0.0.0:9092")],
        refused(
            "requirement failed: controller.listener.names must contain at least one value \
             appearing in the 'listeners' configuration when running the KRaft controller role",
        )
    },
)]
fn a_combined_listener_change(proposed: &[(&str, &str)], expected: Result<(), (i16, String)>) {
    check!(check(&combined(), &[], proposed) == expected);
}
