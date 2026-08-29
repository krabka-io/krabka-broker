use assert2::assert;
use tempfile::tempdir;

use super::*;

#[test]
fn file_config_self_registration_uses_advertised_listener_for_legacy_endpoint() {
    let file: crate::file_config::FileConfig = toml::from_str(
        r#"
inter_broker_listener_name = "INTERNAL"

[[listeners]]
name = "EXTERNAL"
bind_addr = "127.0.0.1:19094"
advertised = "external.example:29094"
protocol = "Plaintext"

[[listeners]]
name = "INTERNAL"
bind_addr = "127.0.0.1:19093"
advertised = "internal.example:29093"
protocol = "Plaintext"
"#,
    )
    .expect("parse file config");
    let mut config = BrokerConfig::default();
    assert!(
        config.listen_addr.port() == 9092,
        "preserve CLI default precondition"
    );
    file.apply_to(&mut config).expect("apply file config");

    let registration = self_registration_record(&config);

    assert!(registration.host == "internal.example");
    assert!(registration.port == 29093);
    assert!(
        registration
            .endpoints
            .iter()
            .map(|endpoint| (
                endpoint.name.as_str(),
                endpoint.host.as_str(),
                endpoint.port
            ))
            .collect::<Vec<_>>()
            == vec![
                ("EXTERNAL", "external.example", 29094),
                ("INTERNAL", "internal.example", 29093),
            ]
    );
}

/// A stretch profile with two data sites and one witness site, with
/// leadership pinned to `dc-a`.
fn three_site_profile() -> crate::config::StretchProfile {
    crate::config::StretchProfile {
        sites: vec!["dc-a".to_string(), "dc-b".to_string(), "dc-w".to_string()],
        witness_site: "dc-w".to_string(),
        preferred_leader_site: "dc-a".to_string(),
    }
}

/// A node of node id 4 that carries `roles` and logs into `log_dir`.
///
/// The registration record mints a directory id under `log_dir`, so the
/// caller passes a temporary directory rather than the source tree.
fn node_with_roles(log_dir: &std::path::Path, roles: Vec<crate::config::NodeRole>) -> BrokerConfig {
    BrokerConfig {
        node_id: krabka_metadata::NodeId(4),
        roles,
        ..BrokerConfig::for_tests(log_dir.to_path_buf())
    }
}

fn broker_witness_record(node_id: u64, value: Option<&str>) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1BrokerConfig(krabka_metadata::BrokerConfigRecord {
        node_id: krabka_metadata::NodeId(node_id),
        config_name: "broker.witness".into(),
        config_value: value.map(str::to_string),
    })
}

#[test]
fn a_witness_registration_batch_publishes_the_witness_role() {
    let log_dir = tempdir().expect("temp log dir");
    let config = node_with_roles(
        log_dir.path(),
        vec![
            crate::config::NodeRole::Controller,
            crate::config::NodeRole::Broker,
            crate::config::NodeRole::Witness,
        ],
    );

    assert!(
        broker_registration_batch(&config)
            == vec![
                krabka_metadata::MetadataRecord::V1BrokerRegistration(self_registration_record(
                    &config
                )),
                broker_witness_record(4, Some("true")),
            ]
    );
}

#[test]
fn a_plain_broker_registration_batch_clears_the_witness_role() {
    let log_dir = tempdir().expect("temp log dir");
    let config = node_with_roles(
        log_dir.path(),
        vec![
            crate::config::NodeRole::Controller,
            crate::config::NodeRole::Broker,
        ],
    );

    assert!(
        broker_registration_batch(&config)
            == vec![
                krabka_metadata::MetadataRecord::V1BrokerRegistration(self_registration_record(
                    &config
                )),
                broker_witness_record(4, None),
            ]
    );
}

#[test]
fn a_stretch_profile_publishes_the_preferred_leader_site_as_a_cluster_default() {
    let config = BrokerConfig {
        stretch: Some(three_site_profile()),
        ..BrokerConfig::for_tests(std::path::PathBuf::new())
    };

    assert!(
        stretch_default_records(&config)
            == vec![krabka_metadata::MetadataRecord::V1BrokerConfig(
                krabka_metadata::BrokerConfigRecord {
                    node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                    config_name: "stretch.preferred.leader.site".into(),
                    config_value: Some("dc-a".into()),
                }
            )]
    );
}

#[test]
fn a_node_without_a_stretch_profile_publishes_no_cluster_default() {
    let config = BrokerConfig::for_tests(std::path::PathBuf::new());

    assert!(stretch_default_records(&config) == vec![]);
}

#[test]
fn self_controller_registration_uses_quorum_endpoint_and_feature_ranges() {
    let config = BrokerConfig {
        node_id: krabka_metadata::NodeId(7),
        incarnation_id: uuid::Uuid::from_u128(0xCAFE),
        controller_quorum_voters: vec![(
            krabka_metadata::NodeId(7),
            "controller.example:19093".into(),
        )],
        controller_listener_protocol: krabka_security::ListenerProtocol::Ssl,
        ..Default::default()
    };

    let registration = self_controller_registration_record(&config);

    assert!(registration.node_id == krabka_metadata::NodeId(7));
    assert!(registration.incarnation_id == uuid::Uuid::from_u128(0xCAFE));
    assert!(registration.features == krabka_metadata::supported_feature_ranges());
    assert!(
        registration.endpoints
            == vec![krabka_metadata::BrokerEndpoint {
                name: "CONTROLLER".into(),
                host: "controller.example".into(),
                port: 19093,
                protocol: krabka_security::ListenerProtocol::Ssl,
            }]
    );
}

#[test]
fn controller_registration_starts_at_kip_919_floor_and_is_idempotent() {
    let config = BrokerConfig::default();
    let registration = self_controller_registration_record(&config);
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&krabka_metadata::MetadataRecord::V1FeatureLevel(
        krabka_metadata::FeatureLevelRecord {
            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: krabka_metadata::metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL - 1,
        },
    ));

    assert!(controller_registration_update(&image, &registration).is_none());

    image.apply(&krabka_metadata::MetadataRecord::V1FeatureLevel(
        krabka_metadata::FeatureLevelRecord {
            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: krabka_metadata::metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL,
        },
    ));
    let update = controller_registration_update(&image, &registration)
        .expect("crossing the KIP-919 floor registers the controller");
    image.apply(&update);

    assert!(image.controller(config.node_id) == Some(&registration));
    assert!(controller_registration_update(&image, &registration).is_none());
}
