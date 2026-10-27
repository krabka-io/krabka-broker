use {krabka_security::gssapi, std::path::PathBuf};

pub fn broker_config(keytab_path: PathBuf, kdc: String) -> krabka_broker::GssapiConfig {
    krabka_broker::GssapiConfig {
        keytab_path,
        service_name: "kafka".to_owned(),
        principal_to_local_rules: vec![krabka_broker::KerberosRule::Default],
        realm: Some("CRABKA.TEST".to_owned()),
        kdc: Some(kdc),
        max_time_skew: gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW,
    }
}
