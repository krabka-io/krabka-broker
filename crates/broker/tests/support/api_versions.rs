use {
    krabka_broker::{
        api_catalog,
        api_catalog::{ClientMetricsReceiver, ListenerKind, VersionGates},
    },
    krabka_protocol::owned::api_versions_response::ApiVersion,
};

pub fn supported_client_apis() -> Vec<ApiVersion> {
    let mut apis = api_catalog::supported_apis(
        ListenerKind::Client,
        ClientMetricsReceiver::Absent,
        VersionGates::default(),
    );
    apis.sort_by_key(|api| api.api_key);
    apis
}
