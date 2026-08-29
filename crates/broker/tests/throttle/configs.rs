//! Wire drivers for the two config RPCs the throttle keys travel on:
//! `IncrementalAlterConfigs` (`api_key` 44) in both its SASL and its PLAINTEXT
//! flavour, and `DescribeConfigs` (`api_key` 32).
//!
//! Setting a throttle is what every test in the suite does first, so the
//! request-shaping boilerplate is collected here and the tests keep only the
//! resource rows they care about.

use std::net::SocketAddr;

use bytes::BytesMut;
use krabka_protocol::{Decode, Encode};
use tokio::net::TcpStream;

use crate::wire::{round_trip, sasl_plain_authenticate};

pub type ConfigOperations = Vec<(String, Option<String>, i8)>;
pub type ConfigResources = Vec<(i8, String, ConfigOperations)>;

/// Drive `IncrementalAlterConfigs` (`api_key=44`) over a SASL/PLAIN connection.
/// `resources` is a list of `(resource_type, name, [(config_name, value, op)])`.
/// Returns the top-level error code from the first resource response. 0 means
/// success.
pub async fn drive_incremental_alter_configs(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    resources: ConfigResources,
) -> i16 {
    const VERSION: i16 = 1;

    use krabka_protocol::owned::{
        incremental_alter_configs_request::{
            AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
        },
        incremental_alter_configs_response::IncrementalAlterConfigsResponse,
    };

    let req = IncrementalAlterConfigsRequest {
        resources: resources
            .into_iter()
            .map(
                |(resource_type, resource_name, configs)| AlterConfigsResource {
                    resource_type,
                    resource_name,
                    configs: configs
                        .into_iter()
                        .map(|(name, value, config_operation)| AlterableConfig {
                            name,
                            config_operation,
                            value,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
            )
            .collect(),
        validate_only: false,
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for IncrementalAlterConfigs");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION)
        .expect("encode IncrementalAlterConfigs");
    let resp_bytes = round_trip(&mut stream, 44, VERSION, 1, true, &body)
        .await
        .expect("IncrementalAlterConfigs round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = IncrementalAlterConfigsResponse::decode(&mut cur, VERSION)
        .expect("decode IncrementalAlterConfigsResponse");

    resp.responses.first().map_or(0, |r| r.error_code)
}

/// Drive `IncrementalAlterConfigs` (`api_key=44`) over a PLAINTEXT connection.
/// There is no SASL, and the compat shim allows everything.
pub async fn drive_incremental_alter_configs_plaintext(
    addr: SocketAddr,
    resources: ConfigResources,
) -> i16 {
    const VERSION: i16 = 1;

    use krabka_protocol::owned::{
        incremental_alter_configs_request::{
            AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
        },
        incremental_alter_configs_response::IncrementalAlterConfigsResponse,
    };

    let req = IncrementalAlterConfigsRequest {
        resources: resources
            .into_iter()
            .map(
                |(resource_type, resource_name, configs)| AlterConfigsResource {
                    resource_type,
                    resource_name,
                    configs: configs
                        .into_iter()
                        .map(|(name, value, config_operation)| AlterableConfig {
                            name,
                            config_operation,
                            value,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
            )
            .collect(),
        validate_only: false,
        ..Default::default()
    };

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION)
        .expect("encode IncrementalAlterConfigs");
    let resp_bytes = round_trip(&mut stream, 44, VERSION, 1, true, &body)
        .await
        .expect("IncrementalAlterConfigs round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = IncrementalAlterConfigsResponse::decode(&mut cur, VERSION)
        .expect("decode IncrementalAlterConfigsResponse");

    resp.responses.first().map_or(0, |r| r.error_code)
}

/// Drive `DescribeConfigs` (`api_key=32`, version=1) over a SASL/PLAIN connection.
/// Returns `Vec<(per-resource error_code, Vec<(name, value)>)>`.
#[allow(dead_code)]
pub async fn drive_describe_configs(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    resources: Vec<(i8, String)>,
) -> Vec<(i16, Vec<(String, String)>)> {
    const VERSION: i16 = 1;

    use krabka_protocol::owned::{
        describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
        describe_configs_response::DescribeConfigsResponse,
    };

    let req = DescribeConfigsRequest {
        resources: resources
            .into_iter()
            .map(|(resource_type, resource_name)| DescribeConfigsResource {
                resource_type,
                resource_name,
                configuration_keys: None,
                ..Default::default()
            })
            .collect(),
        include_synonyms: false,
        include_documentation: false,
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for DescribeConfigs");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION)
        .expect("encode DescribeConfigs");
    let resp_bytes = round_trip(&mut stream, 32, VERSION, 1, false, &body)
        .await
        .expect("DescribeConfigs round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp =
        DescribeConfigsResponse::decode(&mut cur, VERSION).expect("decode DescribeConfigsResponse");

    resp.results
        .into_iter()
        .map(|r| {
            let configs = r
                .configs
                .into_iter()
                .map(|c| (c.name, c.value.unwrap_or_default()))
                .collect();
            (r.error_code, configs)
        })
        .collect()
}
