//! Request-derived controller ranges retain release gates and header framing.
#![cfg(test)]

use {
    assert2::assert,
    krabka_protocol::owned::{
        api_versions_request::ApiVersionsRequest, sasl_handshake_request::SaslHandshakeRequest,
        unregister_controller_request::UnregisterControllerRequest,
    },
    krabka_raft::{
        ControllerApiVersion,
        UnstableApiVersions::{Disabled, Enabled},
    },
    rstest::fixture,
    rstest_bdd::Slot,
    rstest_bdd_macros::{given, scenarios, then, when},
};

enum RequestKind {
    ApiVersions,
    UnregisterController,
    SaslHandshake,
}

struct ControllerRange {
    kind: Option<RequestKind>,
    api: Option<ControllerApiVersion>,
    released: Slot<Option<i16>>,
    unstable: Slot<Option<i16>>,
    probe: Option<(bool, bool)>,
}

#[fixture]
fn range() -> ControllerRange {
    // Start this scenario with fresh inputs and observations.

    ControllerRange {
        kind: None,
        api: None,
        released: Slot::default(),
        unstable: Slot::default(),
        probe: None,
    }
}

#[given("controller request type {request}")]
fn controller_request(range: &mut ControllerRange, request: String) {
    range.kind = Some(match request.as_str() {
        "ApiVersions" => RequestKind::ApiVersions,
        "UnregisterController" => RequestKind::UnregisterController,
        "SaslHandshake" => RequestKind::SaslHandshake,
        _ => panic!("unknown controller API fixture: {request}"),
    });
}

#[when("controller ranges and release gates are derived for probe version {version}")]
fn derive_controller_range(range: &mut ControllerRange, version: i16) {
    let api = match range
        .kind
        .as_ref()
        .expect("a controller request type was given")
    {
        RequestKind::ApiVersions => ControllerApiVersion::for_request::<ApiVersionsRequest>(),
        RequestKind::UnregisterController => {
            ControllerApiVersion::for_request::<UnregisterControllerRequest>()
        }
        RequestKind::SaslHandshake => ControllerApiVersion::for_request::<SaslHandshakeRequest>(),
    };
    range.released.set(api.enabled_max(Disabled));
    range.unstable.set(api.enabled_max(Enabled));
    range.probe = Some((
        api.is_disabled_version(version, Disabled),
        api.is_disabled_version(version, Enabled),
    ));
    range.api = Some(api);
}

#[then(
    "the controller API key is {key}, minimum version is {minimum}, and flexible framing starts at {flexible}"
)]
fn controller_header(range: &ControllerRange, key: i16, minimum: i16, flexible: String) {
    let flexible = if flexible == "never" {
        i16::MAX
    } else {
        flexible
            .parse()
            .expect("flexible framing version is an integer")
    };
    let api = range
        .api
        .as_ref()
        .expect("the controller range was derived");
    assert!((api.api_key, api.min_version, api.flexible_min) == (key, minimum, flexible));
}

#[then(
    "released maximum is {released}, unstable maximum is {unstable}, and the probe is blocked {released_blocked} without unstable support and {unstable_blocked} with it"
)]
fn controller_release_gates(
    range: &ControllerRange,
    released: String,
    unstable: String,
    released_blocked: bool,
    unstable_blocked: bool,
) {
    let maximum = |value: &str| {
        if value == "unavailable" {
            None
        } else {
            Some(value.parse().expect("maximum API version is an integer"))
        }
    };
    assert!(range.released.get() == Some(maximum(&released)));
    assert!(range.unstable.get() == Some(maximum(&unstable)));
    assert!(range.probe == Some((released_blocked, unstable_blocked)));
}

scenarios!("tests/features/migrated/tests/controller_api_version.feature", fixtures = [range: ControllerRange]);
