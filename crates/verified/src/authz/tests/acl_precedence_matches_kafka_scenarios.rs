use {
    super::{
        AclDecision, AclDefault, AclFacts, AclOperationKind, AclPatternKind, AclResourceFacts,
        AclResourceTypeMatch, RequestAuthState, acl_decision, acl_identity_match,
        acl_operation_match, acl_resource_match, check, request_auth_admission,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenarios, then, when},
};

struct AclScenario {
    facts: Option<AclFacts>,
    identity: Option<(bool, bool)>,
    resource: Option<(AclPatternKind, AclResourceFacts)>,
    operation: Option<(AclOperationKind, AclOperationKind, bool)>,
    admission: Option<RequestAuthState>,
    decision: Option<AclDecision>,
    matches: Option<bool>,
}

#[fixture]
fn acl() -> AclScenario {
    // Start this scenario with fresh inputs and observations.

    AclScenario {
        facts: None,
        identity: None,
        resource: None,
        operation: None,
        admission: None,
        decision: None,
        matches: None,
    }
}

#[given(
    "a {principal} principal has matching entries {entries} and the no-ACL policy is {default_policy}"
)]
fn acl_facts(acl: &mut AclScenario, principal: String, entries: String, default_policy: String) {
    let super_user = match principal.as_str() {
        "superuser" => true,
        "regular" => false,
        _ => panic!("unknown principal status"),
    };
    let (saw_allow, saw_deny) = match entries.as_str() {
        "none" => (false, false),
        "allow" => (true, false),
        "deny" => (false, true),
        "allow-and-deny" => (true, true),
        _ => panic!("unknown matching ACL permissions"),
    };
    let default_decision = match default_policy.as_str() {
        "allow" => AclDefault::Allow,
        "deny" => AclDefault::Deny,
        _ => panic!("unknown no-ACL policy"),
    };
    acl.facts = Some(AclFacts {
        super_user,
        saw_allow,
        saw_deny,
        default_decision,
    });
}

#[when("the ACL decision is evaluated")]
fn evaluate_decision(acl: &mut AclScenario) {
    acl.decision = Some(acl_decision(acl.facts.expect("ACL facts were given")));
}

#[then("authorization is {decision}")]
fn decision_matches(acl: &AclScenario, decision: String) {
    let expected = match decision.as_str() {
        "allowed by superuser status" => AclDecision::AllowSuperuser,
        "allowed by a matching ACL" => AclDecision::AllowAcl,
        "allowed because no ACL applies" => AclDecision::AllowNoAcl,
        "denied by an explicit ACL" => AclDecision::DenyExplicit,
        "denied because no ACL grants access" => AclDecision::DenyDefault,
        _ => panic!("unknown ACL decision: {decision}"),
    };
    check!(acl.decision == Some(expected));
}

#[given("an ACL {identity} entry {stored} is checked against {requested}")]
fn identity_facts(acl: &mut AclScenario, identity: String, stored: String, requested: String) {
    match identity.as_str() {
        "principal" | "host" => {}
        _ => panic!("unknown ACL identity field"),
    }
    acl.identity = Some((stored == "*", stored == requested));
}

#[when("the ACL identity is matched")]
fn match_identity(acl: &mut AclScenario) {
    let (wildcard, exact) = acl.identity.expect("identity facts were given");
    acl.matches = Some(acl_identity_match(wildcard, exact));
}

#[given(
    "a {pattern} ACL on {stored_type} named {stored_name} is checked against {requested_type} named {requested_name}"
)]
fn resource_facts(
    acl: &mut AclScenario,
    pattern: String,
    stored_type: String,
    stored_name: String,
    requested_type: String,
    requested_name: String,
) {
    let pattern = match pattern.as_str() {
        "Literal" => AclPatternKind::Literal,
        "Prefixed" => AclPatternKind::Prefixed,
        _ => panic!("unknown ACL resource pattern"),
    };
    acl.resource = Some((pattern, AclResourceFacts {
        resource_type: if stored_type == requested_type {
            AclResourceTypeMatch::Same
        } else {
            AclResourceTypeMatch::Different
        },
        exact_name: stored_name == requested_name,
        wildcard_name: stored_name == "*",
        name_has_prefix: requested_name.starts_with(&stored_name),
    }));
}

#[when("the ACL resource is matched")]
fn match_resource(acl: &mut AclScenario) {
    let (pattern, facts) = acl.resource.expect("resource facts were given");
    acl.matches = Some(acl_resource_match(pattern, facts));
}

fn operation_kind(name: &str) -> AclOperationKind {
    match name {
        "All" => AclOperationKind::All,
        "Read" => AclOperationKind::Read,
        "Write" => AclOperationKind::Write,
        "Create" => AclOperationKind::Create,
        "Delete" => AclOperationKind::Delete,
        "Alter" => AclOperationKind::Alter,
        "Describe" => AclOperationKind::Describe,
        "ClusterAction" => AclOperationKind::ClusterAction,
        "DescribeConfigs" => AclOperationKind::DescribeConfigs,
        "AlterConfigs" => AclOperationKind::AlterConfigs,
        _ => panic!("unknown ACL operation: {name}"),
    }
}

#[given(
    "an ACL {permission} entry for {stored} is checked against requested operation {requested}"
)]
fn operation_facts(acl: &mut AclScenario, permission: String, stored: String, requested: String) {
    let allow = match permission.as_str() {
        "Allow" => true,
        "Deny" => false,
        _ => panic!("unknown ACL permission"),
    };
    acl.operation = Some((operation_kind(&stored), operation_kind(&requested), allow));
}

#[when("the ACL operation is matched")]
fn match_operation(acl: &mut AclScenario) {
    let (stored, requested, allow) = acl.operation.expect("operation facts were given");
    acl.matches = Some(acl_operation_match(stored, requested, allow));
}

#[given("authentication state {state}")]
fn authentication_facts(acl: &mut AclScenario, state: String) {
    let state = match state.as_str() {
        "PreHandshake" => RequestAuthState::PreHandshake,
        "Exchanging" => RequestAuthState::Exchanging,
        "Failed" => RequestAuthState::Failed,
        "Authenticated" => RequestAuthState::Authenticated,
        _ => panic!("unknown authentication state: {state}"),
    };
    acl.admission = Some(state);
}

#[when("a {request} API request is admitted")]
fn evaluate_admission(acl: &mut AclScenario, request: String) {
    let state = acl.admission.expect("authentication facts were given");
    let api_key = match request.as_str() {
        "SaslHandshake" => 17,
        "ApiVersions" => 18,
        "SaslAuthenticate" => 36,
        "Produce" => 0,
        "Invalid" => -1,
        "Unsupported" => i16::MAX,
        _ => panic!("unknown authentication request: {request}"),
    };
    acl.matches = Some(request_auth_admission(state, api_key));
}

#[then("the ACL applies: {matches}")]
fn match_result(acl: &AclScenario, matches: bool) {
    check!(acl.matches == Some(matches));
}
#[then("the authentication state admits the request: {matches}")]
fn admission_result(acl: &AclScenario, matches: bool) {
    check!(acl.matches == Some(matches));
}

scenarios!("tests/features/acl", fixtures = [acl: AclScenario]);
