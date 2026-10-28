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
    admission: Option<(RequestAuthState, i16)>,
    decision: Option<AclDecision>,
    matches: Option<bool>,
}

#[fixture]
fn acl() -> AclScenario {
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

#[given("ACL facts superuser {super_user} allow {allow} deny {deny} default allow {default_allow}")]
fn acl_facts(
    acl: &mut AclScenario,
    super_user: bool,
    allow: bool,
    deny: bool,
    default_allow: bool,
) {
    acl.facts = Some(AclFacts {
        super_user,
        saw_allow: allow,
        saw_deny: deny,
        default_decision: if default_allow {
            AclDefault::Allow
        } else {
            AclDefault::Deny
        },
    });
}

#[when("the ACL decision is evaluated")]
fn evaluate_decision(acl: &mut AclScenario) {
    acl.decision = Some(acl_decision(acl.facts.expect("ACL facts were given")));
}

#[then("the ACL decision is {decision}")]
fn decision_matches(acl: &AclScenario, decision: String) {
    let expected = match decision.as_str() {
        "AllowSuperuser" => AclDecision::AllowSuperuser,
        "AllowAcl" => AclDecision::AllowAcl,
        "AllowNoAcl" => AclDecision::AllowNoAcl,
        "DenyExplicit" => AclDecision::DenyExplicit,
        "DenyDefault" => AclDecision::DenyDefault,
        _ => panic!("unknown ACL decision: {decision}"),
    };
    check!(acl.decision == Some(expected));
}

#[given("an ACL identity wildcard {wildcard} exact {exact}")]
fn identity_facts(acl: &mut AclScenario, wildcard: bool, exact: bool) {
    acl.identity = Some((wildcard, exact));
}

#[when("the ACL identity is matched")]
fn match_identity(acl: &mut AclScenario) {
    let (wildcard, exact) = acl.identity.expect("identity facts were given");
    acl.matches = Some(acl_identity_match(wildcard, exact));
}

#[given(
    "an ACL resource pattern {pattern} same type {same_type} exact {exact} wildcard {wildcard} prefix {prefix}"
)]
fn resource_facts(
    acl: &mut AclScenario,
    pattern: String,
    same_type: bool,
    exact: bool,
    wildcard: bool,
    prefix: bool,
) {
    let pattern = match pattern.as_str() {
        "Literal" => AclPatternKind::Literal,
        "Prefixed" => AclPatternKind::Prefixed,
        _ => panic!("unknown ACL resource pattern: {pattern}"),
    };
    acl.resource = Some((pattern, AclResourceFacts {
        resource_type: if same_type {
            AclResourceTypeMatch::Same
        } else {
            AclResourceTypeMatch::Different
        },
        exact_name: exact,
        wildcard_name: wildcard,
        name_has_prefix: prefix,
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

#[given("an ACL operation {stored} requested {requested} allow {allow}")]
fn operation_facts(acl: &mut AclScenario, stored: String, requested: String, allow: bool) {
    acl.operation = Some((operation_kind(&stored), operation_kind(&requested), allow));
}

#[when("the ACL operation is matched")]
fn match_operation(acl: &mut AclScenario) {
    let (stored, requested, allow) = acl.operation.expect("operation facts were given");
    acl.matches = Some(acl_operation_match(stored, requested, allow));
}

#[given("authentication state {state} and API key {api_key}")]
fn authentication_facts(acl: &mut AclScenario, state: String, api_key: i16) {
    let state = match state.as_str() {
        "PreHandshake" => RequestAuthState::PreHandshake,
        "Exchanging" => RequestAuthState::Exchanging,
        "Failed" => RequestAuthState::Failed,
        "Authenticated" => RequestAuthState::Authenticated,
        _ => panic!("unknown authentication state: {state}"),
    };
    acl.admission = Some((state, api_key));
}

#[when("request admission is evaluated")]
fn evaluate_admission(acl: &mut AclScenario) {
    let (state, api_key) = acl.admission.expect("authentication facts were given");
    acl.matches = Some(request_auth_admission(state, api_key));
}

#[then("the ACL match is {matches}")]
fn match_result(acl: &AclScenario, matches: bool) {
    check!(acl.matches == Some(matches));
}
scenarios!("tests/features/acl", fixtures = [acl: AclScenario]);
