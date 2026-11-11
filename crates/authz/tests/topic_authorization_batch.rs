//! Public batch authorization keeps dispatch mode and input ownership.
#![cfg(test)]

use {
    assert2::assert,
    krabka_authz::{
        AclSource, AuthorizationRequest, AuthorizationResult, Authorizer, authorize_topics,
        authorize_topics_logged,
    },
    krabka_bdd::{StepParam, given, then, when},
    krabka_metadata::{AclOperation, MetadataImage, ResourceType},
    krabka_security::{AuthMethod, Principal},
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::DataTableRow,
    std::{net::SocketAddr, ptr, sync::Mutex},
    uuid::Uuid,
};

#[derive(Debug, PartialEq, Eq)]
struct DispatchCall {
    logged: bool,
    resource_type: ResourceType,
    resource: String,
    operation: AclOperation,
    principal: Principal,
    host: SocketAddr,
}

#[derive(Debug, Default)]
struct DispatchProbe {
    calls: Mutex<Vec<DispatchCall>>,
}

impl DispatchProbe {
    fn record(&self, request: &AuthorizationRequest<'_>, logged: bool) -> AuthorizationResult {
        self.calls.lock().unwrap().push(DispatchCall {
            logged,
            resource_type: request.resource_type,
            resource: request.resource_name.to_owned(),
            operation: request.operation,
            principal: request.principal.clone(),
            host: *request.host,
        });
        AuthorizationResult::Deny
    }
}

impl Authorizer for DispatchProbe {
    fn authorize(&self, _: &dyn AclSource, req: &AuthorizationRequest<'_>) -> AuthorizationResult {
        self.record(req, true)
    }

    fn authorize_quiet(
        &self,
        _: &dyn AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        self.record(req, false)
    }
}

#[derive(Clone, Copy, StepParam)]
enum DispatchMode {
    Default,
    #[step_param("enabled")]
    Logged,
    #[step_param("disabled")]
    Quiet,
}

struct DecisionObservation {
    topic: String,
    decision: AuthorizationResult,
    borrowed_key: *const u8,
}

struct BatchScenario {
    principal: Option<Principal>,
    host: Option<SocketAddr>,
    topics: Vec<String>,
    input_order: Vec<usize>,
    probe: DispatchProbe,
    decisions: Option<Vec<DecisionObservation>>,
}

#[fixture]
fn batch() -> BatchScenario {
    BatchScenario {
        principal: None,
        host: None,
        topics: Vec::new(),
        input_order: Vec::new(),
        probe: DispatchProbe::default(),
        decisions: None,
    }
}

#[given("an authorizer denies WRITE access for principal {name} from {host} to these topics:")]
fn denying_authorizer(
    batch: &mut BatchScenario,
    name: String,
    host: SocketAddr,
    #[datatable] rows: Rows<TopicRow>,
) {
    batch.principal = Some(Principal {
        name,
        auth_method: AuthMethod::SaslPlain,
        groups: Vec::new(),
    });
    batch.host = Some(host);
    topic_sequence(batch, rows);
}

#[derive(DataTableRow)]
struct TopicRow {
    topic: String,
}

fn topic_sequence(batch: &mut BatchScenario, rows: Rows<TopicRow>) {
    for row in rows {
        let index = batch
            .topics
            .iter()
            .position(|topic| *topic == row.topic)
            .unwrap_or_else(|| {
                batch.topics.push(row.topic);
                batch.topics.len() - 1
            });
        batch.input_order.push(index);
    }
}

#[when("the broker checks the batch with {mode} denial logging")]
fn authorize_batch(batch: &mut BatchScenario, mode: DispatchMode) {
    let principal = batch.principal.as_ref().expect("principal was given");
    let host = batch.host.as_ref().expect("request host was given");
    let source = MetadataImage::new(Uuid::nil());
    let names = batch
        .input_order
        .iter()
        .map(|index| batch.topics[*index].as_str());
    let decisions = match mode {
        DispatchMode::Default => authorize_topics(
            &batch.probe,
            &source,
            principal,
            host,
            AclOperation::Write,
            names,
        ),
        DispatchMode::Logged | DispatchMode::Quiet => authorize_topics_logged(
            &batch.probe,
            &source,
            principal,
            host,
            AclOperation::Write,
            names,
            matches!(mode, DispatchMode::Logged),
        ),
    };
    batch.decisions = Some(
        decisions
            .into_iter()
            .map(|(topic, decision)| DecisionObservation {
                topic: topic.to_owned(),
                decision,
                borrowed_key: topic.as_ptr(),
            })
            .collect(),
    );
}

#[derive(DataTableRow)]
struct DispatchRow {
    topic: String,
    logged: bool,
}

#[then("the authorization checks are recorded in request order:")]
fn dispatch_history(batch: &BatchScenario, #[datatable] rows: Rows<DispatchRow>) {
    let principal = batch.principal.as_ref().expect("principal was given");
    let host = batch.host.expect("request host was given");
    let expected: Vec<_> = rows
        .into_iter()
        .map(|row| DispatchCall {
            logged: row.logged,
            resource_type: ResourceType::Topic,
            resource: row.topic,
            operation: AclOperation::Write,
            principal: principal.clone(),
            host,
        })
        .collect();
    assert!(batch.probe.calls.lock().unwrap().as_slice() == expected.as_slice());
}

#[then("the authorization results deny only these topics:")]
fn denied_topics(batch: &BatchScenario, #[datatable] rows: Rows<TopicRow>) {
    let mut actual: Vec<_> = batch
        .decisions
        .as_ref()
        .expect("batch was authorized")
        .iter()
        .map(|observation| (observation.topic.as_str(), observation.decision))
        .collect();
    let mut expected: Vec<_> = rows
        .into_iter()
        .map(|row| (row.topic, AuthorizationResult::Deny))
        .collect();
    actual.sort_by_key(|(topic, _)| *topic);
    expected.sort_by(|left, right| left.0.cmp(&right.0));
    let expected: Vec<_> = expected
        .iter()
        .map(|(topic, decision)| (topic.as_str(), *decision))
        .collect();
    assert!(actual == expected);
}

#[then("the results reuse the caller's topic names")]
fn borrowed_keys(batch: &BatchScenario) {
    for observation in batch.decisions.as_ref().expect("batch was authorized") {
        let original = batch
            .topics
            .iter()
            .find(|topic| **topic == observation.topic)
            .expect("decision refers to an original topic");
        assert!(ptr::eq(observation.borrowed_key, original.as_ptr()));
    }
}

krabka_bdd::scenarios!("tests/features/topic_authorization_batch", fixtures = [batch: BatchScenario]);
