use {
    super::{
        HomogeneousMemberQuota, SubscriberLoad, UniformQuotaSplit, homogeneous_member_quotas,
        select_least_loaded, uniform_quota_split,
    },
    assert2::assert,
    rstest::fixture,
    rstest_bdd::{Slot, datatable::Rows},
    rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
};

#[derive(DataTableRow)]
struct QuotaRow {
    takes_extra: bool,
    retain: usize,
    fill: usize,
}

#[derive(DataTableRow)]
struct LoadRow {
    assigned: String,
    assigned_at_topic_start: usize,
}

struct AssignmentScenario {
    partition_total: usize,
    members: usize,
    minimum: usize,
    extras: usize,
    owned: Vec<usize>,
    loads: Vec<SubscriberLoad>,
    split: Option<UniformQuotaSplit>,
    quotas: Option<Vec<HomogeneousMemberQuota>>,
    selected: Slot<Option<usize>>,
}

#[fixture]
fn assignment() -> AssignmentScenario {
    // Start this scenario with fresh inputs and observations.

    AssignmentScenario {
        partition_total: 0,
        members: 0,
        minimum: 0,
        extras: 0,
        owned: Vec::new(),
        loads: Vec::new(),
        split: None,
        quotas: None,
        selected: Slot::default(),
    }
}

#[given("{total} partitions and {members} members")]
fn partition_counts(assignment: &mut AssignmentScenario, total: usize, members: usize) {
    assignment.partition_total = total;
    assignment.members = members;
}

#[when("the uniform quota is split")]
fn split_quota(assignment: &mut AssignmentScenario) {
    assignment.split = Some(uniform_quota_split(
        assignment.partition_total,
        assignment.members,
    ));
}

#[then(
    "every member has minimum quota {minimum_quota} and {extra_quotas} members receive an extra partition"
)]
fn quota_split(assignment: &AssignmentScenario, minimum_quota: usize, extra_quotas: usize) {
    assert!(
        assignment.split
            == Some(UniformQuotaSplit {
                minimum_quota,
                extra_quotas
            })
    );
}

#[given("minimum quota {minimum} with {extras} extra partitions")]
fn quota_counts(assignment: &mut AssignmentScenario, minimum: usize, extras: usize) {
    assignment.minimum = minimum;
    assignment.extras = extras;
}

#[given("members own partitions {owned}")]
fn owned_partitions(assignment: &mut AssignmentScenario, owned: String) {
    assignment.owned = owned
        .split(',')
        .map(|value| value.parse().expect("owned count is an integer"))
        .collect();
}

#[when("homogeneous member quotas are assigned")]
fn assign_member_quotas(assignment: &mut AssignmentScenario) {
    assignment.quotas = Some(homogeneous_member_quotas(
        assignment.minimum,
        assignment.extras,
        &assignment.owned,
    ));
}

#[then("the member quotas are:")]
fn member_quotas(assignment: &AssignmentScenario, #[datatable] rows: Rows<QuotaRow>) {
    let expected: Vec<_> = rows
        .into_iter()
        .map(|row| HomogeneousMemberQuota {
            takes_extra: row.takes_extra,
            retain: row.retain,
            fill: row.fill,
        })
        .collect();
    assert!(assignment.quotas.as_ref() == Some(&expected));
}

#[given("subscriber loads:")]
fn subscriber_loads(assignment: &mut AssignmentScenario, #[datatable] rows: Rows<LoadRow>) {
    assignment.loads = rows
        .into_iter()
        .map(|row| SubscriberLoad {
            assigned: if row.assigned == "maximum" {
                usize::MAX
            } else {
                row.assigned.parse().expect("subscriber load is an integer")
            },
            assigned_at_topic_start: row.assigned_at_topic_start,
        })
        .collect();
}

#[when("the least loaded subscriber is selected")]
fn select_subscriber(assignment: &mut AssignmentScenario) {
    assignment
        .selected
        .set(select_least_loaded(&assignment.loads));
}

#[then("no subscriber is selected")]
fn no_subscriber(assignment: &AssignmentScenario) {
    assert!(assignment.selected.get() == Some(None));
}

#[then("subscriber {index} is selected")]
fn selected_subscriber(assignment: &AssignmentScenario, index: usize) {
    assert!(assignment.selected.get() == Some(Some(index)));
}
scenarios!("tests/features/uniform_quotas", fixtures = [assignment: AssignmentScenario]);
