//! Behaviour tests for [`assign`](super::assign).
//!
//! The rows come from Kafka's `StickyTaskAssignorTest`. Where Kafka accepts
//! several results because it breaks load ties in hash-map order, the row
//! expects the one that this port's tie-break on the process id and the member
//! id gives, and the comment says which of Kafka's results that is.

use {super::*, assert2::assert};

/// The partitions of one subtopology that a member holds in a role.
struct TaskPartitions {
    subtopology: &'static str,
    partitions: &'static [i32],
}

/// The offset sum that a member reported for one task.
struct ReportedOffset {
    subtopology: &'static str,
    partition: i32,
    offset_sum: i64,
}

/// A member built from the `TaskPartitions` of each role and its
/// `ReportedOffset`s.
struct M {
    id: &'static str,
    process: &'static str,
    active: Vec<TaskPartitions>,
    standby: Vec<TaskPartitions>,
    warmup: Vec<TaskPartitions>,
    offsets: Vec<ReportedOffset>,
}

const fn task(subtopology: &'static str, partitions: &'static [i32]) -> TaskPartitions {
    TaskPartitions {
        subtopology,
        partitions,
    }
}

fn m(id: &'static str, process: &'static str) -> M {
    M {
        id,
        process,
        active: Vec::new(),
        standby: Vec::new(),
        warmup: Vec::new(),
        offsets: Vec::new(),
    }
}

fn m_active(id: &'static str, process: &'static str, active: Vec<TaskPartitions>) -> M {
    M {
        active,
        ..m(id, process)
    }
}

fn role(tasks: &[TaskPartitions]) -> BTreeMap<String, Vec<i32>> {
    tasks
        .iter()
        .map(|entry| (entry.subtopology.to_owned(), entry.partitions.to_vec()))
        .collect()
}

fn member(spec: &M) -> AssignorMember {
    AssignorMember {
        member_id: spec.id.to_owned(),
        process_id: spec.process.to_owned(),
        current_active: role(&spec.active),
        current_standby: role(&spec.standby),
        current_warmup: role(&spec.warmup),
        task_offsets: spec
            .offsets
            .iter()
            .map(|reported| {
                (
                    (reported.subtopology.to_owned(), reported.partition),
                    reported.offset_sum,
                )
            })
            .collect(),
    }
}

#[test]
fn range_assigns_each_member_a_task_of_every_subtopology() {
    // Kafka's `shouldRangeAssignTasksWhenScalingUp`: member2 is new, and each
    // member ends with one active and one standby task of each subtopology.
    let members = [
        member(&m_active("member1", "process1", vec![
            task("s1", &[0, 1]),
            task("s2", &[0, 1]),
        ])),
        member(&m("member2", "process2")),
    ];
    let input = AssignorInput {
        tasks: maplit::btreemap! {
            "s1".to_owned() => vec![0, 1],
            "s2".to_owned() => vec![0, 1],
        },
        stateful: maplit::btreeset! {"s1".to_owned(), "s2".to_owned()},
        num_standby_replicas: 1,
    };
    assert!(
        assign(&members, &input)
            == StreamsAssignment {
                active: maplit::hashmap! {
                    "member1".to_owned() => role(&[TaskPartitions { subtopology: "s1", partitions: &[0] }, TaskPartitions { subtopology: "s2", partitions: &[0] }]),
                    "member2".to_owned() => role(&[TaskPartitions { subtopology: "s1", partitions: &[1] }, TaskPartitions { subtopology: "s2", partitions: &[1] }]),
                },
                standby: maplit::hashmap! {
                    "member1".to_owned() => role(&[TaskPartitions { subtopology: "s1", partitions: &[1] }, TaskPartitions { subtopology: "s2", partitions: &[1] }]),
                    "member2".to_owned() => role(&[TaskPartitions { subtopology: "s1", partitions: &[0] }, TaskPartitions { subtopology: "s2", partitions: &[0] }]),
                },
            }
    );
}

use {
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
    serde::Deserialize,
};
#[derive(Default)]
struct TaskPlacement {
    members: Vec<AssignorMember>,
    input: Option<AssignorInput>,
    forward: Option<StreamsAssignment>,
    reversed: Option<StreamsAssignment>,
}
#[rstest::fixture]
fn task_placement() -> TaskPlacement {
    // Start this scenario with fresh inputs and observations.
    TaskPlacement::default()
}

#[when("the Streams assignor places active and standby tasks in both member orders")]
fn assign_streams_tasks(task_placement: &mut TaskPlacement) {
    let input = task_placement
        .input
        .as_ref()
        .expect("task topology prepared");
    task_placement.forward = Some(assign(&task_placement.members, input));
    let reversed = task_placement
        .members
        .iter()
        .rev()
        .cloned()
        .collect::<Vec<_>>();
    task_placement.reversed = Some(assign(&reversed, input));
}

#[derive(DataTableRow)]
struct TaskMemberInput {
    member: String,
    process: String,
    active: String,
    standby: String,
    warmup: String,
    offsets: String,
}
#[derive(Deserialize)]
struct TaskOffsetInput {
    subtopology: String,
    partition: i32,
    offset: i64,
}
#[derive(DataTableRow)]
struct ExpectedTaskOwner {
    member: String,
    active: String,
    standby: String,
}
#[given(
    "subtopology s has {partitions} tasks stateful {stateful} and requests {standby} standby replicas"
)]
fn task_topology(
    task_placement: &mut TaskPlacement,
    partitions: i32,
    stateful: bool,
    standby: i32,
) {
    task_placement.input = Some(AssignorInput {
        tasks: maplit::btreemap! {"s".to_owned()=>(0..partitions).collect()},
        stateful: if stateful {
            maplit::btreeset! {"s".to_owned()}
        } else {
            BTreeSet::new()
        },
        num_standby_replicas: standby,
    });
}
#[given("Streams members have these processes owned tasks and reported task offsets")]
fn task_member_inputs(
    task_placement: &mut TaskPlacement,
    #[datatable] rows: Rows<TaskMemberInput>,
) {
    task_placement.members = rows
        .into_iter()
        .map(|row| AssignorMember {
            member_id: row.member,
            process_id: row.process,
            current_active: serde_json::from_str(&row.active).expect("active task map"),
            current_standby: serde_json::from_str(&row.standby).expect("standby task map"),
            current_warmup: serde_json::from_str(&row.warmup).expect("warmup task map"),
            task_offsets: serde_json::from_str::<Vec<TaskOffsetInput>>(&row.offsets)
                .expect("reported task offsets")
                .into_iter()
                .map(|reported| ((reported.subtopology, reported.partition), reported.offset))
                .collect(),
        })
        .collect();
}
#[then("both member orders produce these complete active and standby task owners")]
fn expected_task_owners(
    task_placement: &TaskPlacement,
    #[datatable] rows: Rows<ExpectedTaskOwner>,
) {
    let mut expected = StreamsAssignment::default();
    for row in rows {
        if let Some(active) =
            serde_json::from_str::<Option<BTreeMap<String, Vec<i32>>>>(&row.active)
                .expect("active ownership or null")
        {
            expected.active.insert(row.member.clone(), active);
        }
        if let Some(standby) =
            serde_json::from_str::<Option<BTreeMap<String, Vec<i32>>>>(&row.standby)
                .expect("standby ownership or null")
        {
            expected.standby.insert(row.member, standby);
        }
    }
    assert!(task_placement.forward.as_ref() == Some(&expected));
    assert!(task_placement.reversed.as_ref() == Some(&expected));
}
scenarios!("tests/features/migrated/src/coordinator/unified/streams/assignor/tests.feature",fixtures=[task_placement:TaskPlacement]);
