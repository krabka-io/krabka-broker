use assert2::assert;
use uuid::Uuid;

use super::*;

const THIS_BROKER: NodeId = NodeId(1);
const OTHER_BROKER: NodeId = NodeId(2);
const P0: PartitionIndex = PartitionIndex(0);

/// An image with no `__transaction_state` topic, as the image before the
/// topic was created is.
fn image_without_topic() -> MetadataImage {
    MetadataImage::new(Uuid::nil())
}

fn image(topic_id: u128, leader: NodeId, leader_epoch: i32) -> MetadataImage {
    crate::txn::coordinator::test_support::state_image_with_id(
        topic_id,
        leader,
        leader_epoch,
        &[leader],
    )
}

fn leadership(
    topic_id: u128,
    leader_epoch: i32,
    term: Option<(u64, LoadStatus)>,
) -> StatePartitionLeadership {
    StatePartitionLeadership {
        topic_id: Uuid::from_u128(topic_id),
        leader_epoch: LeaderEpoch(leader_epoch),
        term: term.map(|(generation, status)| LeaderTerm { generation, status }),
    }
}

/// One step: the image applied, whether the log is local, the changes it asks
/// for, and the leadership of partition 0 afterwards.
struct Step {
    name: &'static str,
    image: MetadataImage,
    local: bool,
    changes: LeadershipChanges,
    after: Option<StatePartitionLeadership>,
    /// A load that ends before the next step, with its result.
    load_ends: Option<LoadStatus>,
}

fn step(
    name: &'static str,
    image: MetadataImage,
    local: bool,
    changes: LeadershipChanges,
    after: Option<StatePartitionLeadership>,
    load_ends: Option<LoadStatus>,
) -> Step {
    Step {
        name,
        image,
        local,
        changes,
        after,
        load_ends,
    }
}

fn changes(unload: &[i32], load: &[(i32, u64)]) -> LeadershipChanges {
    LeadershipChanges {
        unload: unload.iter().copied().map(PartitionIndex).collect(),
        load: load
            .iter()
            .map(|&(partition, generation)| (PartitionIndex(partition), generation))
            .collect(),
    }
}

fn run(steps: Vec<Step>) {
    let mut known = StatePartitionLeaders::new();
    let mut generation = 0;
    for step in steps {
        let applied = apply_image(
            &mut known,
            THIS_BROKER,
            &step.image,
            |_| step.local,
            || {
                generation += 1;
                generation
            },
        );
        assert!(applied == step.changes, "{}: changes", step.name);
        assert!(
            known.get(&P0).copied() == step.after,
            "{}: leadership",
            step.name
        );
        if let Some(result) = step.load_ends
            && let Some(term) = known.get_mut(&P0).and_then(|entry| entry.term.as_mut())
        {
            term.status = result;
        }
    }
}

#[test]
fn an_election_loads_and_a_resignation_unloads() {
    run(vec![
        step(
            "an image before the topic exists",
            image_without_topic(),
            true,
            changes(&[], &[]),
            None,
            None,
        ),
        step(
            "this broker is elected at epoch 0",
            image(1, THIS_BROKER, 0),
            true,
            changes(&[], &[(0, 1)]),
            Some(leadership(1, 0, Some((1, LoadStatus::Loading)))),
            Some(LoadStatus::Loaded),
        ),
        step(
            "the same image again changes nothing",
            image(1, THIS_BROKER, 0),
            true,
            changes(&[], &[]),
            Some(leadership(1, 0, Some((1, LoadStatus::Loaded)))),
            None,
        ),
        step(
            "a stale image from before the topic existed (#975)",
            image_without_topic(),
            true,
            changes(&[], &[]),
            Some(leadership(1, 0, Some((1, LoadStatus::Loaded)))),
            None,
        ),
        step(
            "another broker is elected at epoch 1",
            image(1, OTHER_BROKER, 1),
            true,
            changes(&[0], &[]),
            Some(leadership(1, 1, None)),
            None,
        ),
        step(
            "a stale image of epoch 0",
            image(1, THIS_BROKER, 0),
            true,
            changes(&[], &[]),
            Some(leadership(1, 1, None)),
            None,
        ),
        step(
            "this broker is elected again at epoch 2",
            image(1, THIS_BROKER, 2),
            true,
            changes(&[], &[(0, 2)]),
            Some(leadership(1, 2, Some((2, LoadStatus::Loading)))),
            None,
        ),
        step(
            "a new term during a load drops the load and starts another",
            image(1, THIS_BROKER, 3),
            true,
            changes(&[0], &[(0, 3)]),
            Some(leadership(1, 3, Some((3, LoadStatus::Loading)))),
            Some(LoadStatus::Failed),
        ),
        step(
            "a failed load waits for the next election",
            image(1, THIS_BROKER, 3),
            true,
            changes(&[], &[]),
            Some(leadership(1, 3, Some((3, LoadStatus::Failed)))),
            None,
        ),
        step(
            "the next election loads again",
            image(1, THIS_BROKER, 4),
            true,
            changes(&[0], &[(0, 4)]),
            Some(leadership(1, 4, Some((4, LoadStatus::Loading)))),
            Some(LoadStatus::Loaded),
        ),
        step(
            "a stale image from before the topic existed, while the log is open",
            image_without_topic(),
            true,
            changes(&[], &[]),
            Some(leadership(1, 4, Some((4, LoadStatus::Loaded)))),
            None,
        ),
        step(
            "the topic is deleted and its log is removed",
            image_without_topic(),
            false,
            changes(&[0], &[]),
            None,
            None,
        ),
        step(
            "the topic is created again and another broker leads it at epoch 0",
            image(2, OTHER_BROKER, 0),
            true,
            changes(&[], &[]),
            Some(leadership(2, 0, None)),
            None,
        ),
        step(
            "the topic is created a third time and this broker leads it at epoch 0",
            image(3, THIS_BROKER, 0),
            true,
            changes(&[], &[(0, 5)]),
            Some(leadership(3, 0, Some((5, LoadStatus::Loading)))),
            None,
        ),
    ]);
}

#[test]
fn an_election_before_the_log_opens_loads_once_it_opens() {
    run(vec![
        step(
            "elected, the log is not open",
            image(1, THIS_BROKER, 0),
            false,
            changes(&[], &[]),
            Some(leadership(1, 0, Some((1, LoadStatus::Pending)))),
            None,
        ),
        step(
            "still not open",
            image(1, THIS_BROKER, 0),
            false,
            changes(&[], &[]),
            Some(leadership(1, 0, Some((1, LoadStatus::Pending)))),
            None,
        ),
        step(
            "the log opened",
            image(1, THIS_BROKER, 0),
            true,
            changes(&[], &[(0, 2)]),
            Some(leadership(1, 0, Some((2, LoadStatus::Loading)))),
            None,
        ),
    ]);
}

#[test]
fn the_load_status_gives_the_kafka_error_code() {
    let cases = [
        (None, Some(crate::codes::NOT_COORDINATOR)),
        (
            Some(LoadStatus::Pending),
            Some(crate::codes::COORDINATOR_LOAD_IN_PROGRESS),
        ),
        (
            Some(LoadStatus::Loading),
            Some(crate::codes::COORDINATOR_LOAD_IN_PROGRESS),
        ),
        (Some(LoadStatus::Loaded), None),
        (
            Some(LoadStatus::Failed),
            Some(crate::codes::NOT_COORDINATOR),
        ),
    ];
    for (status, expected) in cases {
        assert!(coordinator_error(status) == expected, "{status:?}");
    }
}
