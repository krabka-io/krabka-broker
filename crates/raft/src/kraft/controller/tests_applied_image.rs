//! Tests for [`AppliedImage`]: every published snapshot holds exactly the
//! records applied before it, a snapshot a reader holds never changes, and a
//! publish or an overlay copies the image only when it has to.

use {
    super::{
        applied_image::AppliedImage,
        test_support::{topic_record, topic_record_named},
    },
    assert2::{assert, check},
    krabka_metadata::{DeleteTopicRecord, MetadataImage, MetadataRecord},
    std::sync::Arc,
};

/// The image that applying `records` to an empty image yields, applied
/// directly, without [`AppliedImage`].
fn applied_directly(records: &[MetadataRecord]) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    for record in records {
        image.apply(record);
    }
    image
}

fn delete_topic(name: &str) -> MetadataRecord {
    MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
        name: name.to_owned(),
    })
}

/// One step of a publish sequence: the records to apply, then a publish.
struct Step {
    records: Vec<MetadataRecord>,
    /// Whether a reader holds the published snapshot across the step.
    held: bool,
    /// The deep copies the step's publish makes.
    copies: u64,
}

#[test]
fn every_publish_holds_exactly_the_records_applied_before_it() {
    let steps = [
        Step {
            records: topic_record_named("a", 1),
            held: false,
            copies: 0,
        },
        Step {
            records: topic_record_named("b", 2),
            held: false,
            copies: 0,
        },
        // A reader holds the snapshot from before this step, so the publish
        // cannot reuse it and copies the new one.
        Step {
            records: topic_record_named("c", 3),
            held: true,
            copies: 1,
        },
        Step {
            records: vec![delete_topic("a")],
            held: false,
            copies: 0,
        },
        // A publish with nothing applied since the last one.
        Step {
            records: Vec::new(),
            held: false,
            copies: 0,
        },
        Step {
            records: [topic_record_named("a", 4), vec![delete_topic("b")]].concat(),
            held: false,
            copies: 0,
        },
    ];
    let (mut applied, _readers) = AppliedImage::new(MetadataImage::new(uuid::Uuid::nil()));
    let mut history = Vec::new();
    for (index, step) in steps.iter().enumerate() {
        let held = step.held.then(|| {
            let snapshot = Arc::clone(&applied.published());
            (snapshot.as_ref().clone(), snapshot)
        });
        let copies_before = applied.copies();
        for record in &step.records {
            applied.apply(record);
        }
        applied.publish();
        history.extend(step.records.iter().cloned());

        let expected = applied_directly(&history);
        check!(**applied.published() == expected, "step {index}: published");
        check!(*applied == expected, "step {index}: applied");
        check!(
            applied.copies() - copies_before == step.copies,
            "step {index}: copies"
        );
        if let Some((contents, snapshot)) = held {
            check!(
                *snapshot == contents,
                "step {index}: a held snapshot changed"
            );
        }
    }
}

#[test]
fn a_replaced_image_publishes_by_copy_and_then_recycles_again() {
    let (mut applied, _readers) = AppliedImage::new(MetadataImage::new(uuid::Uuid::nil()));
    applied.apply(&topic_record("a")[0]);
    applied.publish();

    let replacement = applied_directly(&topic_record_named("b", 2));
    applied.replace(replacement.clone());
    applied.publish();
    check!(applied.copies() == 1);
    check!(**applied.published() == replacement);

    let next = topic_record_named("c", 3);
    for record in &next {
        applied.apply(record);
    }
    applied.publish();
    check!(applied.copies() == 1);
    check!(**applied.published() == applied_directly(&[topic_record_named("b", 2), next].concat()));
}

#[test]
fn a_republish_changes_identity_and_keeps_contents() {
    let (mut applied, _readers) = AppliedImage::new(MetadataImage::new(uuid::Uuid::nil()));
    let first = topic_record_named("a", 1);
    for record in &first {
        applied.apply(record);
    }
    applied.publish();
    let before = Arc::clone(&applied.published());

    // Applied but not yet published: a republish must not show it.
    let second = topic_record_named("b", 2);
    for record in &second {
        applied.apply(record);
    }
    applied.republish();
    let republished = Arc::clone(&applied.published());
    check!(!Arc::ptr_eq(&before, &republished));
    check!(*republished == *before);
    drop((before, republished));

    applied.publish();
    check!(**applied.published() == applied_directly(&[first, second].concat()));
    // The republish copied; the publish after it reused the republished
    // snapshot, whose contents the replay log still describes.
    check!(applied.copies() == 1);
}

#[test]
fn a_pass_longer_than_the_replay_log_publishes_by_copy() {
    let (mut applied, _readers) = AppliedImage::new(MetadataImage::new(uuid::Uuid::nil()));
    let records: Vec<MetadataRecord> = (0..=16_384_u128)
        .map(|id| topic_record_named(&format!("t{id}"), id + 1).swap_remove(0))
        .collect();
    for record in &records {
        applied.apply(record);
    }
    applied.publish();
    check!(applied.copies() == 1);
    check!(**applied.published() == applied_directly(&records));
}

#[test]
fn an_overlay_copies_at_its_first_write_only() {
    let (mut applied, _readers) = AppliedImage::new(MetadataImage::new(uuid::Uuid::nil()));
    for record in topic_record("a") {
        applied.apply(&record);
    }
    let base = applied.clone();

    let unwritten = applied.overlay();
    check!(unwritten.topic("a").is_some());
    drop(unwritten);
    check!(applied.copies() == 0);

    let mut written = applied.overlay();
    for record in topic_record_named("b", 2) {
        written.apply(&record);
    }
    written.apply(&delete_topic("a"));
    check!(written.topic("a").is_none());
    check!(written.topic("b").is_some());
    drop(written);
    assert!(applied.copies() == 1);
    check!(*applied == base);
}
