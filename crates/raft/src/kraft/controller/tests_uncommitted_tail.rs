//! Tests for a restart over a log whose tail a leader appended and never
//! committed: the tail stays out of the image, the restart reports none of
//! its records as skipped, and the next epoch truncates it.

use {
    super::*,
    crate::kraft::{
        controller::{
            records::{batch_last_offset, encode_batches, metadata_record_batch},
            startup::Assembled,
            test_support::{build_engine_only, open_test_engine, topic_record_named, voter_set},
        },
        types::LogOffsetMetadata,
    },
    assert2::check,
    krabka_metadata::to_kraft_values,
    std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    tracing::{Event as TraceEvent, Level, Subscriber, subscriber},
    tracing_subscriber::{
        Layer,
        layer::{Context, SubscriberExt as _},
        registry,
    },
};

const VOTERS: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];

/// Counts the warnings the controller logs.
#[derive(Clone, Default)]
struct Warnings(Arc<AtomicUsize>);

impl<S: Subscriber> Layer<S> for Warnings {
    fn on_event(&self, event: &TraceEvent<'_>, _: Context<'_, S>) {
        if *event.metadata().level() == Level::WARN
            && event.metadata().target().starts_with("krabka_raft")
        {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// The engine a restart recovers from `dir`, and the number of warnings the
/// recovery logged.
fn restart(dir: &tempfile::TempDir) -> (Assembled, usize) {
    let warnings = Warnings::default();
    let subscriber = registry().with(warnings.clone());
    let assembled = subscriber::with_default(subscriber, || {
        open_test_engine(dir.path().to_path_buf(), voter_set(&VOTERS))
    })
    .expect("restart");
    (assembled, warnings.0.load(Ordering::Relaxed))
}

/// The batch a leader of `epoch` writes at `base` for `records`, encoded
/// against `image`, which then holds them.
fn leader_batch(
    base: Offset,
    epoch: Epoch,
    image: &mut MetadataImage,
    records: &[MetadataRecord],
) -> RecordBatch {
    let mut blobs = Vec::new();
    for record in records {
        blobs.extend(to_kraft_values(record, image).expect("encode a record"));
        image.apply(record);
    }
    let mut batch = metadata_record_batch(epoch, &blobs).expect("build a batch");
    batch.base_offset = base.0;
    batch
}

/// The answer the leader `leader` of `epoch` gives a Fetch.
fn answer(
    leader: NodeId,
    epoch: Epoch,
    hwm: Offset,
    batches: &[RecordBatch],
    diverging: Option<LogOffsetMetadata>,
) -> bytes::Bytes {
    wire::PeerResponse::Fetch(wire::FetchAnswer {
        error_code: 0,
        leader: wire::QuorumLeader {
            leader_id: Some(leader),
            epoch,
            endpoint: None,
        },
        diverging,
        snapshot_id: None,
        hwm: hwm.0,
        log_start_offset: 0,
        records: encode_batches(batches),
    })
    .encode()
}

fn follow(engine: &mut Engine, leader_id: NodeId, leader_epoch: Epoch) {
    engine.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id,
        leader_epoch,
    });
}

/// What the image of `engine` holds of the three topics.
fn topics(engine: &Engine) -> [bool; 3] {
    ["kept", "lost", "won"].map(|name| engine.image.topic(name).is_some())
}

/// The Jepsen shape: a leader of epoch 1 appends a topic and its partition,
/// the cluster crashes before they commit, and the node restarts with them
/// still in its log. The restart rebuilds the image only up to the high
/// watermark, and does not log the partition record that names the topic
/// created in the same tail as a skipped committed record. Epoch 2, whose
/// leader never had the tail, then truncates it and commits records of its
/// own at the same offsets. Neither the running image nor a second restart
/// ever holds the truncated topic.
#[tokio::test]
async fn a_restart_keeps_an_uncommitted_tail_out_of_the_image_until_the_next_epoch_truncates_it() {
    let (mut engine, dir) = build_engine_only().ids(&VOTERS).call();
    follow(&mut engine, NodeId(2), 1);
    let mut epoch_one = MetadataImage::new(uuid::Uuid::nil());
    let kept = leader_batch(Offset(0), 1, &mut epoch_one, &topic_record_named("kept", 1));
    let committed = Offset(batch_last_offset(&kept) + 1);
    let lost = leader_batch(committed, 1, &mut epoch_one, &topic_record_named("lost", 2));
    let tail_end = Offset(batch_last_offset(&lost) + 1);
    engine.on_fetch_response(NodeId(2), &answer(NodeId(2), 1, committed, &[kept], None));
    engine.on_fetch_response(NodeId(2), &answer(NodeId(2), 1, committed, &[lost], None));
    check!(
        (
            engine.log.hwm(),
            engine.log.log_end_offset(),
            topics(&engine)
        ) == (committed, tail_end, [true, false, false])
    );
    drop(engine);

    let (Assembled { mut engine, .. }, warnings) = restart(&dir);
    check!(
        (
            engine.log.hwm(),
            engine.log.log_end_offset(),
            topics(&engine),
            warnings,
        ) == (committed, tail_end, [true, false, false], 0)
    );

    follow(&mut engine, NodeId(3), 2);
    let diverging = LogOffsetMetadata {
        offset: committed.0,
        epoch: 1,
    };
    engine.on_fetch_response(
        NodeId(3),
        &answer(NodeId(3), 2, committed, &[], Some(diverging)),
    );
    check!((engine.log.hwm(), engine.log.log_end_offset()) == (committed, committed));
    let mut epoch_two = MetadataImage::new(uuid::Uuid::nil());
    for record in topic_record_named("kept", 1) {
        epoch_two.apply(&record);
    }
    let won = leader_batch(committed, 2, &mut epoch_two, &topic_record_named("won", 3));
    let won_end = Offset(batch_last_offset(&won) + 1);
    engine.on_fetch_response(NodeId(3), &answer(NodeId(3), 2, won_end, &[won], None));
    check!((engine.log.hwm(), topics(&engine)) == (won_end, [true, false, true]));
    drop(engine);

    let (Assembled { engine, .. }, warnings) = restart(&dir);
    check!((topics(&engine), warnings) == ([true, false, true], 0));
}
