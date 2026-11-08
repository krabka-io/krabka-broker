//! End-to-end actor handle, offsets persistence and released Java ABI.
use {
    super::*,
    crate::coordinator::unified::streams::jvm_assignor::{JvmAssignorOptions, JvmTaskAssignor},
    std::assert,
};

#[tokio::test]
#[ignore = "requires KAFKA44_JVM_CLASSPATH; see tools/jvm-assignor/README.md"]
async fn jvm_assignor_released_jar_runs_through_heartbeat_and_persistence() {
    let mut config = undelayed();
    config
        .task_assignors
        .register(Arc::new(
            JvmTaskAssignor::new("java-sticky".into(), JvmAssignorOptions {
                java: "java".into(),
                classpath: std::env::var("KAFKA44_JVM_CLASSPATH")
                    .expect("worker + Kafka libs/* classpath"),
                class_name:
                    "org.apache.kafka.coordinator.group.streams.assignor.StickyTaskAssignor".into(),
                timeout: Duration::from_secs(5),
            })
            .unwrap(),
        ))
        .unwrap();
    config.task_assignor_name = Some("java-sticky".into());
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coordinator = coordinator_with_log(config, Arc::clone(&log));
    coordinator.set_metadata_source(input_topic_source(2));
    let handle = coordinator.get_or_create_streams("java-group");
    let request = || {
        streams_join_request()
            .topology(one_subtopology().call())
            .member_id("java-member")
            .call()
    };
    let first = heartbeat_result_at(&handle, request(), 1).await.response;
    assert!(first.error_code == codes::UNKNOWN_SERVER_ERROR);
    assert!(log.batches().await.is_empty());
    let response = timeout(Duration::from_secs(10), async {
        loop {
            // A rejected first join retires its empty actor. Wire dispatch
            // reacquires a handle on each request, so model that path here.
            let current = coordinator.get_or_create_streams("java-group");
            let response = heartbeat_result_at(&current, request(), 1).await.response;
            if response.error_code == codes::NONE {
                break response;
            }
            assert!(response.error_code == codes::UNKNOWN_SERVER_ERROR);
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Java worker result consumed on retry");
    assert!(response.member_id == "java-member");
    assert!(!log.batches().await.is_empty());
    let seed = coordinator
        .cached_streams_seed("java-group")
        .expect("committed Java assignment");
    assert!(seed.members.contains_key("java-member"));
    assert!(
        seed.target_per_member["java-member"]
            .active
            .values()
            .map(Vec::len)
            .sum::<usize>()
            == 2
    );
    coordinator.shutdown_all().await;
}
