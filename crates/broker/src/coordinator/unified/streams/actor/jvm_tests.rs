//! Real released-JAR assignment through the production actor target installer.
use {
    super::*,
    crate::coordinator::unified::streams::{
        jvm_assignor::{JvmAssignorOptions, JvmTaskAssignor},
        persistence::StoredSubtopology,
        state::StreamsMemberState,
    },
    assert2::assert,
    std::{env, path::PathBuf, time::Duration},
    tokio::time::{sleep, timeout},
};

#[cfg(test)]
fn options(class_name: &str) -> JvmAssignorOptions {
    JvmAssignorOptions {
        java: PathBuf::from("java"),
        classpath: env::var("KAFKA44_JVM_CLASSPATH")
            .expect("set KAFKA44_JVM_CLASSPATH to worker classes, Kafka libs/* and plugin JAR"),
        class_name: class_name.into(),
        timeout: Duration::from_secs(5),
    }
}

#[tokio::test]
#[ignore = "requires compiled AssignorWorker and released Kafka 4.4 JARs; see tools/jvm-assignor/README.md"]
async fn jvm_assignor_released_jar_installs_target_and_fences_changed_membership() {
    let mut actor = ActorState::new("java-group".into());
    for id in ["a", "b"] {
        let mut member = StreamsMemberState::joining(id, "client", "host");
        member.process_id = id.into();
        actor.state.add_or_update_member(member);
    }
    let topology = StreamsGroupTopologyValue {
        epoch: 1,
        subtopologies: vec![StoredSubtopology {
            subtopology_id: "s".into(),
            ..Default::default()
        }],
    };
    let tasks = BTreeMap::from([("s".into(), 2)]);
    let mut config = StreamsGroupConfig::default();
    config
        .task_assignors
        .register(Arc::new(
            JvmTaskAssignor::new(
                "java-sticky".into(),
                options("org.apache.kafka.coordinator.group.streams.assignor.StickyTaskAssignor"),
            )
            .unwrap(),
        ))
        .unwrap();
    config.task_assignor_name = Some("java-sticky".into());
    let old_target = actor.state.target.clone();
    assert!(compute_and_install_target(&mut actor, &config, &topology, &tasks).is_err());
    assert!(actor.state.target.active == old_target.active);
    // Let the obsolete input's callback finish, then change membership before
    // consuming it. The old completion must not be installed for this input.
    sleep(Duration::from_millis(750)).await;
    actor.state.members.remove("a");
    assert!(compute_and_install_target(&mut actor, &config, &topology, &tasks).is_err());
    assert!(actor.state.target.active == old_target.active);
    timeout(Duration::from_secs(10), async {
        loop {
            if compute_and_install_target(&mut actor, &config, &topology, &tasks).is_ok() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Java assignment completion");
    assert!(actor.state.target.active.len() == 1);
    assert!(actor.state.target.active["b"]["s"] == vec![0, 1]);
    // A failed class load must preserve the prior installed target and group
    // epoch through the production rollback path on every retry.
    let target = actor.state.target.active.clone();
    let epoch = actor.state.group_epoch;
    config
        .task_assignors
        .register(Arc::new(
            JvmTaskAssignor::new("java-failure".into(), options("no.such.Plugin")).unwrap(),
        ))
        .unwrap();
    config.task_assignor_name = Some("java-failure".into());
    assert!(compute_and_install_target(&mut actor, &config, &topology, &tasks).is_err());
    sleep(Duration::from_millis(750)).await;
    assert!(compute_and_install_target(&mut actor, &config, &topology, &tasks).is_err());
    assert!(actor.state.target.active == target);
    assert!(actor.state.group_epoch == epoch);
}
