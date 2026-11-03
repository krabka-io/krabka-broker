//! Shared native-extension implementations and registration fixtures.
use {
    super::{super::config::StreamsGroupConfig, *},
    std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
#[derive(Debug)]
pub struct Pin(pub &'static str, pub &'static str);
impl TaskAssignor for Pin {
    fn name(&self) -> &'static str { self.0 }

    fn assign(
        &self,
        _: &[AssignorMember],
        input: &AssignorInput,
    ) -> Result<StreamsAssignment, String> {
        Ok(StreamsAssignment {
            active: [(self.1.into(), input.tasks.clone())].into(),
            ..Default::default()
        })
    }
}
#[derive(Debug)]
pub struct Fail;
impl TaskAssignor for Fail {
    fn name(&self) -> &'static str { "fail" }

    fn assign(&self, _: &[AssignorMember], _: &AssignorInput) -> Result<StreamsAssignment, String> {
        Err("extension failed".into())
    }
}
pub fn registered_config() -> StreamsGroupConfig {
    let mut config = StreamsGroupConfig::default();
    config
        .task_assignors
        .register(Arc::new(Pin("first", "a")))
        .unwrap();
    config
        .task_assignors
        .register(Arc::new(Pin("second", "b")))
        .unwrap();
    config
}

/// An actual assignor with a controllable failure, for rollback/recovery tests.
#[derive(Debug)]
pub struct Recoverable(pub Arc<AtomicBool>);
impl TaskAssignor for Recoverable {
    fn name(&self) -> &'static str { "recoverable" }

    fn assign(
        &self,
        members: &[AssignorMember],
        input: &AssignorInput,
    ) -> Result<StreamsAssignment, String> {
        if self.0.load(Ordering::SeqCst) {
            Err("injected assignor failure".into())
        } else {
            Ok(assign(members, input))
        }
    }
}
