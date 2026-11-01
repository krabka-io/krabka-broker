//! Shared native-extension implementations and registration fixtures.
use {
    super::{super::config::StreamsGroupConfig, *},
    std::sync::Arc,
};
#[derive(Debug)]
pub(crate) struct Pin(pub(crate) &'static str, pub(crate) &'static str);
impl TaskAssignor for Pin {
    fn name(&self) -> &str { self.0 }

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
pub(crate) struct Fail;
impl TaskAssignor for Fail {
    fn name(&self) -> &str { "fail" }

    fn assign(&self, _: &[AssignorMember], _: &AssignorInput) -> Result<StreamsAssignment, String> {
        Err("extension failed".into())
    }
}
pub(crate) fn registered_config() -> StreamsGroupConfig {
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
