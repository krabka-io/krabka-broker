//! Caller-owned equivalent of tools/kafka44/AssignorPlugin.java.
use {
    krabka_broker::coordinator::unified::streams::{
        assignor::{AssignorInput, AssignorMember, StreamsAssignment, TaskAssignor},
        config::StreamsGroupConfig,
    },
    std::sync::Arc,
};

pub fn register(config: &mut StreamsGroupConfig) -> Result<(), String> {
    for (name, reverse, failure) in [
        ("first", false, false),
        ("last", true, false),
        ("failure", false, true),
        ("unknown-member", false, false),
    ] {
        config.task_assignors.register(Arc::new(FixtureAssignor {
            name,
            reverse,
            failure,
        }))?;
    }
    Ok(())
}
#[derive(Debug)]
struct FixtureAssignor {
    name: &'static str,
    reverse: bool,
    failure: bool,
}
impl TaskAssignor for FixtureAssignor {
    fn name(&self) -> &'static str { self.name }

    fn assign(
        &self,
        members: &[AssignorMember],
        input: &AssignorInput,
    ) -> Result<StreamsAssignment, String> {
        if self.failure {
            return Err("injected assignor failure".into());
        }
        if self.name == "unknown-member" {
            eprintln!("ASSIGNOR_CALLBACK unknown-member injected-unknown-member");
            return Ok(StreamsAssignment {
                active: [("injected-unknown-member".into(), input.tasks.clone())].into(),
                ..Default::default()
            });
        }
        let mut ids: Vec<_> = members
            .iter()
            .map(|member| member.member_id.clone())
            .collect();
        ids.sort();
        let owner = if self.reverse {
            ids.last()
        } else {
            ids.first()
        }
        .ok_or("no members")?;
        eprintln!(
            "ASSIGNOR_CALLBACK {} {} {:?}",
            self.name, owner, input.tasks
        );
        Ok(StreamsAssignment {
            active: [(owner.clone(), input.tasks.clone())].into(),
            ..Default::default()
        })
    }
}
