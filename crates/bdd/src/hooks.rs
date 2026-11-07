//! `#[krabka_bdd::before]` and `#[krabka_bdd::after]` scenario hooks.

use {
    crate::{runner::Outcome, spec::ScenarioSpec},
    krabka_bdd_model::model::{parse_tag_expression, tags_match},
    std::{future::Future, pin::Pin},
};

/// The function a hook registers: it gets the scenario and, after the
/// scenario, its outcome.
pub type HookFn =
    for<'a> fn(&'a ScenarioSpec, Option<&'a Outcome>) -> Pin<Box<dyn Future<Output = ()> + 'a>>;

/// When a hook runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookPhase {
    /// Before the scenario's first step, after the World's `before`.
    BeforeScenario,
    /// After the scenario, before the World's `after`; it always runs.
    AfterScenario,
}

/// A scenario hook, which the hook attributes submit with
/// `inventory::submit!`.
#[derive(Debug, Clone, Copy)]
pub struct Hook {
    /// When the hook runs.
    pub phase: HookPhase,
    /// A tag expression such as `@docker and not @slow`; `None` applies the
    /// hook to every scenario.
    pub tags: Option<&'static str>,
    /// Hooks run in ascending `order`, then by `file:line`.
    pub order: i32,
    /// The source file of the hook.
    pub file: &'static str,
    /// The line of the hook in `file`.
    pub line: u32,
    /// The hook itself.
    pub run: HookFn,
}

inventory::collect!(Hook);

/// Every hook registered in the test binary.
pub fn registered() -> Vec<&'static Hook> { inventory::iter::<Hook>.into_iter().collect() }

/// The hooks of `phase` that apply to a scenario with `tags`, in run order.
///
/// # Errors
///
/// Returns a message naming the hook when its tag expression does not parse.
pub fn select<'h>(
    hooks: &[&'h Hook],
    phase: HookPhase,
    tags: &[&str],
) -> Result<Vec<&'h Hook>, String> {
    let tags: Vec<String> = tags.iter().map(|tag| (*tag).to_owned()).collect();
    let mut selected = Vec::new();
    for hook in hooks.iter().copied().filter(|hook| hook.phase == phase) {
        let applies = match hook.tags {
            None => true,
            Some(expression) => {
                let operation = parse_tag_expression(expression)
                    .map_err(|error| format!("hook at {}:{}: {error}", hook.file, hook.line))?;
                tags_match(&operation, &tags)
            }
        };
        if applies {
            selected.push(hook);
        }
    }
    selected.sort_by(|left, right| {
        (left.order, left.file, left.line).cmp(&(right.order, right.file, right.line))
    });
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, rstest::rstest};

    fn noop<'a>(
        _: &'a ScenarioSpec,
        _: Option<&'a Outcome>,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async {})
    }

    const fn hook(
        phase: HookPhase,
        tags: Option<&'static str>,
        order: i32,
        file: &'static str,
        line: u32,
    ) -> Hook {
        Hook {
            phase,
            tags,
            order,
            file,
            line,
            run: noop,
        }
    }

    static HOOKS: [Hook; 6] = [
        hook(HookPhase::BeforeScenario, None, 0, "b.rs", 3),
        hook(HookPhase::BeforeScenario, Some("@docker"), 0, "a.rs", 9),
        hook(
            HookPhase::BeforeScenario,
            Some("@docker and not @slow"),
            -5,
            "z.rs",
            1,
        ),
        hook(HookPhase::BeforeScenario, None, 10, "a.rs", 1),
        hook(HookPhase::AfterScenario, None, 0, "a.rs", 2),
        hook(HookPhase::AfterScenario, Some("@slow"), 0, "a.rs", 1),
    ];

    fn locations(hooks: &[&Hook]) -> Vec<String> {
        hooks
            .iter()
            .map(|hook| format!("{}:{}", hook.file, hook.line))
            .collect()
    }

    #[rstest]
    #[case::untagged_scenario(HookPhase::BeforeScenario, &[], &["b.rs:3", "a.rs:1"])]
    #[case::tagged_scenario(HookPhase::BeforeScenario, &["docker"], &["z.rs:1", "a.rs:9", "b.rs:3", "a.rs:1"])]
    #[case::excluded_by_not(HookPhase::BeforeScenario, &["docker", "slow"], &["a.rs:9", "b.rs:3", "a.rs:1"])]
    #[case::after_phase(HookPhase::AfterScenario, &["slow"], &["a.rs:1", "a.rs:2"])]
    #[case::after_phase_untagged(HookPhase::AfterScenario, &[], &["a.rs:2"])]
    fn select_filters_by_phase_and_tags_in_order(
        #[case] phase: HookPhase,
        #[case] tags: &[&str],
        #[case] expected: &[&str],
    ) {
        let hooks: Vec<&Hook> = HOOKS.iter().collect();
        let selected = select(&hooks, phase, tags).unwrap();
        assert!(locations(&selected) == expected);
    }

    #[test]
    fn select_names_a_hook_whose_tag_expression_does_not_parse() {
        static BROKEN: Hook = hook(HookPhase::BeforeScenario, Some("@a and"), 0, "h.rs", 4);
        let error = select(&[&BROKEN], HookPhase::BeforeScenario, &[]).unwrap_err();
        assert!(error.starts_with("hook at h.rs:4: `@a and` is not a valid tag expression"));
    }
}
