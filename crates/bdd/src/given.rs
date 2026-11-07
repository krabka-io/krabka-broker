//! A scenario-state slot that an earlier step fills and later steps read.

use std::fmt;

/// One piece of scenario state that an earlier step fills and later steps read.
///
/// It replaces the `Option<T>` field plus `.expect("x requested")` that step
/// fixtures otherwise repeat. A read of an unset slot panics with a message
/// that names the field and the step that fills it, keyword included, so a
/// scenario that leaves out its Given step fails with a pointer to the missing
/// step. Most slots are filled by a Given; a slot that holds what the When did
/// names its When step the same way.
///
/// # Examples
///
/// ```
/// use {assert2::assert, krabka_bdd::Given};
///
/// let mut policy = Given::new("policy", "Given a log has cleanup policy <policy>");
/// assert!(!policy.is_set());
/// policy.set("delete");
/// assert!(*policy.get() == "delete");
/// ```
pub struct Given<T> {
    value: Option<T>,
    field: &'static str,
    step: &'static str,
}

impl<T> Given<T> {
    /// Creates an empty slot.
    ///
    /// `field` names the state in panic messages. `step` is the step that
    /// fills it, with its keyword, such as
    /// `Given a log has cleanup policy <policy>`.
    #[must_use]
    pub const fn new(field: &'static str, step: &'static str) -> Self {
        Self {
            value: None,
            field,
            step,
        }
    }

    /// Stores `value`, replacing any earlier value.
    pub fn set(&mut self, value: T) { self.value = Some(value); }

    /// Returns the stored value.
    ///
    /// # Panics
    ///
    /// Panics if no step has set the slot.
    #[must_use]
    #[track_caller]
    pub fn get(&self) -> &T {
        let Some(value) = &self.value else {
            unset_panic(self.field, self.step);
        };
        value
    }

    /// Returns the stored value mutably.
    ///
    /// # Panics
    ///
    /// Panics if no step has set the slot.
    #[must_use]
    #[track_caller]
    pub fn get_mut(&mut self) -> &mut T {
        let Some(value) = &mut self.value else {
            unset_panic(self.field, self.step);
        };
        value
    }

    /// Reports whether a step has set the slot.
    #[must_use]
    pub const fn is_set(&self) -> bool { self.value.is_some() }

    /// Consumes the slot and returns the stored value.
    ///
    /// # Panics
    ///
    /// Panics if no step has set the slot.
    #[must_use]
    #[track_caller]
    pub fn into_inner(self) -> T {
        let Some(value) = self.value else {
            unset_panic(self.field, self.step);
        };
        value
    }

    /// Moves the stored value out and leaves the slot unset, for a step that
    /// hands the value on, such as joining a task or sending on a channel.
    ///
    /// # Panics
    ///
    /// Panics if no step has set the slot.
    #[must_use]
    #[track_caller]
    pub fn take(&mut self) -> T {
        let Some(value) = self.value.take() else {
            unset_panic(self.field, self.step);
        };
        value
    }
}

#[track_caller]
fn unset_panic(field: &str, step: &str) -> ! {
    panic!(
        "scenario state `{field}` is not set: the scenario needs the step `{step}` before \
         this one"
    )
}

impl<T: fmt::Debug> fmt::Debug for Given<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Given")
            .field("field", &self.field)
            .field("step", &self.step)
            .field("value", &self.value)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        rstest::rstest,
        std::panic::{UnwindSafe, catch_unwind},
    };

    const EXPECTED_PANIC: &str = "scenario state `policy` is not set: the scenario needs the \
                                  step `Given a log has cleanup policy <policy>` before this \
                                  one";

    fn slot() -> Given<String> { Given::new("policy", "Given a log has cleanup policy <policy>") }

    fn panic_message(run: impl FnOnce() + UnwindSafe) -> String {
        let payload = catch_unwind(run).expect_err("the read should panic");
        match payload.downcast::<String>() {
            Ok(message) => *message,
            Err(payload) => (*payload.downcast::<&str>().expect("a string payload")).to_owned(),
        }
    }

    #[test]
    fn set_then_get_returns_the_value() {
        let mut policy = slot();
        policy.set("delete".to_owned());
        assert!(policy.get() == "delete");
    }

    #[test]
    fn set_replaces_an_earlier_value() {
        let mut policy = slot();
        policy.set("delete".to_owned());
        policy.set("compact".to_owned());
        assert!(policy.into_inner() == "compact");
    }

    #[test]
    fn take_moves_the_value_out_and_unsets_the_slot() {
        let mut policy = slot();
        policy.set("delete".to_owned());
        assert!(policy.take() == "delete");
        assert!(!policy.is_set());
    }

    #[test]
    fn get_mut_changes_the_stored_value() {
        let mut policy = slot();
        policy.set("delete".to_owned());
        policy.get_mut().push_str(",compact");
        assert!(policy.get() == "delete,compact");
    }

    #[test]
    fn is_set_reports_whether_a_value_is_stored() {
        let mut policy = slot();
        assert!(!policy.is_set());
        policy.set("delete".to_owned());
        assert!(policy.is_set());
    }

    #[rstest]
    #[case::get(|| { let _ = slot().get().len(); })]
    #[case::get_mut(|| { let _ = slot().get_mut().len(); })]
    #[case::into_inner(|| { let _ = slot().into_inner(); })]
    #[case::take(|| { let _ = slot().take(); })]
    fn reading_an_unset_slot_names_the_field_and_step(#[case] read: fn()) {
        assert!(panic_message(read) == EXPECTED_PANIC);
    }

    #[test]
    fn debug_shows_the_field_step_and_value() {
        let mut policy = slot();
        policy.set("delete".to_owned());
        assert!(
            format!("{policy:?}")
                == r#"Given { field: "policy", step: "Given a log has cleanup policy <policy>", value: Some("delete") }"#
        );
    }
}
