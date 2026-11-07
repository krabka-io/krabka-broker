Feature: A sync step runs outside the scenario's runtime

  Scenario: A sync step blocks on a runtime that its fixture owns
    Given a fixture owns a Tokio runtime of its own
    When a sync step blocks on a task of that runtime
    Then the task has handed the step its result
