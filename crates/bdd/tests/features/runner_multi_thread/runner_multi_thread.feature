Feature: A runner scenario on a multi-thread runtime

  Scenario: A task spawned in a Given is alive in the When
    Given the runner has spawned a task that waits for a release signal
    When the runner waits for the task to report that it runs
    Then the runner task is still running until it is released
