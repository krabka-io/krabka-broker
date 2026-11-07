Feature: Scenarios run on a multi-thread Tokio runtime

  Scenario: A task spawned in a Given keeps running until the Then
    Given a background task has been spawned that waits for a release signal
    When the test thread waits for the task to report that it runs
    Then the background task is still running
