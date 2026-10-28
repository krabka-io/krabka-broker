Feature: Recovered streams groups continue handing tasks to joining processes

  Scenario: a reloaded group assigns tasks to a member that joins after a restart
    Given a streams-enabled broker holds topic restart-input with 2 partitions for group streams-restart
    When a stateless streams member joins and converges on two active tasks
    Then the stateless streams member owns both partitions of subtopology zero
    When the streams broker restarts preserving the original member and assigned tasks
    And a new streams process p2 joins after recovery
    Then the recovered streams coordinator accepts the new process
    When both streams members report owned tasks until the assignment handover converges
    Then the recovered member owns task zero and the joining process owns task one
