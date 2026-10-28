Feature: Stateless streams members receive tasks and can leave their retained group

  Scenario: stateless single member converges
    Given a streams-enabled broker holds topic streams-input with 2 partitions for group streams-app-1
    When a stateless streams member joins and converges on two active tasks
    Then the streams join succeeds with a nonempty member identity and positive epoch
    And the stateless streams member owns both partitions of subtopology zero

  Scenario: leave removes member
    Given a streams-enabled broker holds topic leave-input with 2 partitions for group streams-app-4
    When a stateless streams member joins and converges on two active tasks
    Then the streams join succeeds with a nonempty member identity and positive epoch
    When the joined streams member leaves at epoch minus one
    Then the streams leave heartbeat succeeds
    When an administrator describes the streams group
    Then the retained streams group no longer contains the departed member
