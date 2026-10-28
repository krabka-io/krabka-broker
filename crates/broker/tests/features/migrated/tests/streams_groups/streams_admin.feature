Feature: Streams administration reports membership and filters group types

  Scenario: describe returns the group
    Given a streams-enabled broker holds topic desc-input with 2 partitions for group streams-app-3
    When a stateless streams member joins and converges on two active tasks
    Then the streams join succeeds with a nonempty member identity and positive epoch
    When an administrator describes the streams group
    Then the streams description identifies the live group, its member, and a nonempty state

  Scenario: list groups surfaces streams group
    Given a streams-enabled broker holds topic list-input with 2 partitions for group streams-app-5
    When a stateless streams member joins and converges on two active tasks
    Then the streams join succeeds with a nonempty member identity and positive epoch
    When the streams administrator lists groups filtered by streams
    Then the filtered list contains the group tagged as streams
    When the streams administrator lists groups filtered by consumer
    Then the filtered list excludes the streams group
