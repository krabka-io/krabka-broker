Feature: Reopened logs report Kafka's logical start after local retention
  Scenario Outline: Remote retention does not erase the archive's logical start
    Given three log segments with remote storage <enabled>
    And the logical log start checkpoint is <checkpoint>
    When local retention deletes through offset 6 and the log is reopened
    Then two local segments are removed and start <local> is reported as <reported>

    Examples:
      | enabled | checkpoint | local | reported |
      | true    | untouched  | 6     | 0        |
      | true    | 4          | 4     | 4        |
      | false   | untouched  | 6     | 6        |
