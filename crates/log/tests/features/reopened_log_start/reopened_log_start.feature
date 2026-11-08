Feature: Reopened logs report Kafka's logical start after local retention
  Scenario Outline: Remote retention does not erase the archive's logical start
    Given three log segments with remote storage <enabled>
    And the logical log start checkpoint is <checkpoint>
    When local retention deletes through offset 6 and the log is reopened
    Then two local segments are removed and start <local> is reported as <reported>

    Examples:
      | case                                           | enabled | checkpoint | local | reported |
      | remote storage keeps the archive start         | true    | untouched  | 6     | 0        |
      | remote storage reports a checkpointed start    | true    | 4          | 4     | 4        |
      | local-only storage reports the local start     | false   | untouched  | 6     | 6        |
