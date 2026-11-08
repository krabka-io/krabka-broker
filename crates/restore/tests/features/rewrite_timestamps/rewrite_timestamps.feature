Feature: Archived timestamp semantics during a restore rewrite
  Scenario Outline: A broker append timestamp may precede the producer clock
    Given an archived batch with producer timestamp 2000 and maximum timestamp 1000
    And the archived batch uses <timestamp_type> timestamps
    When the archived batch is prepared for an owned rewrite
    Then the archived batch rewrite is <decision>

    Examples:
      | case                         | timestamp_type | decision |
      | broker-assigned timestamps   | LogAppendTime  | accepted |
      | producer-assigned timestamps | CreateTime     | refused  |
