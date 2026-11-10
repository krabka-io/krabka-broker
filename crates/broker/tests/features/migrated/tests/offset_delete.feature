Feature: Deleting committed consumer offsets safely
  Scenario: Deleting one inactive-group partition preserves the others
    Given group g1 has offsets on topic t1 with 2 partitions
      | partition | offset |
      | 0         | 42     |
      | 1         | 100    |
    When offsets are deleted from topic t1 for partitions 0
    Then offset deletion returns top-level 0 and topic t1 partition results
      | partition | error_code |
      | 0         | 0          |
    And the remaining committed offsets are exactly
      | partition | offset |
      | 0         | absent |
      | 1         | 100    |

  Scenario: A subscribed topic refuses deletion and retains its committed offset
    Given group g5 has offsets on topic t5 with 1 partitions
      | partition | offset |
      | 0         | 9      |
    When a consumer joins with a subscription to the offset topic
    Then membership bootstrap assigns an id and the subscriber joins successfully
    When offsets are deleted from topic t5 for partitions 0
    Then offset deletion returns top-level 0 and topic t5 partition results
      | partition | error_code |
      | 0         | 86         |
    And the remaining committed offsets are exactly
      | partition | offset |
      | 0         | 9      |
