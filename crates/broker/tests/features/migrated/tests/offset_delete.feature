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

  Scenario: An unknown group is refused before partition processing
    Given topic t2 exists but group ghost has never joined or committed
    When offsets are deleted from topic t2 for partitions 0
    Then the unknown group is refused without any topic rows

  Scenario: An existing group still receives a missing-topic partition error
    Given group g3 has offsets on topic t3 with 1 partitions
      | partition | offset |
      | 0         | 7      |
    When offsets are deleted from topic nonexistent for partitions 0
    Then offset deletion returns top-level 0 and topic nonexistent partition results
      | partition | error_code |
      | 0         | 3          |

  Scenario: An invalid partition is refused before a valid partition is deleted
    Given group g4 has offsets on topic t4 with 1 partitions
      | partition | offset |
      | 0         | 5      |
    When offsets are deleted from topic t4 for partitions 0,99
    Then offset deletion returns top-level 0 and topic t4 partition results
      | partition | error_code |
      | 99        | 3          |
      | 0         | 0          |

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
