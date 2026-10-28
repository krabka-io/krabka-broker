Feature: WriteShareGroupState preserves complete responses and stored summaries

  Background:
    Given a led Share coordinator has topic 33333333-3333-3333-3333-333333333333 with 8 partitions and share-group partition 4 at state epoch 17 start 90 leader epoch 0 completed 0

  Scenario: A matching state epoch applies the delivery delta
    Given the WriteShareGroupState coordinator leadership is true
    When WriteShareGroupState writes group share-group leader epoch 3 start 101 completed 9 acknowledged batch 101..105 count 3 to these partitions and observes stored partition 4
      | partition | state_epoch |
      | 4 | 17 |
    Then the complete WriteShareGroupState response has these rows and default tags
      | partition | code | message |
      | 4 | 0 | null |
    And the stored Share write summary is [17,0,101,9]

  Scenario: An uninitialized key refuses the write
    Given the WriteShareGroupState coordinator leadership is true
    When WriteShareGroupState writes group share-group leader epoch 3 start 101 completed 9 acknowledged batch 101..105 count 3 to these partitions and observes stored partition 4
      | partition | state_epoch |
      | 6 | 17 |
    Then the complete WriteShareGroupState response has these rows and default tags
      | partition | code | message |
      | 6 | 42 | Write operation on uninitialized share partition not allowed. |
    And the stored Share write summary is [17,0,90,0]

  Scenario: An old state epoch is fenced
    Given the WriteShareGroupState coordinator leadership is true
    When WriteShareGroupState writes group share-group leader epoch 3 start 101 completed 9 acknowledged batch 101..105 count 3 to these partitions and observes stored partition 4
      | partition | state_epoch |
      | 4 | 16 |
    Then the complete WriteShareGroupState response has these rows and default tags
      | partition | code | message |
      | 4 | 124 | The coordinator rejected the request because the state epoch did not match. |
    And the stored Share write summary is [17,0,90,0]

  Scenario: A resigned coordinator refuses the write
    Given the WriteShareGroupState coordinator leadership is false
    When WriteShareGroupState writes group share-group leader epoch 3 start 101 completed 9 acknowledged batch 101..105 count 3 to these partitions and observes stored partition 4
      | partition | state_epoch |
      | 4 | 17 |
    Then the complete WriteShareGroupState response has these rows and default tags
      | partition | code | message |
      | 4 | 16 | Unable to write share group state: This is not the correct coordinator. |
    And the stored Share write summary is null

  Scenario: An empty group returns no rows
    Given the WriteShareGroupState coordinator leadership is true
    When WriteShareGroupState writes group "" leader epoch 3 start 101 completed 9 acknowledged batch 101..105 count 3 to these partitions and observes stored partition 4
      | partition | state_epoch |
      | 4 | 17 |
    Then the complete WriteShareGroupState response has these rows and default tags
      | partition | code | message |
    And the stored Share write summary is [17,0,90,0]

  Scenario: An empty partition list returns no rows
    Given the WriteShareGroupState coordinator leadership is true
    When WriteShareGroupState writes group share-group leader epoch 3 start 101 completed 9 acknowledged batch 101..105 count 3 to these partitions and observes stored partition 4
      | partition | state_epoch |
    Then the complete WriteShareGroupState response has these rows and default tags
      | partition | code | message |
    And the stored Share write summary is [17,0,90,0]
