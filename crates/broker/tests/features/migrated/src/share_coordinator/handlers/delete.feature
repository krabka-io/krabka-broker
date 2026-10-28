Feature: DeleteShareGroupState returns complete refusals and appends only required tombstones

  Background:
    Given a led Share coordinator has topic 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f with 2 partitions and group g partition 0 at state epoch 1 start 0

  Scenario: An initialized key is tombstoned
    Given the DeleteShareGroupState coordinator leadership is true
    When DeleteShareGroupState requests group g topic 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f partitions [0] and observes its appended records and seeded key
    Then the complete DeleteShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f | 0 | 0 | null |
    And deletion appends exactly ["tombstone"] and keeps seeded partition 0 state false

  Scenario: A missing key writes no tombstone
    Given the DeleteShareGroupState coordinator leadership is true
    When DeleteShareGroupState requests group g topic 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f partitions [1] and observes its appended records and seeded key
    Then the complete DeleteShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f | 1 | 0 | null |
    And deletion appends exactly [] and keeps seeded partition 0 state true

  Scenario: A negative partition is refused
    Given the DeleteShareGroupState coordinator leadership is true
    When DeleteShareGroupState requests group g topic 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f partitions [-1] and observes its appended records and seeded key
    Then the complete DeleteShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f | -1 | 42 | The partition id cannot be a negative number. |
    And deletion appends exactly [] and keeps seeded partition 0 state true

  Scenario: A partition beyond the topic is unknown
    Given the DeleteShareGroupState coordinator leadership is true
    When DeleteShareGroupState requests group g topic 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f partitions [2] and observes its appended records and seeded key
    Then the complete DeleteShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f | 2 | 3 | This server does not host this topic-partition. |
    And deletion appends exactly [] and keeps seeded partition 0 state true

  Scenario: A topic absent from the image is unknown
    Given the DeleteShareGroupState coordinator leadership is true
    When DeleteShareGroupState requests group g topic 22222222-2222-2222-2222-222222222222 partitions [0] and observes its appended records and seeded key
    Then the complete DeleteShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 22222222-2222-2222-2222-222222222222 | 0 | 3 | This server does not host this topic-partition. |
    And deletion appends exactly [] and keeps seeded partition 0 state true

  Scenario: An empty group returns no rows
    Given the DeleteShareGroupState coordinator leadership is true
    When DeleteShareGroupState requests group "" topic 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f partitions [0] and observes its appended records and seeded key
    Then the complete DeleteShareGroupState response has these rows and default tags
      | topic | partition | code | message |
    And deletion appends exactly [] and keeps seeded partition 0 state true

  Scenario: An empty partition list returns no rows
    Given the DeleteShareGroupState coordinator leadership is true
    When DeleteShareGroupState requests group g topic 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f partitions [] and observes its appended records and seeded key
    Then the complete DeleteShareGroupState response has these rows and default tags
      | topic | partition | code | message |
    And deletion appends exactly [] and keeps seeded partition 0 state true

  Scenario: A resigned coordinator refuses deletion
    Given the DeleteShareGroupState coordinator leadership is false
    When DeleteShareGroupState requests group g topic 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f partitions [0] and observes its appended records and seeded key
    Then the complete DeleteShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f | 0 | 16 | Unable to delete share group state: This is not the correct coordinator. |
    And deletion appends exactly [] and keeps seeded partition 0 state false
