Feature: ReadShareGroupState preserves persisted state and refusal fields

  Background:
    Given ReadShareGroupState reads topic 21212121-2121-2121-2121-212121212121 with 8 partitions and share-group partition 4 at state epoch 17 leader epoch 3 start 101 acknowledged batch 101..105 count 3 completed 9

  Scenario: Persisted state is returned
    Given the ReadShareGroupState coordinator leadership is true
    When ReadShareGroupState requests group share-group and these partitions
      | partition | leader_epoch |
      | 4 | 3 |
    Then the complete ReadShareGroupState response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | batches |
      | 4 | 0 | null | 17 | 101 | [[101,105,2,3]] |

  Scenario: An uninitialized key is refused
    Given the ReadShareGroupState coordinator leadership is true
    When ReadShareGroupState requests group share-group and these partitions
      | partition | leader_epoch |
      | 6 | 3 |
    Then the complete ReadShareGroupState response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | batches |
      | 6 | 42 | Read operation on uninitialized share partition not allowed. | 0 | 0 | [] |

  Scenario: An old leader epoch is fenced
    Given the ReadShareGroupState coordinator leadership is true
    When ReadShareGroupState requests group share-group and these partitions
      | partition | leader_epoch |
      | 4 | 2 |
    Then the complete ReadShareGroupState response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | batches |
      | 4 | 74 | The leader epoch in the request is older than the epoch on the broker. | 0 | 0 | [] |

  Scenario: A resigned coordinator refuses reads
    Given the ReadShareGroupState coordinator leadership is false
    When ReadShareGroupState requests group share-group and these partitions
      | partition | leader_epoch |
      | 4 | 3 |
    Then the complete ReadShareGroupState response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | batches |
      | 4 | 16 | Unable to read share group state: This is not the correct coordinator. | 0 | 0 | [] |

  Scenario: An empty group returns no rows
    Given the ReadShareGroupState coordinator leadership is true
    When ReadShareGroupState requests group "" and these partitions
      | partition | leader_epoch |
      | 4 | 3 |
    Then the complete ReadShareGroupState response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | batches |

  Scenario: An empty partition list returns no rows
    Given the ReadShareGroupState coordinator leadership is true
    When ReadShareGroupState requests group share-group and these partitions
      | partition | leader_epoch |
    Then the complete ReadShareGroupState response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | batches |

  Scenario: An empty topic list returns no rows
    Given the ReadShareGroupState coordinator leadership is true
    When ReadShareGroupState requests group share-group with no topic rows
    Then the complete ReadShareGroupState response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | batches |
