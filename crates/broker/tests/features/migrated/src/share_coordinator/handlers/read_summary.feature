Feature: ReadShareGroupStateSummary preserves persisted state and refusal fields

  Background:
    Given ReadShareGroupStateSummary reads topic 29292929-2929-2929-2929-292929292929 with 2 partitions and share-group partition 0 at state epoch 17 leader epoch 3 start 101 acknowledged batch 101..105 count 3 completed 9

  Scenario: Persisted state is returned
    Given the ReadShareGroupStateSummary coordinator leadership is true
    When ReadShareGroupStateSummary requests group share-group and these partitions
      | partition |
      | 0 |
    Then the complete ReadShareGroupStateSummary response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | leader_epoch | delivery_complete_count |
      | 0 | 0 | null | 17 | 101 | 3 | 9 |

  Scenario: An uninitialized key has the Kafka empty summary
    Given the ReadShareGroupStateSummary coordinator leadership is true
    When ReadShareGroupStateSummary requests group share-group and these partitions
      | partition |
      | 1 |
    Then the complete ReadShareGroupStateSummary response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | leader_epoch | delivery_complete_count |
      | 1 | 0 | null | 0 | -1 | 0 | -1 |

  Scenario: A resigned coordinator refuses reads
    Given the ReadShareGroupStateSummary coordinator leadership is false
    When ReadShareGroupStateSummary requests group share-group and these partitions
      | partition |
      | 0 |
    Then the complete ReadShareGroupStateSummary response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | leader_epoch | delivery_complete_count |
      | 0 | 16 | Unable to read share group state summary: This is not the correct coordinator. | 0 | 0 | 0 | -1 |

  Scenario: A negative partition is refused
    Given the ReadShareGroupStateSummary coordinator leadership is true
    When ReadShareGroupStateSummary requests group share-group and these partitions
      | partition |
      | -1 |
    Then the complete ReadShareGroupStateSummary response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | leader_epoch | delivery_complete_count |
      | -1 | 42 | The partition id cannot be a negative number. | 0 | 0 | 0 | -1 |

  Scenario: An out of range partition is unknown
    Given the ReadShareGroupStateSummary coordinator leadership is true
    When ReadShareGroupStateSummary requests group share-group and these partitions
      | partition |
      | 2 |
    Then the complete ReadShareGroupStateSummary response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | leader_epoch | delivery_complete_count |
      | 2 | 3 | This server does not host this topic-partition. | 0 | 0 | 0 | -1 |

  Scenario: An empty group returns no rows
    Given the ReadShareGroupStateSummary coordinator leadership is true
    When ReadShareGroupStateSummary requests group "" and these partitions
      | partition |
      | 0 |
    Then the complete ReadShareGroupStateSummary response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | leader_epoch | delivery_complete_count |

  Scenario: An empty partition list returns no rows
    Given the ReadShareGroupStateSummary coordinator leadership is true
    When ReadShareGroupStateSummary requests group share-group and these partitions
      | partition |
    Then the complete ReadShareGroupStateSummary response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | leader_epoch | delivery_complete_count |

  Scenario: An empty topic list returns no rows
    Given the ReadShareGroupStateSummary coordinator leadership is true
    When ReadShareGroupStateSummary requests group share-group with no topic rows
    Then the complete ReadShareGroupStateSummary response has these rows and default tags
      | partition | code | message | state_epoch | start_offset | leader_epoch | delivery_complete_count |
