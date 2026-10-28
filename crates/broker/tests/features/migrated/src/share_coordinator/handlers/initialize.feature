Feature: Share initialization preserves Kafka release and trunk fencing snapshot semantics

  Scenario: Equal epoch and offset under Kafka4.3.1
    Given Share initialization uses Kafka4.3.1 rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 0 | 5 | 10 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 0 | 0 | null |
    And the initialized Share state summary is [5,0,10,0]
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |
      | 1 | 5 | 0 | 10 | 0 | 1700000000000 | 1700000000000 | [] |

  Scenario: Equal epoch with a new offset under Kafka4.3.1
    Given Share initialization uses Kafka4.3.1 rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 0 | 5 | 20 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 0 | 0 | null |
    And the initialized Share state summary is [5,0,20,0]
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |
      | 1 | 5 | 0 | 20 | 0 | 1700000000000 | 1700000000000 | [] |

  Scenario: An older state epoch under Kafka4.3.1
    Given Share initialization uses Kafka4.3.1 rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 0 | 4 | 10 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 0 | 124 | The coordinator rejected the request because the state epoch did not match. |
    And the initialized Share state summary is [5,0,10,0]
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |

  Scenario: An uninitialized start offset on a new key under Kafka4.3.1
    Given Share initialization uses Kafka4.3.1 rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 1 | 1 | -1 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 1 | 0 | null |
    And the initialized Share state summary is [1,0,-1,-1]
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |
      | 0 | 1 | 0 | -1 | -1 | 1700000000000 | 1700000000000 | [] |

  Scenario: A negative partition under Kafka4.3.1
    Given Share initialization uses Kafka4.3.1 rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | -1 | 1 | 0 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | -1 | 42 | The partition id cannot be a negative number. |
    And the initialized Share state summary is null
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |

  Scenario: A negative state epoch under Kafka4.3.1
    Given Share initialization uses Kafka4.3.1 rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 0 | -1 | 0 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 0 | 0 | null |
    And the initialized Share state summary is [-1,0,0,0]
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |
      | 1 | -1 | 0 | 0 | 0 | 1700000000000 | 1700000000000 | [] |

  Scenario: A partition beyond the topic under Kafka4.3.1
    Given Share initialization uses Kafka4.3.1 rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 3 | 1 | 0 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 3 | 3 | This server does not host this topic-partition. |
    And the initialized Share state summary is null
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |

  Scenario: A topic absent from the image under Kafka4.3.1
    Given Share initialization uses Kafka4.3.1 rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 21212121-2121-2121-2121-212121212121 | 0 | 1 | 0 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 21212121-2121-2121-2121-212121212121 | 0 | 3 | This server does not host this topic-partition. |
    And the initialized Share state summary is null
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |

  Scenario: An empty group id under Kafka4.3.1
    Given Share initialization uses Kafka4.3.1 rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      |  | 20202020-2020-2020-2020-202020202020 | 0 | 6 | 0 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
    And the initialized Share state summary is [5,0,10,0]
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |

  Scenario: A resigned coordinator under Kafka4.3.1
    Given Share initialization uses Kafka4.3.1 rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is false
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 0 | 6 | 0 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 0 | 16 | Unable to initialize share group state: This is not the correct coordinator. |
    And the initialized Share state summary is null
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |

  Scenario: Equal epoch and offset under trunk
    Given Share initialization uses trunk rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 0 | 5 | 10 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 0 | 0 | null |
    And the initialized Share state summary is [5,0,10,0]
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |

  Scenario: Equal epoch with a new offset under trunk
    Given Share initialization uses trunk rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 0 | 5 | 20 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 0 | 0 | null |
    And the initialized Share state summary is [5,0,20,0]
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |
      | 1 | 5 | 0 | 20 | 0 | 1700000000000 | 1700000000000 | [] |

  Scenario: An older state epoch under trunk
    Given Share initialization uses trunk rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 0 | 4 | 10 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 0 | 124 | The coordinator rejected the request because the state epoch did not match. |
    And the initialized Share state summary is [5,0,10,0]
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |

  Scenario: An uninitialized start offset on a new key under trunk
    Given Share initialization uses trunk rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 1 | 1 | -1 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 1 | 0 | null |
    And the initialized Share state summary is [1,0,-1,-1]
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |
      | 0 | 1 | 0 | -1 | -1 | 1700000000000 | 1700000000000 | [] |

  Scenario: A negative partition under trunk
    Given Share initialization uses trunk rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | -1 | 1 | 0 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | -1 | 42 | The partition id cannot be a negative number. |
    And the initialized Share state summary is null
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |

  Scenario: A negative state epoch under trunk
    Given Share initialization uses trunk rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 0 | -1 | 0 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 0 | 42 | The state epoch cannot be a negative number. |
    And the initialized Share state summary is [5,0,10,0]
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |

  Scenario: A partition beyond the topic under trunk
    Given Share initialization uses trunk rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 3 | 1 | 0 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 3 | 3 | This server does not host this topic-partition. |
    And the initialized Share state summary is null
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |

  Scenario: A topic absent from the image under trunk
    Given Share initialization uses trunk rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 21212121-2121-2121-2121-212121212121 | 0 | 1 | 0 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 21212121-2121-2121-2121-212121212121 | 0 | 3 | This server does not host this topic-partition. |
    And the initialized Share state summary is null
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |

  Scenario: An empty group id under trunk
    Given Share initialization uses trunk rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is true
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      |  | 20202020-2020-2020-2020-202020202020 | 0 | 6 | 0 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
    And the initialized Share state summary is [5,0,10,0]
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |

  Scenario: A resigned coordinator under trunk
    Given Share initialization uses trunk rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10
    And the InitializeShareGroupState coordinator leadership is false
    When InitializeShareGroupState applies this request and observes its durable state
      | group | topic | partition | state_epoch | start_offset |
      | g | 20202020-2020-2020-2020-202020202020 | 0 | 6 | 0 |
    Then the complete InitializeShareGroupState response has these rows and default tags
      | topic | partition | code | message |
      | 20202020-2020-2020-2020-202020202020 | 0 | 16 | Unable to initialize share group state: This is not the correct coordinator. |
    And the initialized Share state summary is null
    And initialization appends exactly these complete snapshot records
      | snapshot_epoch | state_epoch | leader_epoch | start_offset | delivery_complete_count | create_timestamp | write_timestamp | batches |
