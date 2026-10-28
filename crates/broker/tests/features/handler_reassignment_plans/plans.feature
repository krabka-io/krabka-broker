Feature: AlterPartitionReassignments plans Kafka-compatible replica transitions and cancellation

  Scenario: A reorder writes the new replica order
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic foo partition 0 is reassigned to 3,2,1 with replication factor changes false
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 3,2,1 | 1,2,3 | none | none | 5 |

  Scenario: The target precedes removing replicas
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic foo partition 0 is reassigned to 3,4,1 with replication factor changes false
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 3,4,1,2 | 1,2,3 | 4 | 2 | 5 |

  Scenario: A new target over an in-flight reassignment retains every old replica
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3,4 | 1,2,3 | 4 | 2,3 |
    When topic foo partition 0 is reassigned to 5,6 with replication factor changes true
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 5,6,1,2,3,4 | 1,2,3 | 5,6 | 1,2,3,4 | 5 |

  Scenario: Pure removal completes immediately and advances the leader epoch
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic foo partition 0 is reassigned to 1,2 with replication factor changes true
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 1,2 | 1,2 | none | none | 6 |

  Scenario: Adding and removing lists are sorted independently from target order
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic foo partition 0 is reassigned to 5,4,1 with replication factor changes false
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 5,4,1,2,3 | 1,2,3 | 4,5 | 2,3 | 5 |

  Scenario: An old removing leader remains in the in-flight union
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3,4 | 1,2,3 | 4 | 1,2,3 |
    When topic foo partition 0 is reassigned to 5 with replication factor changes true
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 5,1,2,3,4 | 1,2,3 | 5 | 1,2,3,4 | 5 |

  Scenario: An added replica must join the ISR before completion
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2 | 1,2 | none | none |
    When topic foo partition 0 is reassigned to 1,2,3 with replication factor changes true
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 1,2,3 | 1,2 | 3 | none | 5 |

  Scenario: Removing the leader elects the first target already in ISR
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic foo partition 0 is reassigned to 2,3 with replication factor changes true
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 2 | 2,3 | 2,3 | none | none | 6 |

  Scenario: An empty difference can complete the existing in-flight addition
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3,4 | 1,3,4 | 4 | 2 |
    When topic foo partition 0 is reassigned to 1,3,4 with replication factor changes false
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 1,3,4 | 1,3,4 | none | none | 6 |

  Scenario: A decrease waits while a target replica is outside the ISR
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2 | none | none |
    When topic foo partition 0 is reassigned to 1,3 with replication factor changes true
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 1,3,2 | 1,2 | none | 2 | 5 |

  Scenario: An assignment already at its target writes nothing
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic foo partition 0 is reassigned to 1,2,3 with replication factor changes false
    Then the reassignment produces no metadata record

  Scenario: Cancelling an added leader elects from the reverted ISR
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 4 | 1,2,3,4 | 1,4 | 4 | 2,3 |
    When reassignment for topic foo partition 0 is cancelled
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 1,2,3 | 1 | none | none | 6 |

  Scenario: Cancelling replaces a leader outside the reverted ISR
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 2 | 1,2,3,4 | 1,4 | 4 | 3 |
    When reassignment for topic foo partition 0 is cancelled
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 1,2,3 | 1 | none | none | 6 |

  Scenario: Cancelling a removal-only reassignment keeps the leader epoch
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | 3 |
    When reassignment for topic foo partition 0 is cancelled
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 1,2,3 | 1,2,3 | none | none | 5 |

  Scenario: Dropping an added replica on cancel advances the leader epoch
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | 3 | none |
    When reassignment for topic foo partition 0 is cancelled
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 1,2 | 1,2 | none | none | 6 |

  Scenario: An ISR containing only added replicas requires unclean election
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 3 | 1,2,3 | 3 | 3 | none |
    When reassignment for topic foo partition 0 is cancelled
    Then the reassignment is refused with code 39 and message Unable to revert partition assignment for foo:0 because it would require an unclean leader election.

  Scenario: An allowed unclean cancel reverts to the first original replica
    Given partition foo:0 at leader epoch 5 and partition epoch 11 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 3 | 1,2,3 | 3 | 3 | none |
    And topic foo permits unclean leader elections
    When reassignment for topic foo partition 0 is cancelled
    Then the complete reassignment record has partition epoch 12 nil directory IDs and these fields
      | leader | replicas | isr | adding | removing | leader_epoch |
      | 1 | 1,2 | 1 | none | none | 6 |

  Scenario: An unknown topic is refused
    Given partition foo:0 at leader epoch 5 and partition epoch 0 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic bar partition 0 is reassigned to 1 with replication factor changes true
    Then the reassignment is refused with code 3 and message Unable to find a topic named bar.

  Scenario: An unknown partition is refused
    Given partition foo:0 at leader epoch 5 and partition epoch 0 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic foo partition 1 is reassigned to 1 with replication factor changes true
    Then the reassignment is refused with code 3 and message Unable to find partition foo:1.

  Scenario: An empty target is refused
    Given partition foo:0 at leader epoch 5 and partition epoch 0 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic foo partition 0 is reassigned to empty with replication factor changes true
    Then the reassignment is refused with code 39 and message The manual partition assignment includes an empty replica list.

  Scenario: A duplicate target replica is refused
    Given partition foo:0 at leader epoch 5 and partition epoch 0 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic foo partition 0 is reassigned to 1,1 with replication factor changes true
    Then the reassignment is refused with code 39 and message The manual partition assignment includes the broker 1 more than once.

  Scenario: An unregistered target replica is refused
    Given partition foo:0 at leader epoch 5 and partition epoch 0 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic foo partition 0 is reassigned to 99 with replication factor changes true
    Then the reassignment is refused with code 39 and message The manual partition assignment includes broker 99, but no such broker is registered.

  Scenario: A negative target broker ID is refused
    Given partition foo:0 at leader epoch 5 and partition epoch 0 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic foo partition 0 is reassigned to -1 with replication factor changes true
    Then the reassignment is refused with code 39 and message The manual partition assignment includes broker -1, but no such broker is registered.

  Scenario: The lowest target broker ID determines the first fault
    Given partition foo:0 at leader epoch 5 and partition epoch 0 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic foo partition 0 is reassigned to 99,1,1 with replication factor changes true
    Then the reassignment is refused with code 39 and message The manual partition assignment includes the broker 1 more than once.

  Scenario: Changing replication factor requires permission
    Given partition foo:0 at leader epoch 5 and partition epoch 0 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When topic foo partition 0 is reassigned to 1,2 with replication factor changes false
    Then the reassignment is refused with code 38 and message The replication factor is changed from 3 to 2

  Scenario: Cancelling without a reassignment is refused
    Given partition foo:0 at leader epoch 5 and partition epoch 0 has these replicas with brokers 1 through 6 registered
      | leader | replicas | isr | adding | removing |
      | 1 | 1,2,3 | 1,2,3 | none | none |
    When reassignment for topic foo partition 0 is cancelled
    Then the reassignment is refused with code 85 and message No partition reassignment is in progress.
