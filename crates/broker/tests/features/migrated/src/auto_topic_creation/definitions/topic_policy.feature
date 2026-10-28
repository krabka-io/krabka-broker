Feature: Auto-created topics inherit coordinator policy and explicitly supplied defaults

  Background:
    Given ordinary topic defaults are 6 partitions and replication 7
    And the offsets coordinator topic uses 11 partitions and replication 3
    And the transaction coordinator topic uses 12 partitions and replication 2
    And the transaction coordinator topic minimum ISR is 2
    And the share coordinator topic uses 13 partitions and replication 4
    And the share coordinator topic minimum ISR is 3
    And the barrier coordinator topic uses 14 partitions and replication 5

  Scenario: Define __consumer_offsets with configuration variant 1
    Given the requested automatic topic name is __consumer_offsets
    When the manager builds the automatic topic definition
    Then the complete automatic topic definition is:
      | field | value |
      | name | __consumer_offsets |
      | partitions | 11 |
      | replication | 3 |
      | configs | [["cleanup.policy","compact"],["compression.type","producer"],["segment.bytes","104857600"]] |

  Scenario: Define __transaction_state with configuration variant 2
    Given the requested automatic topic name is __transaction_state
    When the manager builds the automatic topic definition
    Then the complete automatic topic definition is:
      | field | value |
      | name | __transaction_state |
      | partitions | 12 |
      | replication | 2 |
      | configs | [["cleanup.policy","compact"],["compression.type","uncompressed"],["min.insync.replicas","2"],["segment.bytes","104857600"],["unclean.leader.election.enable","false"]] |

  Scenario: Define __share_group_state with configuration variant 3
    Given the requested automatic topic name is __share_group_state
    When the manager builds the automatic topic definition
    Then the complete automatic topic definition is:
      | field | value |
      | name | __share_group_state |
      | partitions | 13 |
      | replication | 4 |
      | configs | [["cleanup.policy","delete"],["compression.type","producer"],["min.insync.replicas","3"],["retention.ms","-1"],["segment.bytes","104857600"]] |

  Scenario: Define __barrier_state with configuration variant 4
    Given the requested automatic topic name is __barrier_state
    When the manager builds the automatic topic definition
    Then the complete automatic topic definition is:
      | field | value |
      | name | __barrier_state |
      | partitions | 14 |
      | replication | 5 |
      | configs | [["cleanup.policy","compact"]] |

  Scenario: Define orders with configuration variant 5
    Given the requested automatic topic name is orders
    When the manager builds the automatic topic definition
    Then the complete automatic topic definition is:
      | field | value |
      | name | orders |
      | partitions | -1 |
      | replication | -1 |
      | configs | [] |

  Scenario: Define __remote_log_metadata with configuration variant 6
    Given the requested automatic topic name is __remote_log_metadata
    When the manager builds the automatic topic definition
    Then the complete automatic topic definition is:
      | field | value |
      | name | __remote_log_metadata |
      | partitions | -1 |
      | replication | -1 |
      | configs | [] |

  Scenario: Define orders with configuration variant 7
    Given the requested automatic topic name is orders
    And the operator explicitly supplied topic partitions true and replication false
    When the manager builds the automatic topic definition
    Then the complete automatic topic definition is:
      | field | value |
      | name | orders |
      | partitions | 6 |
      | replication | -1 |
      | configs | [] |

  Scenario: Define orders with configuration variant 8
    Given the requested automatic topic name is orders
    And the operator explicitly supplied topic partitions true and replication true
    When the manager builds the automatic topic definition
    Then the complete automatic topic definition is:
      | field | value |
      | name | orders |
      | partitions | 6 |
      | replication | 7 |
      | configs | [] |
