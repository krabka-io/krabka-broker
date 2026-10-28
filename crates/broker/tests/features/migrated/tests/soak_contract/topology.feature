Feature: Fault targets follow the current cluster topology
  Scenario Outline: tests: a target resolves against the cluster as it is
    Given three nodes have controller <controller> and first-partition leader <leader>
    And the fault targets <target>
    When the executor resolves the fault target against the three nodes
    Then the selected fault node is <node>

    Examples:
      | example                       | controller | leader | target         | node |
      | controller                    | 3          | 1      | controller     | 3    |
      | leader                        | 3          | 1      | leader         | 1    |
      | follower                      | 3          | 1      | follower       | 2    |
      | follower when one node is both| 1          | 1      | follower       | 2    |
      | broker                        | none       | none   | broker-index-2 | 3    |

  Scenario Outline: A fault with no eligible target is rejected
    Given three nodes have controller <controller> and first-partition leader <leader>
    And the fault targets <target>
    When the executor resolves the fault target against the three nodes
    Then the fault target cannot be resolved

    Examples:
      | example             | controller | leader | target         |
      | broker out of range | none       | none   | broker-index-3 |
      | no controller       | none       | none   | controller     |
      | unresolved any      | none       | none   | any            |

  Scenario Outline: tests: a target records the one role it held
    Given three nodes have controller <controller> and first-partition leader <leader>
    And faulted node <node> needs its role recorded
    When the executor records the faulted node's current role
    Then the recorded fault role is <role>

    Examples:
      | example                      | controller | leader | node | role       |
      | controller wins over leader  | 1          | 1      | 1    | controller |
      | leader                       | 1          | 2      | 2    | leader     |
      | follower                     | 1          | 2      | 3    | follower   |
      | unknown                      | none       | 2      | 3    | broker     |

  Scenario: tests: a node leads the partitions it is leader of that can move
    Given replicated a partitions have full ISR while b-0 lacks replica two and audit is single-replica
    When the executor lists movable partitions led by nodes one and three
    Then node one leads a-0 and b-0 while node three leads nothing movable

  Scenario Outline: tests: failover needs a live leader elsewhere for every partition
    Given node one previously led a-0 and a-1 which now have leaders <first> and <second>
    When the executor checks that node one's former partitions have failed over
    Then failover is complete for every previously led partition: <complete>

    Examples:
      | example             | first | second | complete |
      | still led by target | 1     | 2      | false    |
      | moved               | 2     | 3      | true     |
      | leaderless          | -1    | 3      | false    |

  Scenario: tests: nothing led has nothing to fail over
    Given replicated a partitions have full ISR while b-0 lacks replica two and audit is single-replica
    And node one previously led no partitions
    When the executor checks that node one's former partitions have failed over
    Then failover is complete for every previously led partition: true

  Scenario Outline: tests: the isr is complete only when every partition holds every replica
    Given replicated a partitions have full ISR while b-0 lacks replica two and audit is single-replica
    And the expected topic partition counts are <topics>
    When the executor checks every expected topic partition for full ISR
    Then every expected partition satisfies full ISR: <complete>

    Examples:
      | example           | topics  | complete |
      | complete          | a:2     | true     |
      | shrunk            | a:2,b:1 | false    |
      | missing partition | a:3     | false    |
      | unknown topic     | c:1     | false    |

  Scenario: tests: the controller is the node whose gauge reads one
    Given controller scrapes include node two active all-inactive and empty snapshots
    When the executor resolves the controller from each scrape snapshot
    Then the active controller is node two only in the first snapshot
