Feature: tests soak contract topology

  Scenario Outline: tests: a target resolves against the cluster as it is
    Then soak contract topology tests: a target resolves against the cluster as it is for <example>

    Examples:
      | example                        |
      | controller                     |
      | leader                         |
      | follower                       |
      | follower when one node is both |
      | broker                         |
      | broker out of range            |
      | no controller                  |
      | unresolved any                 |

  Scenario Outline: tests: a target records the one role it held
    Then soak contract topology tests: a target records the one role it held for <example>

    Examples:
      | example                     |
      | controller wins over leader |
      | leader                      |
      | follower                    |
      | unknown                     |

  Scenario: tests: a node leads the partitions it is leader of that can move
    Then soak contract topology tests: a node leads the partitions it is leader of that can move

  Scenario Outline: tests: failover needs a live leader elsewhere for every partition
    Then soak contract topology tests: failover needs a live leader elsewhere for every partition for <example>

    Examples:
      | example             |
      | still led by target |
      | moved               |
      | leaderless          |

  Scenario: tests: nothing led has nothing to fail over
    Then soak contract topology tests: nothing led has nothing to fail over

  Scenario Outline: tests: the isr is complete only when every partition holds every replica
    Then soak contract topology tests: the isr is complete only when every partition holds every replica for <example>

    Examples:
      | example           |
      | complete          |
      | shrunk            |
      | missing partition |
      | unknown topic     |

  Scenario: tests: the controller is the node whose gauge reads one
    Then soak contract topology tests: the controller is the node whose gauge reads one
