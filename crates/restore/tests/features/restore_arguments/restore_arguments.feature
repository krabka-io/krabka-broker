Feature: Kafka-compatible restore argument values
  Scenario Outline: Cluster IDs accept Kafka and hyphenated UUID spelling
    Given restore cluster ID text <text>
    When the restore cluster ID is parsed
    Then the cluster ID is 01020304-0506-0708-090a-0b0c0d0e0f10

    Examples:
      | text                                 |
      | AQIDBAUGBwgJCgsMDQ4PEA               |
      | 01020304-0506-0708-090a-0b0c0d0e0f10 |

  Scenario: A malformed cluster ID is rejected
    Given restore cluster ID text not-a-cluster-id
    When the restore cluster ID is parsed
    Then the restore cluster ID is rejected

  Scenario Outline: Kafka topic characters are retained
    Given restore topic name <name>
    When the restore topic name is parsed
    Then the restore topic name is retained verbatim

    Examples:
      | name    |
      | orders  |
      | a       |
      | a.b_c-d |
      | 0       |

  Scenario: A topic name can use the entire Kafka length limit
    Given a restore topic name of 249 x characters
    When the restore topic name is parsed
    Then the restore topic name is retained verbatim

  Scenario Outline: Non-Kafka topic names are rejected
    Given restore topic name <name>
    When the restore topic name is parsed
    Then the restore topic name is rejected

    Examples:
      | name      |
      | .         |
      | ..        |
      | has space |
      | has/slash |
      | has:colon |
      | has=equals |
      | café      |

  Scenario: An empty topic name is rejected
    Given an empty restore argument
    When the restore topic name is parsed
    Then the restore topic name is rejected

  Scenario: A topic name cannot exceed the Kafka length limit
    Given a restore topic name of 250 x characters
    When the restore topic name is parsed
    Then the restore topic name is rejected

  Scenario: Partition references use Kafka's topic-partition spelling
    Given restore topic orders partition 3
    When the restore partition is displayed
    Then the restore partition spelling is orders-3

  Scenario Outline: Node IDs remain within Kafka's signed integer range
    Given restore node ID text <text>
    When the restore node ID is parsed
    Then the restore node ID equals <value>

    Examples:
      | text       | value      |
      | 3          | 3          |
      | 2147483647 | 2147483647 |

  Scenario Outline: Malformed or out-of-range node IDs are rejected
    Given restore node ID text <text>
    When the restore node ID is parsed
    Then the restore node ID is rejected

    Examples:
      | text       |
      | -1         |
      | x          |
      | 2147483648 |

  Scenario: An empty node ID is rejected
    Given an empty restore argument
    When the restore node ID is parsed
    Then the restore node ID is rejected
