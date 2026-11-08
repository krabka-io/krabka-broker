Feature: A topic request is validated before the topic is created

  Rule: Each field must hold a valid value on its own

    Scenario Outline: A request with one invalid field is refused
      Given a valid topic request with its <field> set to <value>
      When the request is validated
      Then the request is refused because <reason>

      Examples:
        | case               | field               | value | reason                                           |
        | name with a slash  | name                | a/b   | topic name a/b contains /                        |
        | no partitions      | partitions          | 0     | a topic needs at least 1 partition               |
        | no replicas        | replication factor  | 0     | a topic needs a replication factor of at least 1 |
        | no insync replicas | min insync replicas | 0     | min insync replicas must be at least 1           |

  Rule: Min insync replicas must not exceed the replication factor

    Scenario Outline: A request whose fields conflict is refused
      Given a valid topic request changed to
        | replication factor   | min insync replicas   |
        | <replication factor> | <min insync replicas> |
      When the request is validated
      Then the request is refused because <reason>

      Examples:
        | case                   | replication factor | min insync replicas | reason                                             |
        | one more than replicas | 2                  | 3                   | min insync replicas 3 exceeds replication factor 2 |
        | a single replica       | 1                  | 2                   | min insync replicas 2 exceeds replication factor 1 |

    Scenario: A request whose min insync replicas equal its replication factor is accepted
      Given a valid topic request changed to
        | field               | value |
        | replication factor  | 2     |
        | min insync replicas | 2     |
      When the request is validated
      Then the request is accepted
