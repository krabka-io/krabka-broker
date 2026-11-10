Feature: Topic configuration values keep Kafka's ConfigDef ranges

  Rule: A retention key accepts Kafka's sentinels and nothing below them

    Scenario Outline: A retention value on or above its lower bound is accepted
      Given a topic alteration sets <key> to <value>
      When the broker attempts to validate the topic value
      Then the topic value is accepted

      Examples:
        | case                                     | key                 | value |
        | retention.ms is unlimited at minus one   | retention.ms        | -1    |
        | retention.ms holds a positive window     | retention.ms        | 60000 |
        | local.retention.ms inherits at minus two | local.retention.ms  | -2    |
        | local.retention.ms is unlimited          | local.retention.ms  | -1    |
        | local.retention.ms holds a window        | local.retention.ms  | 60000 |
        | delete.retention.ms may be zero          | delete.retention.ms | 0     |

    Scenario Outline: A retention value below its lower bound is refused
      Given a topic alteration sets <key> to <value>
      When the broker attempts to validate the topic value
      Then the topic value is refused with Invalid value <value> for configuration <key>: Value must be at least <bound>

      Examples:
        | case                              | key                 | value | bound |
        | retention.ms just below minus one | retention.ms        | -2    | -1    |
        | local.retention.ms below inherit  | local.retention.ms  | -3    | -2    |
        | delete.retention.ms below zero    | delete.retention.ms | -1    | 0     |

  Rule: remote.storage.enable takes only a Kafka boolean

    Scenario Outline: Either boolean turns tiered storage on or off
      Given a topic alteration sets remote.storage.enable to <value>
      When the broker attempts to validate the topic value
      Then the topic value is accepted

      Examples:
        | case               | value |
        | tiering turned on  | true  |
        | tiering turned off | false |

    Scenario: A word that is not a boolean is refused
      Given a topic alteration sets remote.storage.enable to yes
      When the broker attempts to validate the topic value
      Then the topic value is refused as not a Kafka boolean
