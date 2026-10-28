Feature: AlterConfigs broker replacements validate target identity and dynamic keys without normalizing stored text
  Background:
    Given brokers 1 and 2 are registered and broker 1 serves the configuration replacement

  Scenario Outline: Broker replacement errors preserve Kafka target and configuration diagnostics
    Given broker replacement targets <resource> with key <key> value <value>
    When the broker validates the complete dynamic configuration replacement
    Then broker replacement refuses with code 42 and message <message>
    Examples:
      | resource        | key                                          | value   | message                                                                                                  |
      | 2               | leader.replication.throttled.rate            | 1       | Unexpected broker id, expected 1, but received 2                                                          |
      | 7               | leader.replication.throttled.rate            | 1       | Unexpected broker id, expected 1, but received 7                                                          |
      | one             | leader.replication.throttled.rate            | 1       | Node id must be an integer, but it is: one                                                                |
      | cluster-default | log.dirs                                     | /tmp    | Cannot update these configs dynamically: [log.dirs]                                                      |
      | cluster-default | auto.leader.rebalance.enable                 | false   | Cannot update these configs dynamically: [auto.leader.rebalance.enable]                                  |
      | 1               | sasl.server.max.receive.size                 | 1048576 | Cannot update these configs dynamically: [sasl.server.max.receive.size]                                  |
      | cluster-default | connection.failed.authentication.delay.ms    | 0       | Cannot update these configs dynamically: [connection.failed.authentication.delay.ms]                     |
      | 1               | num.io.threads                               | abc     | Invalid value abc for configuration num.io.threads: Not a number of type INT                              |

  Scenario: The cluster default may store known dynamic and plugin keys
    Given broker replacement targets cluster-default with configurations
      | key              | value    |
      | log.retention.ms | 86400000 |
      | plugin.key       | x        |
    When the broker validates the complete dynamic configuration replacement
    Then the exact ordered broker configuration records are
      | node    | key              | value    |
      | cluster | log.retention.ms | 86400000 |
      | cluster | plugin.key       | x        |

  Scenario: A named broker may set its minimum ISR override
    Given broker replacement targets 1 with configurations
      | key                 | value |
      | min.insync.replicas | 2     |
    When the broker validates the complete dynamic configuration replacement
    Then the exact ordered broker configuration records are
      | node | key                 | value |
      | 1    | min.insync.replicas | 2     |

  Scenario: Validated values retain exact client spelling and records follow key order
    Given broker replacement targets cluster-default with configurations
      | key                                 | value            |
      | max.connections                     | " 100 "          |
      | log.cleaner.min.cleanable.ratio      | 1                |
      | log.cleaner.io.max.bytes.per.second  | 104857600        |
      | log.cleanup.policy                  | compact , delete |
    When the broker validates the complete dynamic configuration replacement
    Then the exact ordered broker configuration records are
      | node    | key                                 | value            |
      | cluster | log.cleaner.io.max.bytes.per.second  | 104857600        |
      | cluster | log.cleaner.min.cleanable.ratio      | 1                |
      | cluster | log.cleanup.policy                  | compact , delete |
      | cluster | max.connections                     | " 100 "          |
