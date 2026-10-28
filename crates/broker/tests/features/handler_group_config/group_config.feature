Feature: Incremental group configuration follows Kafka controller semantics
  Scenario: Stable metadata stores every Kafka-supported group key
    Given the stable group configuration request sets these Kafka-supported keys
      | key                                 | value          |
      | consumer.session.timeout.ms         | 50000          |
      | consumer.heartbeat.interval.ms      | 6000           |
      | consumer.assignment.interval.ms     | 500            |
      | share.session.timeout.ms            | 50000          |
      | share.heartbeat.interval.ms         | 6000           |
      | share.record.lock.duration.ms       | 45000          |
      | share.delivery.count.limit          | 7              |
      | share.partition.max.record.locks    | 1000           |
      | share.isolation.level               | read_committed |
      | share.renew.acknowledge.enable       | false          |
      | share.assignment.interval.ms        | 500            |
      | streams.initial.rebalance.delay.ms  | 0              |
      | streams.assignment.interval.ms      | 500            |
      | streams.num.standby.replicas         | 1              |
    When the group configuration handler stages the request
    Then one authoritative group configuration record preserves every requested key and value

  Scenario Outline: Share offset reset accepts supported strategies and rejects malformed durations
    Given share group share-workers sets offset reset strategy <strategy>
    When the group configuration handler stages the request
    Then offset reset validation returns error <code> and stages a record only on success
    Examples:
      | strategy          | code |
      | latest            | 0    |
      | earliest          | 0    |
      | by_duration:PT1H   | 0    |
      | by_duration:-PT1H  | 40   |
      | by_duration:       | 40   |
      | none              | 40   |

  Scenario Outline: Operation merge and group identity failures precede record construction
    Given group <group> requests operation <operation> on key <key> with value 1
    When the controller merges operations and validates the group resource
    Then the group resource is refused with code <code> and message <message>
    Examples:
      | group | operation | key                         | code | message                                                                                   |
      | g     | 0         | not.a.group.key             | 40   | Unknown group config name: not.a.group.key                                                 |
      | g     | 2         | streams.session.timeout.ms  | 40   | Can't APPEND to key streams.session.timeout.ms because its type is not LIST.                |
      | empty | 0         | streams.num.standby.replicas | 42   | Default group resources are not allowed.                                                   |

  Scenario: A valid group operation produces the authoritative configuration record
    Given group g requests operation 0 on key streams.num.standby.replicas with value 1
    When the controller merges operations and validates the group resource
    Then the resulting record sets one standby replica for group g
