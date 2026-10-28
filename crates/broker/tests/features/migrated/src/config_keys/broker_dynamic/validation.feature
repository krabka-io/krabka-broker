Feature: Kafka dynamic broker configuration admission
  Unknown extension keys remain usable. Kafka-defined keys obey their dynamic
  scope, listener prefix, type, bounds, and validation precedence rules.

  Scenario: Custom key allowed
    Given the dynamic broker properties are:
      | key | value |
      | plugin.custom.key | "x" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is accepted

  Scenario: Broker log dirs refused
    Given the dynamic broker properties are:
      | key | value |
      | log.dirs | "/tmp" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Cannot update these configs dynamically: [log.dirs]

  Scenario: Ssl key requires listener prefix
    Given the dynamic broker properties are:
      | key | value |
      | ssl.keystore.location | "/k" |
    And the change targets a named broker
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message These security configs can be dynamically updated only per-listener using the listener prefix: [ssl.keystore.location]

  Scenario: Listener ssl key requires named broker
    Given the dynamic broker properties are:
      | key | value |
      | listener.name.client.ssl.keystore.location | "/k" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Cannot update these configs at default cluster level, broker id must be specified: [listener.name.client.ssl.keystore.location]

  Scenario: Listener ssl key allowed on named broker
    Given the dynamic broker properties are:
      | key | value |
      | listener.name.client.ssl.keystore.location | "/k" |
    And the change targets a named broker
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is accepted

  Scenario: Topic retention value allowed
    Given the dynamic broker properties are:
      | key | value |
      | log.retention.ms | " 86400000 " |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is accepted

  Scenario: Dynamic integer value allowed
    Given the dynamic broker properties are:
      | key | value |
      | num.io.threads | "16" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is accepted

  Scenario: Throttle rate must be nonnegative
    Given the dynamic broker properties are:
      | key | value |
      | leader.replication.throttled.rate | "-1" |
    And the change targets a named broker
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Invalid value -1 for configuration leader.replication.throttled.rate: Value must be at least 0

  Scenario: Minimum insync replicas must be positive
    Given the dynamic broker properties are:
      | key | value |
      | min.insync.replicas | "0" |
    And the change targets a named broker
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Invalid value 0 for configuration min.insync.replicas: Value must be at least 1

  Scenario: Startup only key refused
    Given the dynamic broker properties are:
      | key | value |
      | auto.leader.rebalance.enable | "false" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Cannot update these configs dynamically: [auto.leader.rebalance.enable]

  Scenario: Queued max requests refused
    Given the dynamic broker properties are:
      | key | value |
      | queued.max.requests | "10" |
    And the change targets a named broker
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Cannot update these configs dynamically: [queued.max.requests]

  Scenario: Non dynamic key precedes bad value
    Given the dynamic broker properties are:
      | key | value |
      | log.cleaner.enable | "false" |
      | num.io.threads | "abc" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Cannot update these configs dynamically: [log.cleaner.enable]

  Scenario: Offsets replication factor refused
    Given the dynamic broker properties are:
      | key | value |
      | offsets.topic.replication.factor | "1" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Cannot update these configs dynamically: [offsets.topic.replication.factor]

  Scenario: Dynamic integer value must parse
    Given the dynamic broker properties are:
      | key | value |
      | num.io.threads | "abc" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Invalid value abc for configuration num.io.threads: Not a number of type INT

  Scenario: Connection limit must be nonnegative
    Given the dynamic broker properties are:
      | key | value |
      | max.connections | "-5" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Invalid value -5 for configuration max.connections: Value must be at least 0

  Scenario: Network threads must be positive
    Given the dynamic broker properties are:
      | key | value |
      | num.network.threads | "0" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Invalid value 0 for configuration num.network.threads: Value must be at least 1

  Scenario: Tiered offset boolean is case insensitive
    Given the dynamic broker properties are:
      | key | value |
      | follower.fetch.last.tiered.offset.enable | " TRUE " |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is accepted

  Scenario: Listener override reports base key
    Given the dynamic broker properties are:
      | key | value |
      | listener.name.external.num.network.threads | "abc" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Invalid value abc for configuration num.network.threads: Not a number of type INT

  Scenario: Per broker key refused on cluster default
    Given the dynamic broker properties are:
      | key | value |
      | sasl.enabled.mechanisms | "PLAIN" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Cannot update these configs at default cluster level, broker id must be specified: [sasl.enabled.mechanisms]

  Scenario: Per broker key allowed on named broker
    Given the dynamic broker properties are:
      | key | value |
      | sasl.enabled.mechanisms | "PLAIN" |
    And the change targets a named broker
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is accepted

  Scenario: Consumer assignment interval at the maximum
    Given the dynamic broker properties are:
      | key | value |
      | group.consumer.assignment.interval.ms | "15000" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is accepted

  Scenario: Consumer assignment interval at the minimum
    Given the dynamic broker properties are:
      | key | value |
      | group.consumer.assignment.interval.ms | "0" |
    And the change targets a named broker
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is accepted

  Scenario: Consumer assignment interval above the maximum
    Given the dynamic broker properties are:
      | key | value |
      | group.consumer.assignment.interval.ms | "15001" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message group.consumer.assignment.interval.ms must be less than or equal to group.consumer.max.assignment.interval.ms

  Scenario: Share assignment interval above the maximum
    Given the dynamic broker properties are:
      | key | value |
      | group.share.assignment.interval.ms | " 60000 " |
    And the change targets a named broker
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message group.share.assignment.interval.ms must be less than or equal to group.share.max.assignment.interval.ms

  Scenario: Streams assignment interval above the maximum
    Given the dynamic broker properties are:
      | key | value |
      | group.streams.assignment.interval.ms | "20000" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message group.streams.assignment.interval.ms must be less than or equal to group.streams.max.assignment.interval.ms

  Scenario: Consumer interval is checked before share
    Given the dynamic broker properties are:
      | key | value |
      | group.share.assignment.interval.ms | "20000" |
      | group.consumer.assignment.interval.ms | "20000" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message group.consumer.assignment.interval.ms must be less than or equal to group.consumer.max.assignment.interval.ms

  Scenario: Negative assignment interval fails the type check first
    Given the dynamic broker properties are:
      | key | value |
      | group.consumer.assignment.interval.ms | "-1" |
    And the change targets the cluster default
    When dynamic broker validation runs with unstable API versions enabled
    Then the dynamic broker configuration is rejected with INVALID_REQUEST and message Invalid value -1 for configuration group.consumer.assignment.interval.ms: Value must be at least 0
