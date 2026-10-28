Feature: Kafka released group overrides honor broker bounds and types
  Group administrators receive the same validation outcomes as Kafka 4.3.1.

  Scenario: Validate consumer.session.timeout.ms=50000
    Given the broker uses default group configuration bounds
    And group override consumer.session.timeout.ms is 50000
    When the group alteration is validated with released API rules
    Then the group alteration is accepted

  Scenario: Validate consumer.heartbeat.interval.ms=6000
    Given the broker uses default group configuration bounds
    And group override consumer.heartbeat.interval.ms is 6000
    When the group alteration is validated with released API rules
    Then the group alteration is accepted

  Scenario: Validate consumer.assignment.interval.ms=500
    Given the broker uses default group configuration bounds
    And group override consumer.assignment.interval.ms is 500
    When the group alteration is validated with released API rules
    Then the group alteration is accepted

  Scenario: Validate share.session.timeout.ms=50000
    Given the broker uses default group configuration bounds
    And group override share.session.timeout.ms is 50000
    When the group alteration is validated with released API rules
    Then the group alteration is accepted

  Scenario: Validate share.heartbeat.interval.ms=6000
    Given the broker uses default group configuration bounds
    And group override share.heartbeat.interval.ms is 6000
    When the group alteration is validated with released API rules
    Then the group alteration is accepted

  Scenario: Validate share.record.lock.duration.ms=45000
    Given the broker uses default group configuration bounds
    And group override share.record.lock.duration.ms is 45000
    When the group alteration is validated with released API rules
    Then the group alteration is accepted

  Scenario: Validate share.delivery.count.limit=7
    Given the broker uses default group configuration bounds
    And group override share.delivery.count.limit is 7
    When the group alteration is validated with released API rules
    Then the group alteration is accepted

  Scenario: Validate share.partition.max.record.locks=1000
    Given the broker uses default group configuration bounds
    And group override share.partition.max.record.locks is 1000
    When the group alteration is validated with released API rules
    Then the group alteration is accepted

  Scenario: Validate share.isolation.level=read_committed
    Given the broker uses default group configuration bounds
    And group override share.isolation.level is read_committed
    When the group alteration is validated with released API rules
    Then the group alteration is accepted

  Scenario: Validate share.renew.acknowledge.enable=false
    Given the broker uses default group configuration bounds
    And group override share.renew.acknowledge.enable is false
    When the group alteration is validated with released API rules
    Then the group alteration is accepted

  Scenario: Validate share.assignment.interval.ms=0
    Given the broker uses default group configuration bounds
    And group override share.assignment.interval.ms is 0
    When the group alteration is validated with released API rules
    Then the group alteration is accepted

  Scenario: Validate streams.initial.rebalance.delay.ms=0
    Given the broker uses default group configuration bounds
    And group override streams.initial.rebalance.delay.ms is 0
    When the group alteration is validated with released API rules
    Then the group alteration is accepted

  Scenario: Validate consumer.session.timeout.ms=1000
    Given the broker uses default group configuration bounds
    And group override consumer.session.timeout.ms is 1000
    When the group alteration is validated with released API rules
    Then the group alteration is refused with consumer.session.timeout.ms must be greater than or equal to group.consumer.min.session.timeout.ms

  Scenario: Validate consumer.heartbeat.interval.ms=20000
    Given the broker uses default group configuration bounds
    And group override consumer.heartbeat.interval.ms is 20000
    When the group alteration is validated with released API rules
    Then the group alteration is refused with consumer.heartbeat.interval.ms must be less than or equal to group.consumer.max.heartbeat.interval.ms

  Scenario: Validate share.record.lock.duration.ms=500
    Given the broker uses default group configuration bounds
    And group override share.record.lock.duration.ms is 500
    When the group alteration is validated with released API rules
    Then the group alteration is refused with Invalid value 500 for configuration share.record.lock.duration.ms: Value must be at least 1000

  Scenario: Validate share.record.lock.duration.ms=3600000
    Given the broker uses default group configuration bounds
    And group override share.record.lock.duration.ms is 3600000
    When the group alteration is validated with released API rules
    Then the group alteration is refused with share.record.lock.duration.ms must be less than or equal to group.share.max.record.lock.duration.ms

  Scenario: Validate share.delivery.count.limit=1
    Given the broker uses default group configuration bounds
    And group override share.delivery.count.limit is 1
    When the group alteration is validated with released API rules
    Then the group alteration is refused with Invalid value 1 for configuration share.delivery.count.limit: Value must be at least 2

  Scenario: Validate share.isolation.level=read_everything
    Given the broker uses default group configuration bounds
    And group override share.isolation.level is read_everything
    When the group alteration is validated with released API rules
    Then the group alteration is refused with Invalid value read_everything for configuration share.isolation.level: String must be one of: read_committed, read_uncommitted

  Scenario: Validate share.renew.acknowledge.enable=maybe
    Given the broker uses default group configuration bounds
    And group override share.renew.acknowledge.enable is maybe
    When the group alteration is validated with released API rules
    Then the group alteration is refused with Invalid value maybe for configuration share.renew.acknowledge.enable: Expected value to be either true or false

  Scenario: Validate streams.session.timeout.ms=90000
    Given the broker uses default group configuration bounds
    And group override streams.session.timeout.ms is 90000
    When the group alteration is validated with released API rules
    Then the group alteration is refused with streams.session.timeout.ms must be less than or equal to group.streams.max.session.timeout.ms

  Scenario: Validate streams.num.standby.replicas=3
    Given the broker uses default group configuration bounds
    And group override streams.num.standby.replicas is 3
    When the group alteration is validated with released API rules
    Then the group alteration is refused with streams.num.standby.replicas must be less than or equal to group.streams.max.standby.replicas

  Scenario: Validate streams.assignment.interval.ms=20000
    Given the broker uses default group configuration bounds
    And group override streams.assignment.interval.ms is 20000
    When the group alteration is validated with released API rules
    Then the group alteration is refused with streams.assignment.interval.ms must be less than or equal to group.streams.max.assignment.interval.ms
