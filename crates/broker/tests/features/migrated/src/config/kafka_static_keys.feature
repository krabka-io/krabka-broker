Feature: Truthful static Kafka configuration descriptions
  DescribeConfigs states operator values and intentional departures from Kafka
  defaults in catalog order. Runtime provenance never names a missing field,
  and opt-in or unbounded settings retain Kafka's visibility and sentinel rules.

  Scenario: Every catalog key except the trunk-only streams assignor has a Kafka default
    Given the broker has no operator-supplied static configuration overrides
    When the static Kafka key catalog is enumerated
    Then only group.streams.assignors lacks a released Kafka default

  Scenario: Static config keys are listed once
    Given the broker has no operator-supplied static configuration overrides
    When the static Kafka key catalog is enumerated
    Then every static Kafka key name occurs exactly once

  Scenario: Runtime provenance names real configuration fields
    Given the broker has no operator-supplied static configuration overrides
    When the catalog's runtime field names are resolved against the runtime configuration schema
    Then every referenced runtime configuration field exists

  Scenario: An untouched broker discloses only its three intentional default differences
    Given the broker has no operator-supplied static configuration overrides
    When the broker describes the selected static Kafka settings
    Then the ordered stated static settings are:
      | key                         | value   |
      | socket.send.buffer.bytes    | 1048576 |
      | socket.receive.buffer.bytes | 1048576 |
      | broker.heartbeat.interval.ms | 3000   |

  Scenario: Released mode does not disclose a trunk-only streams assignor
    Given the broker has no operator-supplied static configuration overrides
    And the static-config query selects the streams assignor list
    When the broker describes the selected static Kafka settings
    Then the ordered stated static settings are:
      | key | value |

  Scenario: Unstable mode discloses the running streams assignor
    Given the broker has no operator-supplied static configuration overrides
    And the static-config query selects the streams assignor list
    And unstable API versions are enabled for static configuration
    When the broker describes the selected static Kafka settings
    Then the ordered stated static settings are:
      | key                     | value |
      | group.streams.assignors | auto  |

  Scenario: Kafka-default metadata-log settings need no static-value disclosure
    Given the broker has no operator-supplied static configuration overrides
    And the static-config query selects metadata-log settings
    When the broker describes the selected static Kafka settings
    Then the ordered stated static settings are:
      | key | value |

  Scenario: Disabling metadata no-op records is disclosed as a zero idle interval
    Given the broker has no operator-supplied static configuration overrides
    And the static-config query selects metadata-log settings
    And the metadata log runs with these changed settings:
      | key                           | value |
      | metadata.max.idle.interval.ms | 0     |
    When the broker describes the selected static Kafka settings
    Then the ordered stated static settings are:
      | key                           | value |
      | metadata.max.idle.interval.ms | 0     |

  Scenario: Unbounded metadata retention uses Kafka's minus-one sentinels
    Given the broker has no operator-supplied static configuration overrides
    And the static-config query selects metadata-log settings
    And the metadata log runs with these changed settings:
      | key                          | value |
      | metadata.max.retention.bytes | -1    |
      | metadata.max.retention.ms    | -1    |
    When the broker describes the selected static Kafka settings
    Then the ordered stated static settings are:
      | key                          | value |
      | metadata.max.retention.bytes | -1    |
      | metadata.max.retention.ms    | -1    |

  Scenario: Every changed metadata-log setting reports its actual runtime units
    Given the broker has no operator-supplied static configuration overrides
    And the static-config query selects metadata-log settings
    And the metadata log runs with these changed settings:
      | key                           | value    |
      | metadata.log.segment.bytes    | 16777216 |
      | metadata.log.segment.ms       | 3600000  |
      | metadata.max.retention.bytes  | 0        |
      | metadata.max.retention.ms     | 0        |
      | metadata.max.idle.interval.ms | 250      |
    When the broker describes the selected static Kafka settings
    Then the ordered stated static settings are:
      | key                           | value    |
      | metadata.log.segment.bytes    | 16777216 |
      | metadata.log.segment.ms       | 3600000  |
      | metadata.max.retention.bytes  | 0        |
      | metadata.max.retention.ms     | 0        |
      | metadata.max.idle.interval.ms | 250      |

  Scenario: Kafka-default follower-fetch limits need no static-value disclosure
    Given the broker has no operator-supplied static configuration overrides
    And the static-config query selects follower-fetch limits
    And the follower requests at most 1048576 bytes per partition and 10485760 bytes per response
    When the broker describes the selected static Kafka settings
    Then the ordered stated static settings are:
      | key | value |

  Scenario: Changed follower-fetch limits report the bytes sent on the wire
    Given the broker has no operator-supplied static configuration overrides
    And the static-config query selects follower-fetch limits
    And the follower requests at most 2097152 bytes per partition and 20971520 bytes per response
    When the broker describes the selected static Kafka settings
    Then the ordered stated static settings are:
      | key                              | value    |
      | replica.fetch.max.bytes           | 2097152  |
      | replica.fetch.response.max.bytes | 20971520 |
