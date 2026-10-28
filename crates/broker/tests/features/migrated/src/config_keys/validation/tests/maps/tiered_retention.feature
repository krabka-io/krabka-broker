Feature: Tiered topic retention follows Kafka log configuration rules
  Local retention, cleanup policy and remote copying are validated together.

  Scenario: 01 remote.storage.enable=true with broker tiered storage false
    Given broker tiered storage is false
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
    When the resulting tiered topic map is canonicalized
    Then the topic combination is refused with exact Kafka message Tiered Storage functionality is disabled in the broker. Topic cannot be configured with remote log storage.

  Scenario: 02 remote.storage.enable=false with broker tiered storage false
    Given broker tiered storage is false
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | false |
    When the resulting tiered topic map is canonicalized
    Then the resulting tiered topic map is accepted

  Scenario: 03 remote.storage.enable=true, retention.bytes=100, local.retention.bytes=-1 with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
      | retention.bytes | 100 |
      | local.retention.bytes | -1 |
    When the resulting tiered topic map is canonicalized
    Then the topic combination is refused with exact Kafka message Invalid value -1 for configuration local.retention.bytes: Value must not be -1 as retention.bytes value is set as 100.

  Scenario: 04 remote.storage.enable=true, retention.bytes=100, local.retention.bytes=200 with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
      | retention.bytes | 100 |
      | local.retention.bytes | 200 |
    When the resulting tiered topic map is canonicalized
    Then the topic combination is refused with exact Kafka message Invalid value 200 for configuration local.retention.bytes: Value must not be more than retention.bytes property value: 100

  Scenario: 05 remote.storage.enable=true, retention.bytes=-1, local.retention.bytes=-1 with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
      | retention.bytes | -1 |
      | local.retention.bytes | -1 |
    When the resulting tiered topic map is canonicalized
    Then the resulting tiered topic map is accepted

  Scenario: 06 remote.storage.enable=true, local.retention.ms=-1 with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
      | local.retention.ms | -1 |
    When the resulting tiered topic map is canonicalized
    Then the topic combination is refused with exact Kafka message Invalid value -1 for configuration local.retention.ms: Value must not be -1 as retention.ms value is set as 604800000.

  Scenario: 07 remote.storage.enable=true, retention.ms=-1, local.retention.ms=5000 with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
      | retention.ms | -1 |
      | local.retention.ms | 5000 |
    When the resulting tiered topic map is canonicalized
    Then the resulting tiered topic map is accepted

  Scenario: 08 remote.storage.enable=true, remote.log.copy.disable=true, retention.ms=1000, local.retention.ms=500 with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
      | remote.log.copy.disable | true |
      | retention.ms | 1000 |
      | local.retention.ms | 500 |
    When the resulting tiered topic map is canonicalized
    Then the topic combination is refused with exact Kafka message When `remote.log.copy.disable` is set to true, the `local.retention.ms` and `retention.ms` must be set to the identical value because there will be no more logs copied to the remote storage.

  Scenario: 09 remote.storage.enable=true, remote.log.copy.disable=true, local.retention.ms=-2 with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
      | remote.log.copy.disable | true |
      | local.retention.ms | -2 |
    When the resulting tiered topic map is canonicalized
    Then the resulting tiered topic map is accepted

  Scenario: 10 remote.storage.enable=true, cleanup.policy=compact with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
      | cleanup.policy | compact |
    When the resulting tiered topic map is canonicalized
    Then the topic combination is refused with exact Kafka message Remote log storage only supports topics with cleanup.policy=delete or cleanup.policy being an empty list.

  Scenario: 11 remote.storage.enable=true, cleanup.policy=compact,delete with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
      | cleanup.policy | compact,delete |
    When the resulting tiered topic map is canonicalized
    Then the topic combination is refused with exact Kafka message Remote log storage only supports topics with cleanup.policy=delete or cleanup.policy being an empty list.

  Scenario: 12 remote.storage.enable=true, cleanup.policy= with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
      | cleanup.policy |  |
    When the resulting tiered topic map is canonicalized
    Then the resulting tiered topic map is accepted

  Scenario: 13 remote.storage.enable=false, local.retention.bytes=500, retention.bytes=100 with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | false |
      | local.retention.bytes | 500 |
      | retention.bytes | 100 |
    When the resulting tiered topic map is canonicalized
    Then the resulting tiered topic map is accepted

  Scenario: 14 remote.storage.enable=true, local.retention.ms=1000, remote.copy.lag.ms=2000 with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
      | local.retention.ms | 1000 |
      | remote.copy.lag.ms | 2000 |
    When the resulting tiered topic map is canonicalized
    Then the topic combination is refused with exact Kafka message Invalid value 2000 for configuration remote.copy.lag.ms: Value must not exceed local.retention.ms (effective value: 1000)

  Scenario: 15 remote.storage.enable=true, local.retention.ms=-1, retention.ms=-1, remote.copy.lag.ms=2000 with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
      | local.retention.ms | -1 |
      | retention.ms | -1 |
      | remote.copy.lag.ms | 2000 |
    When the resulting tiered topic map is canonicalized
    Then the resulting tiered topic map is accepted

  Scenario: 16 remote.storage.enable=true, retention.bytes=100, remote.copy.lag.bytes=200 with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.storage.enable | true |
      | retention.bytes | 100 |
      | remote.copy.lag.bytes | 200 |
    When the resulting tiered topic map is canonicalized
    Then the topic combination is refused with exact Kafka message Invalid value 200 for configuration remote.copy.lag.bytes: Value must not exceed local.retention.bytes (effective value: 100)

  Scenario: 17 remote.copy.lag.ms=-1 with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | remote.copy.lag.ms | -1 |
    When the resulting tiered topic map is canonicalized
    Then the resulting tiered topic map is accepted

  Scenario: 18 max.decompressed.message.bytes=2147483639 with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | max.decompressed.message.bytes | 2147483639 |
    When the resulting tiered topic map is canonicalized
    Then the resulting tiered topic map is accepted

  Scenario: 19 errors.deadletterqueue.group.enable=true with broker tiered storage true
    Given broker tiered storage is true
    And the topic alteration contains these properties:
      | key | value |
      | errors.deadletterqueue.group.enable | true |
    When the resulting tiered topic map is canonicalized
    Then the resulting tiered topic map is accepted
