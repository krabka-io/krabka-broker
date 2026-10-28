Feature: Kafka transactional-ID expiry configuration defaults
  Default transaction expiry and cleanup intervals agree with Kafka, and their
  startup-only nature remains visible to operators through DescribeConfigs.

  Scenario Outline: Transaction expiry settings retain Kafka defaults and read-only flags
    Given the registered broker configuration key is <key>
    When DescribeConfigs reads the registered key's metadata
    Then the registered key's quoted Kafka default is <value>
    And the registered key is read-only to dynamic alterations

    Examples:
      | key | value |
      | transactional.id.expiration.ms | "604800000" |
      | transaction.remove.expired.transaction.cleanup.interval.ms | "3600000" |
