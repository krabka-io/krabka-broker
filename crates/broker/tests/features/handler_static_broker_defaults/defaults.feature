Feature: DescribeConfigs identifies built-in transaction expiry settings as defaults
  Scenario: Unconfigured expiry settings retain their complete Kafka metadata
    Given the broker has Kafka defaults for transactional ID expiry and expired transaction cleanup
    When DescribeConfigs requests both transaction expiry keys with synonyms and documentation
    Then both expiry entries report their exact Kafka default values types documentation and default source synonyms
