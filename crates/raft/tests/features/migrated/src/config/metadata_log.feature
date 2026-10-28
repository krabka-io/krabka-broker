Feature: Kafka metadata log storage defaults
  A freshly configured metadata log follows Kafka's size, retention and directory
  conventions without requiring operator overrides.

  Background:
    Given the metadata log root directory is /mnt/kafka/kafka-metadata-logs

  Scenario: Fresh metadata storage uses Kafka's documented default limits
    When default metadata log storage is resolved
    Then metadata storage uses these Kafka limits:
      | setting               | value      |
      | segment_bytes         | 1073741824 |
      | segment_roll_ms       | 604800000  |
      | retention_bytes       | 104857600  |
      | retention_ms          | 604800000  |
      | idle_ms               | 500        |
      | minimum_segment_bytes | 8388608    |

  Scenario: The metadata partition uses Kafka's cluster metadata directory name
    When default metadata log storage is resolved
    Then the metadata partition directory is /mnt/kafka/kafka-metadata-logs/__cluster_metadata-0
