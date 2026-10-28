Feature: Runtime message size limits fit Kafka integer bytes
  The TOML surface accepts zero and rejects byte counts Kafka cannot represent.

  Scenario Outline: Apply a valid maximum message size
    Given the runtime file sets message_max_bytes to <value>
    When the runtime file is parsed and applied to a default broker
    Then the partition maximum message size is <bytes> bytes

    Examples:
      | value | bytes |
      | 0B | 0 |
      | 2KiB | 2048 |
      | 2147483647B | 2147483647 |

  Scenario Outline: Refuse an unrepresentable maximum message size
    Given the runtime file sets message_max_bytes to <value>
    When the runtime file is parsed and applied to a default broker
    Then the runtime file cannot be applied as a valid message size

    Examples:
      | value |
      | -1B |
      | 2147483648B |
      | 2GiB |
      | 1.5B |

  Scenario: Omitting the size preserves Kafka's default
    Given the runtime file omits message_max_bytes
    When the runtime file is parsed and applied to a default broker
    Then the partition maximum message size is 1048588 bytes
