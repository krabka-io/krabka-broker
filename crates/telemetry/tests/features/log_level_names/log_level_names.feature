Feature: Kafka log severity names
  Scenario Outline: Kafka severity names round trip
    Given log severity <name>
    When the severity is encoded and decoded using Kafka names
    Then the decoded severity equals the original severity

    Examples:
      | name  |
      | FATAL |
      | ERROR |
      | WARN  |
      | INFO  |
      | DEBUG |
      | TRACE |

  Scenario Outline: Non-level names are rejected
    Given Kafka log level text <name>
    When the Kafka log level text is decoded
    Then the text is rejected as a log severity

    Examples:
      | name |
      | info |
      | OFF  |

  Scenario: An empty name is rejected
    Given an empty Kafka log level text
    When the Kafka log level text is decoded
    Then the text is rejected as a log severity
