Feature: Default committed-offset retention
  An operator who leaves retention unset gets Kafka's seven-day retention
  lifetime and ten-minute cleanup cadence.

  Scenario: An untouched broker retains offsets for Kafka's default lifetime
    Given the operator has not overridden offset retention or its sweep interval
    When the broker resolves its offset retention policy
    Then offsets are retained for 10080 minutes with a 600000 millisecond sweep interval
