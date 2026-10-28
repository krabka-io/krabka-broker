Feature: Telemetry preserves evidence even when brokers do not answer
  Scenario: tests: an unanswering node still gets a row for every sample
    Given node k1 has ten data bytes but its metrics listener refuses connections
    When telemetry samples every twenty milliseconds for fifty milliseconds and stops
    Then each unanswered sample retains the node process and disk measurements

  Scenario: tests: a cluster of no nodes starts and stops
    Given the telemetry cluster has no nodes
    When telemetry starts with a ten-millisecond interval and immediately stops
    Then the empty run has an empty summary and still writes its evidence file
