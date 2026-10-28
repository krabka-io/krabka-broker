Feature: Benchmark record isolation
  Scenario: A scenario cannot measure latency from another scenario's records
    Given a 64-byte benchmark record stamped for scenario 42
    When the record timestamp is read as scenario 7
    Then the foreign scenario record has no benchmark timestamp
