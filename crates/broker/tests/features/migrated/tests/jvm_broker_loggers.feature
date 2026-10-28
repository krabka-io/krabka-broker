@docker
Feature: Operators manage broker logger levels through Kafka configuration tools
  Scenario: Operators inspect override and restore a broker logger level
    Given a registered broker starts with its default INFO logger level
    When the operator describes broker loggers with and without the all flag
    Then both successful reports expose the existing broker logger at INFO
    When the operator sets the broker logger to DEBUG and reads it back
    Then the alteration succeeds and the next report shows DEBUG
    When the operator deletes the logger override and reads all levels again
    Then the deletion succeeds and the startup INFO level is restored
