Feature: Listener idle timeout defaults
  A listener without an operator override inherits Kafka's idle connection
  window rather than an unlimited or immediate timeout.

  Scenario: An unconfigured listener inherits Kafka's ten-minute idle window
    Given no broker-wide or listener-specific idle timeout is configured
    When the broker resolves the PLAINTEXT listener idle timeout
    Then the PLAINTEXT idle timeout is Kafka's 600000 milliseconds
