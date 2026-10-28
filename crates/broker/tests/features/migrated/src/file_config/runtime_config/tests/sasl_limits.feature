Feature: Runtime SASL limits honor Kafka defaults and zero delay semantics

  Scenario: Defaults match Kafka
    Given a runtime configuration file contains:
      """
      [runtime]
      """
    When the runtime files are applied in order to a default broker
    Then SASL accepts at most 524288 bytes and failed authentication waits 100 milliseconds

  Scenario: Zero delay disables waiting with a smaller receive ceiling
    Given a runtime configuration file contains:
      """
      [runtime]
      sasl_server_max_receive = "64KiB"
      connection_failed_authentication_delay = "0ms"
      """
    When the runtime files are applied in order to a default broker
    Then SASL accepts at most 65536 bytes and failed authentication waits 0 milliseconds

  Scenario: Explicit delay retains default receive ceiling
    Given a runtime configuration file contains:
      """
      [runtime]
      connection_failed_authentication_delay = "250ms"
      """
    When the runtime files are applied in order to a default broker
    Then SASL accepts at most 524288 bytes and failed authentication waits 250 milliseconds

  Scenario: Negative authentication delay is refused
    Given a runtime configuration file contains:
      """
      [runtime]
      connection_failed_authentication_delay = "-1ms"
      """
    When the runtime files are applied in order to a default broker
    Then runtime application refuses a field named connection_failed_authentication_delay
