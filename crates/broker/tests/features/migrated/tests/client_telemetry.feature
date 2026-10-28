Feature: Telemetry receiver negotiation and client instance lifecycle

  Scenario Outline: API advertisement follows receiver configuration
    Given a standalone broker with client metrics receiver <enabled>
    When ApiVersions is requested
    Then API keys 71,72 are advertised <advertised>

    Examples:
      | enabled | advertised |
      | true    | true       |
      | false   | false      |

  Scenario: get telemetry subscriptions with nil id returns assigned id and no subscription
    Given a standalone broker with client metrics receiver true
    When GetTelemetrySubscriptions requests instance hex 00000000000000000000000000000000
    Then the subscription succeeds and assigns a nonzero instance
    And requested metrics are empty and the push interval is 300000 ms
    And compression codes are 4,3,1,2, the maximum is 1048576 bytes, and delta temporality is true

  Scenario: get telemetry subscriptions with set id echoes it
    Given a standalone broker with client metrics receiver true
    When GetTelemetrySubscriptions requests instance hex 11111111111111111111111111111111
    Then the subscription succeeds and echoes instance hex 11111111111111111111111111111111

  Scenario Outline: unregistered pushes distinguish reserved and unknown instances
    Given a standalone broker with client metrics receiver true
    When PushTelemetry sends instance hex <id>, subscription 0, compression 0, and payload hex 000102
    Then PushTelemetry is exactly a default response with error <error>

    Examples:
      | id                               | error |
      | 00000000000000000000000000000000 | 42    |
      | 00000000000000000000000000000001 | 42    |
      | 22222222222222222222222222222222 | 117   |

  Scenario Outline: enrolled pushes validate subscription identity and compression
    Given a controller-backed broker with a client metrics receiver
    And an uncompressed OTLP gauge org.apache.kafka.consumer.fetch.size with integer value 7
    When match-all subscription all is configured with interval 100 ms
    Then the client metrics configuration has one successful resource response
    When GetTelemetrySubscriptions requests instance hex 00000000000000000000000000000000
    Then the subscription succeeds and assigns a nonzero instance
    And requested metrics are * and the push interval is 100 ms
    And compression codes are 4,3,1,2, the maximum is 1048576 bytes, and delta temporality is true
    When the assigned instance pushes the OTLP gauge with subscription XOR <xor> and compression <compression>
    Then PushTelemetry returns error <error>

    Examples:
      | xor   | compression | error |
      | 0     | 0           | 0     |
      | 21845 | 0           | 117   |
      | 0     | 5           | 76    |

  Scenario: closing the connection drops the client instance
    Given a standalone broker with client metrics receiver true
    When a separate telemetry connection is opened
    And GetTelemetrySubscriptions requests instance hex 33333333333333333333333333333333
    And GetTelemetrySubscriptions requests instance hex 33333333333333333333333333333333
    Then the GetTelemetrySubscriptions response codes are 0,89
    When that connection closes and instance hex 33333333333333333333333333333333 reconnects every 50 ms for at most 10000 ms
    Then reconnection eventually assigns the instance with error 0, with only error 89 before it
