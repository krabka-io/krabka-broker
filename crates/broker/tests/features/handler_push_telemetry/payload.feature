Feature: PushTelemetry validates payloads after subscription assignment
  Scenario Outline: Telemetry payload framing compression decoding and size limits follow the selected Kafka version
    Given a telemetry client has obtained a subscription with a 1024-byte limit and unstable API versions <unstable>
    And the telemetry push carries <payload> using compression ID <compression>
    When the client pushes telemetry using its assigned subscription
    Then the complete telemetry response has error <code>
    Examples:
      | unstable | payload | compression | code |
      | false | gzip OTLP | 1 | 87 |
      | false | uncompressed OTLP | 0 | 87 |
      | false | empty bytes | 1 | 0 |
      | false | gzip OTLP | 9 | 76 |
      | false | gzip OTLP | 12 | 76 |
      | false | gzip OTLP | -1 | 76 |
      | false | 1025 uncompressed bytes | 0 | 118 |
      | false | 65536 zero bytes compressed with gzip | 1 | 87 |
      | false | gzip bytes that are not OTLP | 1 | 87 |
      | false | bytes that are not gzip | 1 | 87 |
      | true | gzip OTLP | 1 | 87 |
      | true | uncompressed OTLP | 0 | 87 |
      | true | empty bytes | 1 | 0 |
      | true | gzip OTLP | 9 | 76 |
      | true | gzip OTLP | 12 | 76 |
      | true | gzip OTLP | -1 | 76 |
      | true | 1025 uncompressed bytes | 0 | 118 |
      | true | 65536 zero bytes compressed with gzip | 1 | 118 |
      | true | gzip bytes that are not OTLP | 1 | 87 |
      | true | bytes that are not gzip | 1 | 87 |
