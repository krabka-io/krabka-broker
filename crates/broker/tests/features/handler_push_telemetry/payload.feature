Feature: PushTelemetry validates payloads after subscription assignment
  Scenario Outline: Telemetry payload framing compression decoding and size limits follow the selected Kafka version
    Given a telemetry client has obtained a subscription with a 1024-byte limit and unstable API versions <unstable>
    And the telemetry push carries <payload> using compression ID <compression>
    When the client pushes telemetry using its assigned subscription
    Then the complete telemetry response has error <code>
    Examples:
      | case                                                      | unstable | payload                               | compression | code |
      | an unsubscribed metric is an invalid record               | false    | gzip OTLP                             | 1           | 87   |
      | compression ID 0 reads the payload as uncompressed        | false    | uncompressed OTLP                     | 0           | 87   |
      | an empty push is accepted                                 | false    | empty bytes                           | 1           | 0    |
      | compression ID 9 is not masked to gzip                    | false    | gzip OTLP                             | 9           | 76   |
      | compression ID 12 is not masked to zstd                   | false    | gzip OTLP                             | 12          | 76   |
      | a negative compression ID                                 | false    | gzip OTLP                             | -1          | 76   |
      | a payload one byte past the limit                         | false    | 1025 uncompressed bytes               | 0           | 118  |
      | 4.3.1 answers an oversized decompression as invalid       | false    | 65536 zero bytes compressed with gzip | 1           | 87   |
      | a payload that is not OTLP is an invalid record           | false    | gzip bytes that are not OTLP          | 1           | 87   |
      | a payload that does not decompress is an invalid record   | false    | bytes that are not gzip               | 1           | 87   |
      | trunk answers an oversized decompression as too large     | true     | 65536 zero bytes compressed with gzip | 1           | 118  |
      | trunk still answers a payload that is not OTLP as invalid | true     | gzip bytes that are not OTLP          | 1           | 87   |
