Feature: Pinned librdkafka clients negotiate versions and complete classic and consumer group protocols

  Scenario: round trip group join and api versions with kcat
    Given a host broker has client metrics enabled false
    And these pinned clients have separate RF1 single-partition topics and consumer groups
      | image                                         | program | library           | version_marker    | topic                    | group              | payload         |
      | mirror.gcr.io/edenhill/kcat:1.7.1               | kcat    | librdkafka 1.8.2  | librdkafka 1.8.2  | librdkafka-conformance-0  | librdkafka-group-0 | hello-from-kcat |
      | docker.io/krabka-io/kcat:librdkafka-2.15.0      | kcat    | librdkafka 2.15.0 | librdkafka 2.15.0 | librdkafka-conformance-1  | librdkafka-group-1 | hello-from-kcat |
    Then every client version banner contains its declared linked-library marker
    When each client requests topic metadata with api.version.request enabled and protocol tracing
    Then every metadata trace contains ApiVersionRequest and ApiVersionResponse
    When each client produces its declared payload to partition 0
    And each client consumes one record through a classic group from earliest with auto commit and auto offset store disabled
    Then each consumer prints exactly its declared payload followed by one newline and its trace contains JoinGroup

  Scenario: next gen group topic ids and telemetry with librdkafka 2x
    Given a host broker has client metrics enabled true
    And these pinned clients have separate RF1 single-partition topics and consumer groups
      | image                                    | program | library           | version_marker    | topic                   | group                        | payload                 |
      | docker.io/krabka-io/kcat:librdkafka-2.15.0 | kcat    | librdkafka 2.15.0 | librdkafka 2.15.0 | librdkafka-2x-next-gen   | librdkafka-2x-consumer-group   | hello-from-librdkafka-2x |
    Then every client version banner contains its declared linked-library marker
    And the sole client is the pinned librdkafka 2.15.0 client
    When the modern client produces its payload with idempotence, metrics push and protocol tracing enabled
    Then the producer trace sends and receives InitProducerId and GetTelemetrySubscriptions
    When the modern client consumes one record using group.protocol consumer from earliest with auto commit disabled
    Then the payload matches exactly, the trace sends and receives ConsumerGroupHeartbeat without JoinGroup or SyncGroup, and the highest Fetch version is at least 13
