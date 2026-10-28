Feature: Client telemetry validation and time windows
  # id_xor mutates the actual assigned subscription id; zero sends it unchanged.

  Scenario: Push validation order and timestamps across one client session
    Given a client gets all metrics at time zero with interval 60000 milliseconds
    When the client pushes at 1000 milliseconds with {"id_xor":0,"compression_supported":true,"payload_len":10,"terminating":false}
    Then the telemetry push returns Kafka code 0
    When the client pushes at 2000 milliseconds with {"id_xor":0,"compression_supported":true,"payload_len":10,"terminating":false}
    Then the telemetry push returns Kafka code 89
    When the client pushes at 3000 milliseconds with {"id_xor":21845,"compression_supported":true,"payload_len":10,"terminating":false}
    Then the telemetry push returns Kafka code 89
    When the client pushes at 61000 milliseconds with {"id_xor":21845,"compression_supported":true,"payload_len":10,"terminating":false}
    Then the telemetry push returns Kafka code 117
    When the client pushes at 62000 milliseconds with {"id_xor":0,"compression_supported":true,"payload_len":10,"terminating":false}
    Then the telemetry push returns Kafka code 89
    When the client pushes at 122000 milliseconds with {"id_xor":0,"compression_supported":false,"payload_len":4096,"terminating":false}
    Then the telemetry push returns Kafka code 76
    When the client pushes at 183000 milliseconds with {"id_xor":0,"compression_supported":true,"payload_len":1025,"terminating":false}
    Then the telemetry push returns Kafka code 118
    When the client pushes at 244000 milliseconds with {"id_xor":0,"compression_supported":true,"payload_len":1024,"terminating":false}
    Then the telemetry push returns Kafka code 0
    When the client pushes at 245000 milliseconds with {"id_xor":0,"compression_supported":true,"payload_len":10,"terminating":true}
    Then the telemetry push returns Kafka code 0
    When the client pushes at 400000 milliseconds with {"id_xor":0,"compression_supported":true,"payload_len":10,"terminating":false}
    Then the telemetry push returns Kafka code 42

  Scenario: Get throttling recovers after errors that request a new subscription
    Given a client gets all metrics at time zero with interval 100 milliseconds
    When the client requests its subscription at 99 milliseconds
    Then the subscription request returns Kafka code 89
    When the client requests its subscription at 100 milliseconds
    Then the subscription request returns Kafka code 0
    When the client pushes at 150 milliseconds with {"id_xor":0,"compression_supported":true,"payload_len":10,"terminating":false}
    Then the telemetry push returns Kafka code 0
    When the client requests its subscription at 200 milliseconds
    Then the subscription request returns Kafka code 89
    When the client pushes at 260 milliseconds with {"id_xor":1,"compression_supported":true,"payload_len":10,"terminating":false}
    Then the telemetry push returns Kafka code 117
    When the client requests its subscription at 261 milliseconds
    Then the subscription request returns Kafka code 0
    When the client requests its subscription at 262 milliseconds
    Then the subscription request returns Kafka code 89
    When the client pushes at 400 milliseconds with {"id_xor":0,"compression_supported":false,"payload_len":10,"terminating":false}
    Then the telemetry push returns Kafka code 76
    When the client requests its subscription at 401 milliseconds
    Then the subscription request returns Kafka code 0
