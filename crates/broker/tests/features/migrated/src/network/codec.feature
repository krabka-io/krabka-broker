Feature: Kafka network frame limits
  The codec accepts requests beyond Tokio's smaller default while enforcing
  Kafka's 100-MiB ceiling from the length header before allocating a payload.

  Scenario: The network codec is configured for Kafka's 100-MiB ceiling
    Given the network codec uses Kafka's default maximum frame size
    When the network codec's configured frame limit is read
    Then the configured frame limit is 100 MiB or 104857600 bytes

  Scenario: A nine-MiB payload is accepted intact
    Given the network codec uses Kafka's default maximum frame size
    And a complete Kafka frame carries 9437184 payload bytes filled with 165
    When the network codec decodes the received Kafka frame bytes
    Then the complete payload is delivered unchanged below Kafka's frame limit

  Scenario: A one-byte oversize declaration is rejected from its header
    Given the network codec uses Kafka's default maximum frame size
    And only a Kafka frame header declaring 104857601 payload bytes has arrived
    When the network codec decodes the received Kafka frame bytes
    Then the frame is rejected as too big before its payload arrives
