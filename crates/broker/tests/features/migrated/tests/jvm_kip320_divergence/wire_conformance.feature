Feature: KIP-320 wire compatibility with the official JVM consumer

  Scenario: Official JVM Fetch and epoch validation agree with the Rust epoch boundary
    Given a single Krabka topic krabka-kip320-wire with epoch-zero records ["e0-0","e0-1","e0-2","e0-3","e0-4"]
    When leadership advances one epoch and records ["e1-0","e1-1","e1-2","e1-3","e1-4"] are produced
    And the Rust client queries partition 0 epoch 0 without current-epoch fencing
    Then the Rust epoch answer has Kafka code 0 and the recorded epoch-zero end offset
    When the official JVM consumer validates positions and drains both Fetch v12 epochs
    Then the Java consumer reports KIP320PROBE OK without RecordDeserializationException
    And the wire conformance broker and Java container are shut down
