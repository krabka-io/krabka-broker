Feature: Kafka reads Krabka tiered segment exports
  Scenario: Kafka reads Krabka's offloaded producer segment
    Given a sealed Krabka segment containing an idempotent producer batch
    And the segment and producer snapshot are copied to Kafka local tiered storage
    And Kafka 4.0 tiered-storage JVM tools
    When Kafka reads the offloaded segment and restores its producer state
    Then Kafka accepts the segment bytes indexes and producer snapshot
