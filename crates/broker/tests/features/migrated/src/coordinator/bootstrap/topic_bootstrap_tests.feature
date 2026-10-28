Feature: Coordinator topic bootstrap uses Kafka configurations
  Scenario: Coordinator internal topics use Kafka configuration maps
    Given a running broker configures transaction and Share state minimum ISR to Kafka default two
    When the broker creates offsets transaction state and Share state topics through CreateTopics version 7
    Then all coordinator topics are created and each complete configuration map equals Kafka defaults
