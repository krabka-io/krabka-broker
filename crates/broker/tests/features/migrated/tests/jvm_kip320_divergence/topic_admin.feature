Feature: Read only the Kafka topic ISR field

  Scenario: Ignore numeric topic suffixes and non-ISR replica fields
    Given a Kafka topic description Topic: krabka-kip320-3 Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2 Elr: 3
    When the ISR field is parsed
    Then the complete ISR is [1,2]
