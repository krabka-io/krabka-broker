Feature: Failover qualification against Kafka
  Scenario: Producer throughput must recover as quickly as Kafka
    Given Krabka's producer rate is 8500 per second at 6 seconds and recovers at 8 seconds
    When the recorded failover runs are checked against the Kafka baseline
    Then the failover gate reports Krabka producer rate recovery 4s is slower than kafka 2s

  Scenario: Consumer throughput must recover as quickly as Kafka
    Given Krabka's consumer rate is 8000 per second at 6 seconds and recovers at 8 seconds
    When the recorded failover runs are checked against the Kafka baseline
    Then the failover gate reports Krabka consumer rate recovery 4s is slower than kafka 2s

  Scenario Outline: Failover may not lose more messages or spike latency above Kafka
    Given Krabka drops <dropped> messages and reaches <latency> milliseconds maximum latency
    When the recorded failover runs are checked against the Kafka baseline
    Then the failover gate reports <violation>

    Examples:
      | case                          | dropped | latency | violation                                           |
      | messages lost during failover | 5       | 42      | Krabka dropped 5 messages vs kafka 0                |
      | a latency spike above Kafka   | 0       | 90      | Krabka latency spike 90ms is higher than kafka 42ms |
