Feature: JVM health filters isolate partitions on a failed log directory

  Scenario: both health filters print a partition on a failed log dir
    Given Kafka 4.3.1 hosts four RF1 partitions across two directories with its second directory on a 4-MiB tmpfs
    When 20000 one-KiB records fill Kafka's second directory and kafka-topics reads both health filters
    Then Kafka's unavailable and under-replicated filters name exactly the failed-disk partitions, which alone lose leader and ISR
    Given krabka hosts four RF1 partitions assigned across two online directories
    When krabka's second directory fails and kafka-topics reads both health filters
    Then krabka's unavailable and under-replicated filters name exactly the failed-disk partitions, which alone lose leader and ISR
