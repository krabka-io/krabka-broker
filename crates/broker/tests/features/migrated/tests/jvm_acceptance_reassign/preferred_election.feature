Feature: Preferred leader election through Kafka JVM tools
  Scenario: jvm kafka leader election preferred
    Given topic krabka-elect-preferred-itest has preferred replica 1 alive in ISR while metadata names replica 2 as leader
    When the SASL JVM tool requests preferred election of partition 0
    Then the JVM command succeeds and broker 2 observes preferred replica 1 leading

  Scenario: election selectors report election not needed as apache kafka does
    Given Kafka and Krabka expose the healthy single-replica election partitions
      | topic                         | partition |
      | krabka-elect-selectors-itest   | 0         |
      | krabka-elect-selectors-itest   | 1         |
      | krabka-elect-selectors-itest   | 2         |
    When both JVM clients elect by JSON partition list and by all-topic-partitions
    Then each JSON-selected partition reports AlreadyElected and all-topic-partitions agrees between brokers

  Scenario: a preferred replica outside the isr reports code 80
    Given topic krabka-elect-unavailable-itest is led by replica 1 with its preferred replica offline and outside ISR
    When the JVM elects the unavailable preferred replica through the JSON selector
    Then the tool reports exception org.apache.kafka.common.errors.PreferredLeaderNotAvailableException for the partition
