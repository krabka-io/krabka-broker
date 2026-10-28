@docker
Feature: Topic administration through Kafka command-line tools
  Scenario: Creating a topic exposes its name and partition count to Kafka tools
    Given a host broker is reachable by the Kafka topic administration tools
    When kafka-topics creates topic described with 2 partitions
    And kafka-topics describes the administered topic
    Then the topic description names described with PartitionCount 2

  Scenario: Increasing topic partitions is visible in a subsequent description
    Given the existing topic krabka-alter-parts-itest has one partition
    When kafka-topics increases the topic to three partitions
    And kafka-topics describes the administered topic
    Then the topic description reports three partitions

  Scenario: Deleting a topic removes it from the Kafka tool listing
    Given a host broker is reachable by the Kafka topic administration tools
    When kafka-topics creates topic deleted-by-cli with 1 partitions
    And kafka-topics lists the current topics
    Then the administered topic appears in the topic listing
    When kafka-topics deletes the administered topic
    And kafka-topics lists the current topics
    Then the administered topic is absent from the topic listing

  Scenario: A compact and delete cleanup list survives creation and both describe tools
    Given a host broker is reachable by the Kafka topic administration tools
    When kafka-topics creates topic krabka-compact-delete-itest with compact and delete cleanup
    And kafka-topics describes the administered topic
    And kafka-configs describes the topic configuration
    Then both administrative tools preserve cleanup.policy as compact,delete
