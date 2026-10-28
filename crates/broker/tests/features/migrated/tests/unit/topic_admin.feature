Feature: Kafka topic administration
  Scenario: create then delete topic round trip
    Given a single broker for topic administration
    And topic definition alpha has 2 partitions and replication factor 1
    When the administrator creates the defined topic
    Then creation returns one topic with 2 partitions
    When the administrator deletes the defined topic
    Then deletion returns one successful topic response

  Scenario: create topic with zero partitions errors
    Given a single broker for topic administration
    And topic definition zero has 0 partitions and replication factor 1
    When the administrator creates the defined topic
    Then topic creation returns Kafka error 37

  Scenario: duplicate create returns topic already exists
    Given a single broker for topic administration
    And topic definition dup has 1 partitions and replication factor 1
    And the topic definition already exists
    When the administrator creates the defined topic
    Then topic creation returns Kafka error 36

  Scenario: create topics rf too high returns invalid replication factor
    Given a single broker for topic administration
    And topic definition boom has 1 partitions and replication factor 5
    When the administrator creates the defined topic
    Then topic creation returns Kafka error 38
