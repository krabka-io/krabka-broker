Feature: Engine checkpoints are Kafka JVM compatible

  @docker
  Scenario: Kafka dump-log decodes the engine checkpoint
    Given a metadata image contains broker registration, topic partitions, configuration, finalized features and private control records
    When Kafka dump-log decodes the engine snapshot checkpoint
    Then all Kafka record families decode with valid CRCs, identities, partition epochs and the committed timestamp

  @docker
  Scenario: Kafka metadata-shell replays the engine checkpoint
    Given a metadata image contains broker registration, topic partitions, configuration, finalized features and private control records
    And Kafka dump-log has decoded the engine snapshot checkpoint
    When Kafka metadata-shell replays the same checkpoint
    Then Kafka metadata-shell successfully lists the orders topic
