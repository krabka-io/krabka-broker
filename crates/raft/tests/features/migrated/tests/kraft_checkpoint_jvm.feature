Feature: Engine checkpoints are Kafka JVM compatible
  Scenario: Kafka JVM tools decode and replay the engine checkpoint
    Given a metadata image contains broker registration, topic partitions, configuration, finalized features and private control records
    When Kafka dump-log decodes the engine snapshot checkpoint
    Then all Kafka record families decode with valid CRCs, identities, partition epochs and the committed timestamp
    When Kafka metadata-shell replays the same checkpoint
    Then Kafka metadata-shell successfully lists the orders topic
