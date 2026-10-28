Feature: JVM compatibility of native bootstrap checkpoints

  Scenario: JVM Kafka reads a native bootstrap checkpoint
    Given a bootstrap checkpoint with feature levels [["metadata.version",25],["group.version",1],["transaction.version",2]]
    When Kafka 4.0.0 dumps the bootstrap.checkpoint with its cluster metadata decoder
    Then the JVM decoder succeeds with output fragments [["SnapshotHeader",true],["FEATURE_LEVEL_RECORD",true],["metadata.version",true],["SnapshotFooter",true],["isvalid: false",false]]
