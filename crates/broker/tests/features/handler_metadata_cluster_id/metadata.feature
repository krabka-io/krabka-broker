Feature: Metadata reports the Kafka cluster identity
  Scenario: Metadata reports the cluster UUID in Kafka representation
    Given the metadata broker cluster UUID contains consecutive bytes 1 through 16
    When a describer requests version 9 cluster metadata
    Then the metadata cluster ID is the Kafka UUID string AQIDBAUGBwgJCgsMDQ4PEA
