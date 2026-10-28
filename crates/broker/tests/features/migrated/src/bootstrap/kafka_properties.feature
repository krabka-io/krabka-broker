Feature: Broker bootstrap reads Kafka-produced metadata property files

  Background:
    Given a fresh unformatted broker storage root


  Scenario: Read Java 21 and later
    Given the meta log directory contains meta.properties:
      """
      #
      #Thu Feb 29 12:34:56 UTC 2024
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      directory.id=U-hCdTzaSPyFB3swwc7Qdw
      node.id=2
      version=1
      """
    When the broker reads the metadata properties and directory UUID
    Then the Kafka properties identify node 2, cluster AQIDBAUGBwgJCgsMDQ4PEA and directory U-hCdTzaSPyFB3swwc7Qdw

  Scenario: Read Java 17 hash order
    Given the meta log directory contains meta.properties:
      """
      #
      #Thu Feb 29 12:34:56 UTC 2024
      node.id=2
      version=1
      directory.id=U-hCdTzaSPyFB3swwc7Qdw
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      """
    When the broker reads the metadata properties and directory UUID
    Then the Kafka properties identify node 2, cluster AQIDBAUGBwgJCgsMDQ4PEA and directory U-hCdTzaSPyFB3swwc7Qdw

  Scenario: Read Handwritten without comments
    Given the meta log directory contains meta.properties:
      """
      version=1
      cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
      node.id=2
      directory.id=U-hCdTzaSPyFB3swwc7Qdw
      """
    When the broker reads the metadata properties and directory UUID
    Then the Kafka properties identify node 2, cluster AQIDBAUGBwgJCgsMDQ4PEA and directory U-hCdTzaSPyFB3swwc7Qdw
