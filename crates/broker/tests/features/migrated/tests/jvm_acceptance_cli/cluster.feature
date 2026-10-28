@docker
Feature: Cluster identity discovery through Kafka administration tools
  Scenario: The stock Kafka tool discovers the broker's immutable cluster identity
    Given a host broker with its immutable cluster identity is reachable by Kafka tools
    When the Kafka 7.5 cluster-id subcommand queries the broker
    Then the tool identifies the broker's current cluster id
