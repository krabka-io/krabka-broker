@docker
Feature: Operators inspect JBOD storage through stock Kafka tools
  Scenario: Operators see both configured JBOD directories
    Given six partitions are materialized across a broker configured with two log directories
    When the operator describes broker one with kafka log dirs
    Then the report contains both canonical log directories and the topic partitions
