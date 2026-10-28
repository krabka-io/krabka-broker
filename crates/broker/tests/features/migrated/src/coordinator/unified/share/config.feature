Feature: Share group default configuration
  Scenario: An unconfigured Share group inherits Kafka defaults
    Given a Share group has no dynamic configuration overrides
    When the coordinator resolves the Share group configuration
    Then every Share setting equals the Kafka 4.3.1 default
