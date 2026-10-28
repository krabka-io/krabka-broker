@docker
Feature: Freeze scope is visible and read only through Kafka configuration tools
  Scenario: Stock config tools can read a freeze but cannot mutate it
    Given a literal write freeze protects the configuration inspection topic
    When the operator describes the frozen topic configuration
    Then the read succeeds and exposes the synthesized freeze scope
    When the operator tries setting clearing and deleting the write freeze config key
    Then all three config writes fail with the invalid configuration exception and freeze command guidance
    When the operator reads the freeze scope after the rejected writes
    Then the original freeze scope remains unchanged
