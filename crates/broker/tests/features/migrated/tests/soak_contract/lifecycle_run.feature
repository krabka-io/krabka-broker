Feature: Lifecycle checkpoints verify that configuration remains applied

  Background:
    Given the lifecycle set min.insync.replicas to 2

  Scenario: tests: a configuration holds what was set
    Given DescribeConfigs reports min.insync.replicas as 2
    When the lifecycle checkpoint compares expected and described topic configuration
    Then the topic configuration has no mismatches

  Scenario: a changed setting reports its actual value
    Given DescribeConfigs reports min.insync.replicas as 1
    When the lifecycle checkpoint compares expected and described topic configuration
    Then the topic configuration mismatch is
      """
      t min.insync.replicas is Some("1"), not "2"
      """

  Scenario: a hidden setting reports its absent value
    Given DescribeConfigs reports min.insync.replicas as hidden
    When the lifecycle checkpoint compares expected and described topic configuration
    Then the topic configuration mismatch is
      """
      t min.insync.replicas is None, not "2"
      """

  Scenario: an omitted setting reports that it was not described
    Given DescribeConfigs reports min.insync.replicas as missing
    When the lifecycle checkpoint compares expected and described topic configuration
    Then the topic configuration mismatch is
      """
      t min.insync.replicas is not described; it was set to "2"
      """
