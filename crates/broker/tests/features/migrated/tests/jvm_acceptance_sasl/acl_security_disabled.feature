@docker
Feature: ACL administration reports when authorization is disabled
  Scenario: An ACL listing distinguishes disabled authorization from an empty policy
    Given krabka and an Apache Kafka reference both have no configured authorizer
    When the operator lists ACLs on both unsecured brokers
    Then both commands reject ACL administration without printing any bindings
    And the security disabled exception class and message match Apache Kafka
