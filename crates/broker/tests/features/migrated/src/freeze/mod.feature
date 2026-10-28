Feature: Freeze scope vocabulary
  Error messages, audits, and break-glass targets use the same lowercase
  pattern names that Kafka ACL operators already know.

  Scenario Outline: Freeze pattern names use Kafka ACL vocabulary
    Given a freeze scope uses Kafka ACL pattern type <pattern>
    When the scope's pattern type is rendered for an operator
    Then the operator-facing pattern type is <name>

    Examples:
      | pattern  | name     |
      | Literal  | literal  |
      | Prefixed | prefixed |
