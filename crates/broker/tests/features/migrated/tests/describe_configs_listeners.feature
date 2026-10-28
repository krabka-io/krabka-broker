Feature: Listener configuration described through role-specific endpoints

  Scenario Outline: each role reports the listener keys through the listener a client uses
    Given a node configured with <roles> process roles
    When DescribeConfigs requests listener keys through every endpoint that serves the node
    Then every endpoint reports the role-specific listener values sources synonyms and mutability

    Examples:
      | roles           |
      | controller-only |
      | broker-only     |
      | combined        |
