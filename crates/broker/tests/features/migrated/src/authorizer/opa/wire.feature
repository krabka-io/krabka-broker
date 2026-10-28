Feature: src authorizer opa wire

  Scenario Outline: tests: operation str maps kafka names
    Then authorizer opa wire tests: operation str maps kafka names for <example>

    Examples:
      | example          |
      | read             |
      | write            |
      | two phase commit |
