Feature: tests acl handlers produce fetch

  Scenario: produce denied without topic acl
    Then acl handlers produce fetch: produce denied without topic acl

  Scenario: produce allowed with topic write acl
    Then acl handlers produce fetch: produce allowed with topic write acl

  Scenario: fetch denied without topic read acl
    Then acl handlers produce fetch: fetch denied without topic read acl
