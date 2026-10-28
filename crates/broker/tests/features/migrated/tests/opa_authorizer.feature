Feature: tests opa authorizer

  Scenario: produce blocked by opa returns topic authorization failed
    Then opa authorizer: produce blocked by opa returns topic authorization failed

  Scenario: produce allowed by opa succeeds
    Then opa authorizer: produce allowed by opa succeeds
