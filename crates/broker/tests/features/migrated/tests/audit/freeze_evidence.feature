Feature: tests audit freeze evidence

  Scenario: every kfc9 phase writes one chained audit record
    Then audit freeze evidence: every kfc9 phase writes one chained audit record

  Scenario: the proposal id joins each approval to the transition it authorized
    Then audit freeze evidence: the proposal id joins each approval to the transition it authorized

  Scenario: a signed freeze reverifies from the audit topic with no metadata image
    Then audit freeze evidence: a signed freeze reverifies from the audit topic with no metadata image
