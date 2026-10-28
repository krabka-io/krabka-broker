Feature: tests break glass two person rule

  Scenario: a proposal two approvals and the transition they authorize
    Then break glass two person rule: a proposal two approvals and the transition they authorize

  Scenario: one principal cannot stand in for two
    Then break glass two person rule: one principal cannot stand in for two

  Scenario: the proposer cannot approve their own proposal
    Then break glass two person rule: the proposer cannot approve their own proposal

  Scenario: a principal outside the approver set is refused
    Then break glass two person rule: a principal outside the approver set is refused

  Scenario: an approval joins to the login that made it
    Then break glass two person rule: an approval joins to the login that made it
