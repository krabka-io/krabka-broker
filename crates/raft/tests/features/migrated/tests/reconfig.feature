Feature: Quorum reconfiguration request identity
  Add, remove and update constructors retain the same voter and directory identity.
  Add requests may acknowledge the local append without waiting for commitment.

  Scenario Outline: Voter changes retain exact request identity
    Given quorum voter 2 with its directory identity derived from the same integer
    When a <operation> voter change is constructed
    Then the <operation> change preserves voter ID 2, directory integer 2, and local-append acknowledgement for adds

    Examples:
      | operation |
      | add       |
      | remove    |
      | update    |
