Feature: tests group protocol negotiation

  Scenario: empty intersection returns inconsistent group protocol
    Then group protocol negotiation: empty intersection returns inconsistent group protocol

  Scenario: vote picks cooperative when majority
    Then group protocol negotiation: vote picks cooperative when majority

  Scenario: vote ties broken lexicographically
    Then group protocol negotiation: vote ties broken lexicographically

  Scenario: single member picks its first protocol
    Then group protocol negotiation: single member picks its first protocol

  Scenario: protocol type mismatch rejected
    Then group protocol negotiation: protocol type mismatch rejected
