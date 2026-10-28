Feature: Classic group protocol negotiation uses common candidates ordered votes and protocol types
  Background:
    Given a classic group coordinator is ready

  Scenario: empty intersection returns inconsistent group protocol
    Given consumer members propose these ordered protocol names with empty metadata:
      | client | protocols |
      | member-a | range |
      | member-b | cooperative-sticky |
    When the members join group cg-empty-intersection concurrently on separate connections
    Then the sorted member error codes are exactly 0,23

  Scenario: vote picks cooperative when majority
    Given consumer members propose these ordered protocol names with empty metadata:
      | client | protocols |
      | member-a | cooperative-sticky,range |
      | member-b | cooperative-sticky,range |
      | member-c | range,cooperative-sticky |
    When the members join group cg-vote-cooperative concurrently on separate connections
    Then all 3 members succeed with selected protocol cooperative-sticky

  Scenario: vote ties broken lexicographically
    Given consumer members propose these ordered protocol names with empty metadata:
      | client | protocols |
      | member-a | range,cooperative-sticky |
      | member-b | cooperative-sticky,range |
    When the members join group cg-tie concurrently on separate connections
    Then all 2 members succeed with selected protocol cooperative-sticky

  Scenario: single member picks its first protocol
    Given consumer members propose these ordered protocol names with empty metadata:
      | client | protocols |
      | single-member | range |
    When the members join group cg-single concurrently on separate connections
    Then all 1 members succeed with selected protocol range

  Scenario: protocol type mismatch rejected
    When member-a establishes group cg-type-mismatch with consumer protocol type and range
    Then all 1 members succeed with selected protocol range
    When member-b first joins cg-type-mismatch with stream protocol type and range
    Then the mismatched member receives exactly the error 23 default response without a protocol or member id
