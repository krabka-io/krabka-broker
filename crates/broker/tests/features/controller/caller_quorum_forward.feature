Feature: Controller quorum forwarding preserves caller authorization

  Scenario: Forwarding preserves the Describe-authorized caller
    Given a two-voter SASL PLAIN cluster grants ClusterAction to everyone and Describe only to alice
    When alice sends DescribeQuorum to the elected leader's follower
    Then the forwarded quorum response succeeds at both levels and identifies the elected leader
