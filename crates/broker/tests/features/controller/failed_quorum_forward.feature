Feature: Quorum forwarding while the advertised leader has crashed

  Scenario: A dead leader hint produces a typed quorum error
    Given two controller voters have an elected leader and the follower knows its identity
    When the leader crashes and a client immediately sends DescribeQuorum to the surviving follower
    Then the follower returns a typed NOT_LEADER_OR_FOLLOWER response instead of closing the client connection
