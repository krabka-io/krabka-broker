Feature: tests quorum

  Scenario: three node cluster elects leader
    Then quorum: three node cluster elects leader

  Scenario: create topic on any node propagates
    Then quorum: create topic on any node propagates

  Scenario: leader kill recovers
    Then quorum: leader kill recovers

  Scenario: follower forwards create topic
    Then quorum: follower forwards create topic

  Scenario: concurrent topic creates one wins
    Then quorum: concurrent topic creates one wins
