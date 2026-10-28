Feature: A metadata quorum elects controllers and serializes client changes

  Background:
    Given three brokers share a static metadata voter set

  Scenario: three node cluster elects leader
    When all metadata voters observe their elected controller
    Then every surviving voter agrees on one nonzero controller

  Scenario: create topic on any node propagates
    Given all three metadata voters have observed a controller leader
    When a client creates topic prop with 3 partitions through voter index 0
    Then the latest topic creation succeeds
    When a client reads metadata through voter index 2 after prop reaches its image
    Then voter index 2 reports topic prop in client metadata

  Scenario: leader kill recovers
    Given all three metadata voters have observed a controller leader
    When the elected metadata controller is shut down
    And the surviving voters elect a replacement controller
    Then every surviving voter agrees on one nonzero controller
    When a client creates topic post-kill with 1 partitions through voter index 0
    Then the latest topic creation succeeds

  Scenario: follower forwards create topic
    Given all three metadata voters have observed a controller leader
    When a client creates topic via-follower through a metadata follower
    Then the latest topic creation succeeds

  Scenario: concurrent topic creates one wins
    Given all three metadata voters have observed a controller leader
    And every voter has registered all three brokers
    When clients concurrently create topic race through all three voters
    Then exactly one race creation succeeds and the other two report TOPIC_ALREADY_EXISTS
