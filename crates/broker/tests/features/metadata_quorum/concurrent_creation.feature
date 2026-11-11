Feature: Concurrent metadata changes

  Rule: Concurrent topic creations have exactly one winner
    Scenario: Concurrent creation through three voters creates one topic
      Given a three-broker metadata quorum has elected a controller
      And every voter has registered all three brokers
      When clients concurrently create topic race through all three voters
      Then the race returns one success plus two TOPIC_ALREADY_EXISTS refusals
