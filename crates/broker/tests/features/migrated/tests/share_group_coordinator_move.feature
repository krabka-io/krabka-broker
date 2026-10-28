Feature: Share membership epochs are acknowledged only after durable coordinator writes

  Background:
    Given three brokers replicate share-move's offsets partition without shrinking its ISR

  Scenario: a join that does not commit is not answered
    Given the share coordinator has installed the full three-replica offsets ISR
    When an offsets follower crashes while remaining in the share coordinator's ISR
    And member-1 joins the share coordinator at epoch zero
    Then the uncommitted join returns only COORDINATOR_NOT_AVAILABLE without an epoch

  Scenario: a member keeps its epoch when its coordinator stops cleanly
    When member-1 joins the share coordinator at epoch zero
    Then the share join succeeds with a positive member epoch
    When the share coordinator completes a controlled shutdown within thirty seconds
    And member-1 resumes at its acknowledged epoch through the replacement coordinator
    Then the recovered member succeeds with the same identity and an epoch at least as new
