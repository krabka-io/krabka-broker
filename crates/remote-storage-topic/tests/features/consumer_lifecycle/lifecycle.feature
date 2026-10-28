Feature: Metadata consumer lifecycle
  Scenario: Initial connection retries without reassignment
    Given the initial metadata connection fails once before returning client 42
    When the metadata consumer connects without being reassigned
    Then client 42 is returned after exactly two connection attempts

  Scenario Outline: Cancellation interrupts connection and retry waits
    Given metadata connection retries wait one hour
    When the consumer is cancelled while <stage>
    Then connection completion reports cancellation immediately
    Examples:
      | stage                                |
      | its initial connection is pending    |
      | waiting to retry a failed connection |

  Scenario: Assignments track replacement and removal of partition tasks
    Given a metadata consumer has fetch wait 750ms size 2MiB and retry delay 300ms
    When partition 2 is spawned at 7 then 9 and partition 0 is added before removing partition 2
    Then only partition 0 remains assigned and the configured fetch policy is retained
