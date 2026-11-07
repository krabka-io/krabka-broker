Feature: A broker checkpoints its audit chain at the configured cadence

  The [audit.checkpoint] section sets two triggers, every_n events and
  every_secs seconds. Whichever fires first writes a signed checkpoint over
  the chain head to the audit topic. A checkpoint covers the events chained
  before it.

  Rule: A checkpoint follows every configured number of events

    Scenario Outline: The event count triggers a checkpoint
      Given the broker checkpoints every 3 audit events
      When the broker writes <events> audit events
      Then the checkpoint count on the audit topic is <checkpoints>

      Examples:
        | case                | events | checkpoints |
        | one short           | 2      | 0           |
        | exactly the count   | 3      | 1           |
        | one past the count  | 4      | 1           |
        | twice the count     | 6      | 2           |

    Scenario: A count checkpoint covers the event that reached the count
      Given the broker checkpoints every 3 audit events
      When the broker writes 3 audit events
      Then the last checkpoint signs the chain head at sequence 2

  Rule: An elapsed interval checkpoints only events written since the last checkpoint

    Scenario: The interval checkpoints new events
      Given the broker checkpoints every 60 seconds
      And the broker has written 2 audit events
      When 60 seconds pass
      Then the checkpoint count on the audit topic is 1

    Scenario: The interval writes no checkpoint when no event arrived
      Given the broker checkpoints every 60 seconds
      When 60 seconds pass
      Then the checkpoint count on the audit topic is 0

  Rule: A broker that stops checkpoints its uncommitted events

    Scenario: Shutdown signs the chain head of the last event
      Given the broker checkpoints at a count it does not reach
      And the broker has written 2 audit events
      When the broker stops
      Then the last checkpoint signs the chain head at sequence 1

  Rule: A broker without an audit signing key writes no checkpoints

    Scenario: An unsigned broker leaves its chain unattested
      Given the broker has no audit signing key
      When the broker writes 3 audit events
      Then the checkpoint count on the audit topic is 0
