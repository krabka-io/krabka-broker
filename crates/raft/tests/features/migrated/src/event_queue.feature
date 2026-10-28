Feature: Kafka controller event queue measurements
  Queue delay includes busy and idle time before a command starts. Timer events
  have no queue delay. A command whose caller has left still runs and counts as
  both started and timed out.

  Scenario: Timer turns do not record queue delay
    Given an event queue with a timer after 9 ms of work and 40 ms idle
    When the event queue runs the next turn for 3 ms
    Then queue time is unrecorded ms, processing is 3 ms, started count is 1, timed-out count is 0, and idle time is 40 ms

  Scenario: Queued commands include the preceding busy turn in queue delay
    Given an event queue with a waiting-caller-command after 25 ms of work and 0 ms idle
    When the event queue runs the next turn for 7 ms
    Then queue time is 25 ms, processing is 7 ms, started count is 1, timed-out count is 0, and idle time is 0 ms

  Scenario: An abandoned caller is counted as timed out while its command still runs
    Given an event queue with a abandoned-caller-command after 30000 ms of work and 5 ms idle
    When the event queue runs the next turn for 1 ms
    Then queue time is 30005 ms, processing is 1 ms, started count is 1, timed-out count is 1, and idle time is 5 ms
