Feature: Bounded graceful KRaft shutdown
  Shutdown waits at most Kafka's five seconds, or two and a half fetch timeouts
  when a shorter configured timeout lets the successor election finish sooner.

  Scenario Outline: The successor-election window is capped at five seconds
    Given a KRaft fetch timeout of <timeout_ms> ms
    When the controller chooses its graceful shutdown window
    Then the graceful shutdown window is <window_ms> ms

    Examples:
      | case                                 | timeout_ms | window_ms |
      | a short timeout shortens the window  | 500        | 1250      |
      | exactly the five-second cap          | 2000       | 5000      |
      | a long timeout is capped             | 20000      | 5000      |
