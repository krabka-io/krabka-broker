Feature: Bounded graceful KRaft shutdown
  Shutdown waits at most Kafka's five seconds, or two and a half fetch timeouts
  when a shorter configured timeout lets the successor election finish sooner.

  Scenario Outline: The successor-election window is capped at five seconds
    Given a KRaft fetch timeout of <timeout_ms> ms
    When the controller chooses its graceful shutdown window
    Then the graceful shutdown window is <window_ms> ms

    Examples:
      | timeout_ms | window_ms |
      | 500        | 1250      |
      | 2000       | 5000      |
      | 20000      | 5000      |
