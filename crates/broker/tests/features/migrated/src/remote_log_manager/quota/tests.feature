Feature: Remote-log quotas follow Kafka sampled rates and retain history across updates

  Scenario: An idle quota allows an operation immediately
    Given a remote-log quota permits 1000 bytes per second with Kafka sample windows
    When the manager samples the remote-log throttle at millisecond 0
    Then the remote-log throttle is 0 milliseconds

  Scenario Outline: A burst decays to the allowed rate as the window widens
    Given a remote-log quota permits 1000 bytes per second with Kafka sample windows
    When the manager records 2000 remote-log bytes at millisecond 0
    And the manager samples the remote-log throttle at millisecond <at>
    Then the remote-log throttle is <wait> milliseconds

    Examples:
      | at | wait |
      | 0 | 1000 |
      | 1500 | 500 |
      | 2000 | 0 |

  Scenario: A sample expires exactly at the end of the whole window
    Given a remote-log quota permits 1 bytes per second with Kafka sample windows
    When the manager records 1000000 remote-log bytes at millisecond 0
    And the manager samples the remote-log throttle at millisecond 10999
    Then the remote-log throttle is 999989001 milliseconds
    When the manager samples the remote-log throttle at millisecond 11000
    Then the remote-log throttle is 0 milliseconds

  Scenario: Later samples add to the measured byte rate
    Given a remote-log quota permits 1000 bytes per second with Kafka sample windows
    When the manager records 1000 remote-log bytes at millisecond 0
    And the manager records 1000 remote-log bytes at millisecond 1000
    And the manager records 2000 remote-log bytes at millisecond 2000
    And the manager samples the remote-log throttle at millisecond 2000
    Then the remote-log throttle is 2000 milliseconds

  Scenario: Raising the bound releases the previously recorded burst
    Given a remote-log quota permits 1000 bytes per second with Kafka sample windows
    When the manager records 2000 remote-log bytes at millisecond 0
    And the manager samples the remote-log throttle at millisecond 0
    Then the remote-log throttle is 1000 milliseconds
    When the remote-log byte-rate bound is changed to 4000
    And the manager samples the remote-log throttle at millisecond 0
    Then the remote-log throttle is 0 milliseconds

  Scenario: Lowering an unlimited bound throttles existing history
    Given a remote-log quota permits 9223372036854775807 bytes per second with Kafka sample windows
    When the manager records 2000 remote-log bytes at millisecond 0
    And the manager samples the remote-log throttle at millisecond 0
    Then the remote-log throttle is 0 milliseconds
    When the remote-log byte-rate bound is changed to 500
    And the manager samples the remote-log throttle at millisecond 0
    Then the remote-log throttle is 3000 milliseconds

  Scenario: Acquisition records only the bytes it admits
    Given a remote-log quota permits 1000 bytes per second with Kafka sample windows
    When the manager tries to acquire 3000 remote-log bytes at millisecond 0
    Then the remote-log acquisition is admitted
    When the manager tries to acquire 1 remote-log bytes at millisecond 0
    Then the remote-log acquisition is refused for 2000 milliseconds
    When the manager samples the remote-log throttle at millisecond 0
    Then the remote-log throttle is 2000 milliseconds
