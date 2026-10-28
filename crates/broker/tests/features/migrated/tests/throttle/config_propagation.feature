Feature: Administrators configure replication throttles through committed metadata

  Background:
    Given an authenticated replication administrator

  Scenario: broker scoped alter persists in image
    When the administrator sets the broker leader replication rate to 2048 bytes per second
    Then the throttle alteration succeeds
    And the committed broker image reports a leader replication rate of 2048 bytes per second

  Scenario: topic throttle config propagates
    Given an administered topic foo with partition 0
    When the administrator throttles foo partition 0 on brokers 1 and 2
    Then the throttle alteration succeeds
    And the committed topic throttle applies to brokers 1 and 2 and excludes broker 3
