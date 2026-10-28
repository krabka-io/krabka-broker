@docker
Feature: Stock partition reassignment tools install and clear broker throttles
  Scenario: A stock reassignment installs a visible throttle and verification removes it after completion
    Given three registered SASL brokers host two replicas of topic krabka-throttle-reassign-itest and select a free destination while retaining the first replica
    When the stock reassignment tool executes the selected move with throttle 1024
    Then the throttled reassignment execution succeeds
    When the stock config tool describes broker one's dynamic configuration
    Then the broker config report includes leader.replication.throttled.rate=1024
    When the controller receives an ISR update including the new replica and completes the reassignment
    Then the final replica set contains exactly the retained and new brokers
    When the stock reassignment tool verifies the completed move
    Then verification succeeds and broker one's committed leader throttle is removed
