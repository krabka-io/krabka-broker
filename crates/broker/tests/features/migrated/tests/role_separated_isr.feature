Feature: Broker leaders send ISR changes to a controller-only voter

  Scenario: a restarted follower rejoins the isr when the controller is controller only
    Given one controller-only voter manages two broker-only nodes
    When a two-replica rolesep-isr partition is created
    Then the controller commits brokers 2 and 3 in the partition ISR
    When the broker-only follower crashes without controlled shutdown
    Then only the partition leader remains in the controller's ISR
    When the follower restarts using the same broker-only configuration
    Then the controller commits brokers 2 and 3 in the partition ISR
