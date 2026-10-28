Feature: Share acquisition locks expire, renew, and enforce delivery attempt limits

  Scenario: lock timeout redelivers
    Given one share record has a 200-millisecond lock and delivery attempt limit default
    When the lock-test member acquires its single record without acknowledging
    Then the lock-test fetch acquires one record at delivery count 1
    When the background sweeper expires the unacknowledged acquisition
    And the lock-test member fetches at session epoch 1
    Then the lock-test fetch acquires one record at delivery count 2

  Scenario: delivery limit archives
    Given one share record has a 150-millisecond lock and delivery attempt limit 2
    When the lock-test member acquires its single record without acknowledging
    Then the lock-test fetch acquires one record at delivery count 1
    When the background sweeper expires the unacknowledged acquisition
    And the lock-test member fetches at session epoch 1
    Then the lock-test fetch acquires one record at delivery count 2
    When the background sweeper expires the unacknowledged acquisition
    And the lock-test member fetches at session epoch 2
    Then the lock-test fetch acquires no records

  Scenario: renew extends lock not redelivered
    Given one share record has a 500-millisecond lock and delivery attempt limit default
    When the lock-test member acquires its single record without acknowledging
    Then the lock-test fetch acquires one record at delivery count 1
    When the member renews the acquisition after 200 milliseconds
    Then the acquisition renewal succeeds
    When the clock reaches 600 milliseconds after the original acquisition
    And the lock-test member fetches at session epoch 2
    Then the lock-test fetch acquires no records

  Scenario: no renew redelivers after lock expiry
    Given one share record has a 500-millisecond lock and delivery attempt limit default
    When the lock-test member acquires its single record without acknowledging
    Then the lock-test fetch acquires one record at delivery count 1
    When 800 milliseconds elapse without a renewal
    And the lock-test member fetches at session epoch 1
    Then the lock-test fetch acquires one record at delivery count 2
