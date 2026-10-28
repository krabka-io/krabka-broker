Feature: Incremental fetch sessions cache subscriptions and enforce their lifecycle epochs
  Background:
    Given a fresh broker is ready for fetch session requests

  Scenario: new session then incremental filters unchanged partitions
    Given topic t contains 3 initially empty partitions
    When a full fetch opens a session for 3 partitions with wait 100 milliseconds
    Then the broker allocates a positive fetch session id
    And the fetch response has error 0 and session id allocated
    And the full fetch response includes one topic and all three partitions
    When an immediate incremental fetch uses the allocated id at epoch 1
    Then the fetch response has error 0 and session id allocated
    And the fetch response contains no topic rows
    When the producer appends 5 records to t partition 0
    And an incremental fetch uses the allocated id at epoch 2 and waits 200 milliseconds
    Then the fetch response has error 0 and session id allocated
    And only partition 0 is returned with exactly five records

  Scenario: forgotten topics drop partitions from subscription
    Given topic t contains 3 initially empty partitions
    When a full fetch opens a session for 3 partitions with wait 100 milliseconds
    Then the broker allocates a positive fetch session id
    When the session forgets t partition 1 at epoch 1
    Then the fetch response has error 0 and session id allocated
    When the producer appends 4 records to t partition 1
    And the producer appends 2 records to t partition 2
    And an incremental fetch uses the allocated id at epoch 2 and waits 200 milliseconds
    Then the fetch response has error 0 and session id allocated
    And only partition 2 reappears after writes to forgotten partition 1 and retained partition 2

  Scenario: unknown session id returns not found
    When an immediate incremental fetch uses unknown id 999999
    Then the fetch response has error 70 and session id 0
    And the fetch response contains no topic rows

  Scenario: stale session epoch returns invalid epoch
    Given topic t contains 1 initially empty partitions
    When a full fetch opens a session for 1 partitions with wait 0 milliseconds
    Then the broker allocates a positive fetch session id
    When the allocated session is fetched with stale epoch 99
    Then the fetch response has error 71 and session id 0
    And the fetch response contains no topic rows

  Scenario: close session drops cache entry
    Given topic t contains 1 initially empty partitions
    When a full fetch opens a session for 1 partitions with wait 0 milliseconds
    Then the broker allocates a positive fetch session id
    When the allocated session is closed with epoch -1
    Then the fetch response has error 0 and session id 0
    When an incremental fetch reuses the closed session id
    Then the fetch response has error 70 and session id 0

  Scenario: sessionless zero id with stray epoch is session id not found
    When a sessionless fetch sends zero id with stray epoch 7
    Then the fetch response has error 70 and session id 0

  Scenario: sessionless full fetch round trip
    Given topic t contains 1 initially empty partitions
    When the producer appends 2 records to t partition 0
    And a full sessionless fetch uses id 0 and epoch -1 with wait 100 milliseconds
    Then the fetch response has error 0 and session id 0
    And the sessionless fetch returns one topic with exactly two records
