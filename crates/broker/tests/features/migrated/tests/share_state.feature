Feature: Share-state coordinator discovery and durable persister RPCs

  Scenario: find coordinator share creates topic and returns broker
    Given a fresh broker with no share coordinator lookup yet
    When g1 first looks up a share coordinator for topic UUID 03030303030303030303030303030303 partition zero
    Then the first share coordinator answer contains no node and COORDINATOR_NOT_AVAILABLE
    When g1 retries the same share coordinator lookup until it is available
    Then the share coordinator answer names the running broker's exact listener

  Scenario: persister round trip
    Given a fresh broker with no share coordinator lookup yet
    And g1's topic round-trip partition zero is initialized at state epoch 0 and start zero
    When the persister writes state epoch 0, start 5, complete count 2, and acquired batch 5..9
    Then the share-state write reports NONE
    When the full state of g1's partition is read
    Then the full share state has start five and includes acquired offsets five through nine
    When the summary of g1's partition is read
    Then the summary reports state epoch zero, start five, and complete count two
    When g1's share-state partition is deleted
    Then the share-state deletion succeeds
    When the full state of g1's partition is read
    Then reading the deleted share partition returns the complete uninitialized-partition refusal

  Scenario: write fences stale state epoch
    Given a fresh broker with no share coordinator lookup yet
    And g1's topic stale-state-epoch partition zero is initialized at state epoch 5 and start zero
    When the persister writes state epoch 0, start 0, complete count 0, and acquired batch none
    Then the share-state write reports FENCED_STATE_EPOCH

  Scenario: state survives restart
    Given a durable broker ready to serve share-state recovery
    And g1's topic survives-restart partition zero is initialized at state epoch 0 and start zero
    When the persister writes state epoch 0, start 7, complete count 3, and acquired batch none
    Then the share-state write reports NONE
    When start offset seven is persisted and the share-state broker restarts on the same directory
    And the summary of g1's partition is read
    Then the recovered share-state summary succeeds with start offset seven
