Feature: Transaction timeout and producer identity decisions
  Timeouts are signed milliseconds; external-2pc denotes the no-timeout sentinel.
  Producer identities use producer-id:epoch notation. Identity rollover, retries
  and completion must fence stale state without confusing it with idempotency.

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (1: boundary past timeout, state ongoing)
    Given a transaction in state ongoing with timeout 60000 and start time 0 ms
    When the transaction timeout reaper runs at 60001 ms
    Then the timeout reaper aborts the transaction: true

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (2: boundary at timeout, state ongoing)
    Given a transaction in state ongoing with timeout 60000 and start time 0 ms
    When the transaction timeout reaper runs at 60000 ms
    Then the timeout reaper aborts the transaction: false

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (3: boundary before timeout, state ongoing)
    Given a transaction in state ongoing with timeout 60000 and start time 0 ms
    When the transaction timeout reaper runs at 59999 ms
    Then the timeout reaper aborts the transaction: false

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (4: boundary one millisecond deadline, state ongoing)
    Given a transaction in state ongoing with timeout 1 and start time 10 ms
    When the transaction timeout reaper runs at 11 ms
    Then the timeout reaper aborts the transaction: false

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (5: boundary one millisecond expired, state ongoing)
    Given a transaction in state ongoing with timeout 1 and start time 10 ms
    When the transaction timeout reaper runs at 12 ms
    Then the timeout reaper aborts the transaction: true

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (6: boundary not ongoing, state not-ongoing)
    Given a transaction in state not-ongoing with timeout 1 and start time 0 ms
    When the transaction timeout reaper runs at 9223372036854775807 ms
    Then the timeout reaper aborts the transaction: false

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (7: boundary external owner, state ongoing)
    Given a transaction in state ongoing with timeout external-2pc and start time 0 ms
    When the transaction timeout reaper runs at 9223372036854775807 ms
    Then the timeout reaper aborts the transaction: false

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (8: boundary external owner extreme interval, state ongoing)
    Given a transaction in state ongoing with timeout external-2pc and start time -9223372036854775808 ms
    When the transaction timeout reaper runs at 9223372036854775807 ms
    Then the timeout reaper aborts the transaction: false

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (9: boundary clock rollback, state ongoing)
    Given a transaction in state ongoing with timeout 60000 and start time 100000 ms
    When the transaction timeout reaper runs at 0 ms
    Then the timeout reaper aborts the transaction: false

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (10: boundary zero timeout before start, state ongoing)
    Given a transaction in state ongoing with timeout 0 and start time 10 ms
    When the transaction timeout reaper runs at 9 ms
    Then the timeout reaper aborts the transaction: false

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (11: boundary zero timeout at start, state ongoing)
    Given a transaction in state ongoing with timeout 0 and start time 10 ms
    When the transaction timeout reaper runs at 10 ms
    Then the timeout reaper aborts the transaction: false

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (12: boundary extreme clock rollback, state ongoing)
    Given a transaction in state ongoing with timeout 0 and start time 9223372036854775807 ms
    When the transaction timeout reaper runs at -9223372036854775808 ms
    Then the timeout reaper aborts the transaction: false

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (13: boundary zero timeout after start, state ongoing)
    Given a transaction in state ongoing with timeout 0 and start time 10 ms
    When the transaction timeout reaper runs at 11 ms
    Then the timeout reaper aborts the transaction: true

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (14: boundary foreign negative timeout passed, state ongoing)
    Given a transaction in state ongoing with timeout -5 and start time 10 ms
    When the transaction timeout reaper runs at 6 ms
    Then the timeout reaper aborts the transaction: true

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (15: boundary foreign negative timeout boundary, state ongoing)
    Given a transaction in state ongoing with timeout -5 and start time 10 ms
    When the transaction timeout reaper runs at 5 ms
    Then the timeout reaper aborts the transaction: false

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (16: boundary elapsed time does not overflow, state ongoing)
    Given a transaction in state ongoing with timeout 60000 and start time -9223372036854775808 ms
    When the transaction timeout reaper runs at 9223372036854775807 ms
    Then the timeout reaper aborts the transaction: true

  Scenario: Only an ongoing transaction strictly beyond its timeout is reaped (17: boundary large timeout on rollback, state ongoing)
    Given a transaction in state ongoing with timeout 2147483646 and start time 9223372036854775807 ms
    When the transaction timeout reaper runs at -9223372036854775808 ms
    Then the timeout reaper aborts the transaction: false

  Scenario Outline: Epoch ceilings rotate verified producer identities only when replacement IDs are available
    Given producer 7 at epoch <epoch> has verified identity <verified>, recovery mode <recovery>, and replacement producer ID <fresh>
    When the coordinator allocates the next producer identity
    Then the allocated producer identity is <allocated>

    Examples:
      | verified | recovery | epoch | fresh | allocated |
      | false | false | 32767 | none | 7:32767 |
      | false | true | 32767 | 11 | 7:32767 |
      | true | false | 32765 | none | 7:32766 |
      | true | false | 32766 | none | unavailable |
      | true | false | 32766 | 11 | 11:0 |
      | true | true | 32766 | none | 7:32767 |
      | true | true | 32767 | none | unavailable |
      | true | true | 32767 | 11 | 11:0 |

  Scenario: InitProducerId distinguishes identity bumps from retries and fenced callers (1: entry_pid 7, entry_epoch 4)
    Given a producer entry with ID 7, epoch 4, last epoch -1, and previous ID -1
    When InitProducerId requests producer ID -1 and epoch -1
    Then InitProducerId identity handling is bump-without-expected-identity

  Scenario: InitProducerId distinguishes identity bumps from retries and fenced callers (2: entry_pid 7, entry_epoch 4)
    Given a producer entry with ID 7, epoch 4, last epoch -1, and previous ID -1
    When InitProducerId requests producer ID -1 and epoch 4
    Then InitProducerId identity handling is bump-without-expected-identity

  Scenario: InitProducerId distinguishes identity bumps from retries and fenced callers (3: entry_pid 7, entry_epoch 4)
    Given a producer entry with ID 7, epoch 4, last epoch -1, and previous ID -1
    When InitProducerId requests producer ID 7 and epoch 4
    Then InitProducerId identity handling is bump-epoch

  Scenario: InitProducerId distinguishes identity bumps from retries and fenced callers (4: entry_pid 7, entry_epoch 5)
    Given a producer entry with ID 7, epoch 5, last epoch 4, and previous ID -1
    When InitProducerId requests producer ID 7 and epoch 4
    Then InitProducerId identity handling is retry-current-identity

  Scenario: InitProducerId distinguishes identity bumps from retries and fenced callers (5: entry_pid 7, entry_epoch 5)
    Given a producer entry with ID 7, epoch 5, last epoch 4, and previous ID -1
    When InitProducerId requests producer ID 7 and epoch 3
    Then InitProducerId identity handling is fence-requester

  Scenario: InitProducerId distinguishes identity bumps from retries and fenced callers (6: entry_pid 7, entry_epoch 4)
    Given a producer entry with ID 7, epoch 4, last epoch -1, and previous ID -1
    When InitProducerId requests producer ID 7 and epoch 5
    Then InitProducerId identity handling is fence-requester

  Scenario: InitProducerId distinguishes identity bumps from retries and fenced callers (7: entry_pid 7, entry_epoch 4)
    Given a producer entry with ID 7, epoch 4, last epoch -1, and previous ID -1
    When InitProducerId requests producer ID 9 and epoch 4
    Then InitProducerId identity handling is fence-requester

  Scenario: InitProducerId distinguishes identity bumps from retries and fenced callers (8: entry_pid 11, entry_epoch 0)
    Given a producer entry with ID 11, epoch 0, last epoch 32766, and previous ID 7
    When InitProducerId requests producer ID 7 and epoch 32766
    Then InitProducerId identity handling is retry-current-identity

  Scenario: InitProducerId distinguishes identity bumps from retries and fenced callers (9: entry_pid 11, entry_epoch 0)
    Given a producer entry with ID 11, epoch 0, last epoch 32766, and previous ID 7
    When InitProducerId requests producer ID 7 and epoch 32767
    Then InitProducerId identity handling is fence-requester

  Scenario: InitProducerId distinguishes identity bumps from retries and fenced callers (10: entry_pid 11, entry_epoch 0)
    Given a producer entry with ID 11, epoch 0, last epoch 32766, and previous ID 7
    When InitProducerId requests producer ID 7 and epoch 5
    Then InitProducerId identity handling is fence-requester

  Scenario: InitProducerId distinguishes identity bumps from retries and fenced callers (11: entry_pid 11, entry_epoch 0)
    Given a producer entry with ID 11, epoch 0, last epoch -1, and previous ID 7
    When InitProducerId requests producer ID 7 and epoch 32766
    Then InitProducerId identity handling is fence-requester

  Scenario: Commit completion requires the prepared identity or the exact already-completed identity (1: boundary prepared identity, current 7:3)
    Given a current transaction identity 7:3 in state prepare-commit, prepared identity 7:3, and completion identity 7:4
    When the coordinator completes the prepared commit
    Then transaction completion is proceed

  Scenario: Commit completion requires the prepared identity or the exact already-completed identity (2: boundary stale prepared identity, current 7:4)
    Given a current transaction identity 7:4 in state prepare-commit, prepared identity 7:3, and completion identity 7:4
    When the coordinator completes the prepared commit
    Then transaction completion is reject-stale-identity

  Scenario: Commit completion requires the prepared identity or the exact already-completed identity (3: boundary not prepared, current 7:3)
    Given a current transaction identity 7:3 in state ongoing, prepared identity 7:3, and completion identity 7:4
    When the coordinator completes the prepared commit
    Then transaction completion is reject-state

  Scenario: Commit completion requires the prepared identity or the exact already-completed identity (4: boundary rotated identity completed, current 11:0)
    Given a current transaction identity 11:0 in state complete-commit, prepared identity 7:32767, and completion identity 11:0
    When the coordinator completes the prepared commit
    Then transaction completion is already-complete

  Scenario: Commit completion requires the prepared identity or the exact already-completed identity (5: boundary old identity marked complete, current 7:32767)
    Given a current transaction identity 7:32767 in state complete-commit, prepared identity 7:32767, and completion identity 11:0
    When the coordinator completes the prepared commit
    Then transaction completion is reject-state
