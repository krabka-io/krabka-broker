Feature: Transaction timeout validation and prepared transaction recovery

  Scenario: Positive client timeout is stored unchanged
    Given a transaction-version-2 coordinator with two-phase commit enabled and 8000ms maximum timeout owns txn-small
    When InitProducerId requests timeout 500 with two-phase commit false
    Then the complete initialization response has code 0 and persisted timeout 500

  Scenario: Timeout above the broker maximum is refused
    Given a transaction-version-2 coordinator with two-phase commit enabled and 8000ms maximum timeout owns txn-above-max
    When InitProducerId requests timeout 10000 with two-phase commit false
    Then the complete initialization response has code 50 and persisted timeout absent

  Scenario: Two-phase commit stores the sentinel timeout
    Given a transaction-version-2 coordinator with two-phase commit enabled and 8000ms maximum timeout owns txn-2pc
    When InitProducerId requests timeout 500 with two-phase commit true
    Then the complete initialization response has code 0 and persisted timeout 2147483647

  Scenario: Zero timeout is refused
    Given a transaction-version-2 coordinator with two-phase commit enabled and 8000ms maximum timeout owns txn-zero
    When InitProducerId requests timeout 0 with two-phase commit false
    Then the complete initialization response has code 50 and persisted timeout absent

  Scenario: Prepared recovery fences stale identities and commits idempotently
    Given a transaction-version-2 coordinator with two-phase commit enabled and 8000ms maximum timeout owns txn-2pc
    And the 500ms two-phase transaction has a persisted ongoing producer identity
    When two clients recover the prepared transaction with a 500ms timeout and keepPreparedTxn enabled
    Then both recoveries retain the ongoing identity and the second recovery increments the recovery producer epoch
    When the older recovery client commits, then the current recovery client commits and retries
    Then the older client is fenced, the current commit is idempotent and the persisted state is CompleteCommit without a next producer identity
