Feature: Soak ledgers preserve transactional visibility and duplicate positions

  Background:
    Given the self-check idempotent and transactional paths have matching topics on an undisturbed broker

  Scenario: an undisturbed load reads back once and hides what it aborted
    When the workload runs for 12 seconds, stops, settles transactions and reads all declared isolation levels and acknowledged group commits
    Then reconciliation has no findings, committed and aborted transactions both occurred and group commits were checked

  Scenario: a record stored twice is read back at both offsets and a stranger is skipped
    When the idempotent path stores sequence 0 twice and an unrelated probe record at offset 2
    And the uncommitted reader reads the idempotent producer's ledger
    Then the ledger contains sequence 0 at offsets 0 and 1, reports that duplicate and excludes the unrelated probe record
