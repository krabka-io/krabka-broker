Feature: Share isolation defers open transactions and archives aborted data

  Scenario: read committed skips open txn then sees committed
    Given a share group whose isolation override is read_committed
    When transactional producer share-rc-tid opens and flushes records a,b,c
    And a share consumer joins the isolation-test group
    And the committed share consumer fetches six times while the transaction stays open
    Then all six open-transaction share fetches acquire zero records
    When the share-test transaction is committed: true
    And the same consumer fetches committed records using epochs six through twenty-nine
    Then the previously deferred share values are exactly a,b,c

  Scenario Outline: aborted transaction data is archived under read committed
    Given a share group whose isolation override is <level>
    When transactional producer share-aborted-tid opens and flushes records a,b,c
    And the share-test transaction is committed: <commit>
    And a plain v0 record is appended after the transaction marker
    And a share consumer joins the isolation-test group
    And the share consumer reads until plain offset four is acquired
    Then the acquired offsets are <offsets> and their visible values are <values>

    Examples:
      | level            | commit | offsets | values   |
      | read_committed   | true   | 0,1,2,4 | a,b,c,v0 |
      | read_committed   | false  | 4       | v0       |
      | read_uncommitted | false  | 0,1,2,4 | a,b,c,v0 |
      | unset            | false  | 0,1,2,4 | a,b,c,v0 |
