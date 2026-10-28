Feature: Transaction markers decide when group offsets materialize

  Background:
    Given a source topic and group coordinator are ready on one broker

  Scenario: txn offset commit visible via offset fetch after commit marker
    When transaction tid-materialize stages group g-materialize offset 3
    And OffsetFetch reads group g-materialize
    Then the fetched offset is -1
    When EndTxn resolves tid-materialize by commit
    And OffsetFetch reads group g-materialize
    Then the fetched offset is 3

  Scenario: txn offset commit dropped on abort marker
    When transaction tid-materialize stages group g-materialize offset 5
    And EndTxn resolves tid-materialize by abort
    And OffsetFetch reads group g-materialize
    Then the fetched offset is -1

  Scenario Outline: require stable offset fetch tracks transaction outcome
    Given group g-materialize has stable committed offset 3
    When transaction tid-materialize stages group g-materialize offset 9
    And v10 OffsetFetch reads g-materialize with and without require_stable
    Then the full strict response has offset -1 and UNSTABLE_OFFSET_COMMIT 88 while the relaxed response retains offset 3
    When EndTxn resolves tid-materialize by <outcome>
    And v10 require_stable OffsetFetch reads the resolved transaction
    Then the full stable v10 response reports offset <offset> with no error

    Examples:
      | outcome | offset |
      | commit  | 9      |
      | abort   | 3      |
