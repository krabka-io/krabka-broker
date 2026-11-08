Feature: ShareFetch exposes distinct storage corruption and broker read failures
  Scenario Outline: A failed log read maps to the Kafka partition error for its cause
    Given reading share records from an online log dir fails with <failure>
    When ShareFetch settles the failed read for its partition row
    Then the ShareFetch partition error is <code> and the log dir offline state is <offline>
    Examples:
      | failure                             | code | offline |
      | a log disk I/O error                | 56   | true    |
      | a corrupt transaction index         | 2    | false   |
      | a record body shorter than one byte | 2    | false   |
      | a broker I/O error                  | -1   | false   |
