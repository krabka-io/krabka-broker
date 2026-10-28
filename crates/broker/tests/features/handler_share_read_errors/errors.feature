Feature: ShareFetch exposes distinct storage corruption and broker read failures
  Scenario Outline: A failed log read maps to the Kafka partition error for its cause
    Given reading share records fails with <failure>
    When ShareFetch maps the failed read to a partition response error
    Then the ShareFetch partition error is <code>
    Examples:
      | failure                            | code |
      | a log disk I/O error                | 56   |
      | a corrupt transaction index         | 2    |
      | a record body shorter than one byte | 2    |
      | a broker I/O error                  | -1   |
