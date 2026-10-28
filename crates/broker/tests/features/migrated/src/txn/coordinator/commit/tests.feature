Feature: Transaction-state append errors preserve Kafka coordinator availability semantics

  Scenario Outline: Map a failed transaction log append
    Given the transaction state log append failed with Kafka code <append>
    When the transaction coordinator maps the append failure for its client
    Then the transaction append response carries Kafka code <response>

    Examples:
      | append | response |
      | 3 | 15 |
      | 20 | 15 |
      | 2 | 2 |
      | 19 | 15 |
      | 7 | 15 |
      | 6 | 16 |
      | 56 | 16 |
      | 10 | -1 |
      | 18 | -1 |
      | -1 | -1 |
