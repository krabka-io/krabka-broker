Feature: Transaction status records retain Kafka state identifiers
  Persisted transaction states must recover to the same coordinator state.

  Scenario Outline: Persist and recover transaction state <state>
    Given a transaction is in state <state>
    When the transaction state is persisted and recovered through its Kafka status byte
    Then the persisted status byte is <status> and the recovered state is <state>

    Examples:
      | state | status |
      | Empty | 0 |
      | Ongoing | 1 |
      | PrepareCommit | 2 |
      | PrepareAbort | 3 |
      | CompleteCommit | 4 |
      | CompleteAbort | 5 |
      | Dead | 6 |

  Scenario Outline: Unsupported transaction status <status> remains absent
    Given a transaction log record contains Kafka status byte <status>
    When the Kafka transaction status byte is recovered
    Then the status byte has no modeled transaction state

    Examples:
      | status |
      | 7 |
      | -1 |
