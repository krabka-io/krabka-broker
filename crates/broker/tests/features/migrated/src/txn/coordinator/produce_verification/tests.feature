Feature: Verify-only transaction partition checks follow Kafka validation order

  Background:
    Given producer 7 at epoch 3 requests transaction partition verification

  Scenario Outline: Verify a partition against the held transaction
    Given the held transaction has producer <producer>, epoch <epoch>, state <state>, and partition membership <contains>
    When the transaction coordinator verifies the requested partition
    Then transaction partition verification returns Kafka code <code>

    Examples:
      | producer | epoch | state | contains | code |
      | 8 | 3 | Ongoing | true | 49 |
      | 7 | 4 | Ongoing | true | 90 |
      | 7 | 3 | PrepareCommit | true | 51 |
      | 7 | 3 | PrepareAbort | false | 51 |
      | 7 | 3 | Ongoing | true | 0 |
      | 7 | 3 | Ongoing | false | 120 |
      | 7 | 3 | CompleteCommit | false | 120 |
