Feature: The partition leader checks an idempotent producer's sequence and epoch

  Rule: A batch that continues the producer's sequence is appended

    Scenario: A first batch from a new producer is appended
      When the producer sends sequences 0 to 4 at epoch 0
      Then the batch is appended

    Scenario: The batch after the last retained one is appended
      Given the producer has appended sequences 0 to 4 at epoch 0
      When the producer sends sequences 5 to 7 at epoch 0
      Then the batch is appended

  Rule: Only an exact retry of the last retained batch is a duplicate

    Scenario: A retry of the last batch answers the offset it was written at
      Given the producer's batch of sequences 3 to 4 at epoch 0 was written at offset 10
      When the producer sends sequences 3 to 4 at epoch 0
      Then the batch is a duplicate of the batch at offset 10

    Scenario Outline: A batch that does not follow the last retained one is out of order
      Given the producer has appended sequences 3 to 4 at epoch 0
      When the producer sends sequences <first> to <last> at epoch 0
      Then the batch is refused with OUT_OF_ORDER_SEQUENCE_NUMBER

      Examples:
        | case                                   | first | last |
        | the same first sequence, fewer records | 3     | 3    |
        | overlapping the retained batch         | 2     | 3    |
        | a gap after the retained batch         | 10    | 11   |

  Rule: A newer epoch fences older epochs and restarts the sequence at zero

    Scenario: A batch from an older epoch is fenced
      Given the producer has appended sequences 0 to 4 at epoch 5
      When the producer sends sequences 5 to 7 at epoch 4
      Then the batch is refused with INVALID_PRODUCER_EPOCH

    Scenario: A newer epoch starts again at sequence zero
      Given the producer has appended sequences 0 to 2 at epoch 5
      When the producer sends sequences 0 to 0 at epoch 6
      Then the batch is appended

    Scenario: A newer epoch that continues the old sequence is out of order
      Given the producer has appended sequences 0 to 2 at epoch 5
      When the producer sends sequences 3 to 3 at epoch 6
      Then the batch is refused with OUT_OF_ORDER_SEQUENCE_NUMBER

    Scenario: A retry at the newer epoch is a duplicate again
      Given the producer has appended sequences 0 to 2 at epoch 5
      And the producer's batch of sequences 0 to 0 at epoch 6 was written at offset 10
      When the producer sends sequences 0 to 0 at epoch 6
      Then the batch is a duplicate of the batch at offset 10
