Feature: Replicated idempotent producer state survives leader replacement and re-election

  Background:
    Given three registered brokers replicate an idempotent producer's log
    And producer 7331 uses epoch 0 with 3 records per batch at timestamp 1700000000000

  Scenario: a leader elected by a controlled shutdown answers a replicated retry as a duplicate
    Given topic idempotent-bounce chooses a data leader other than the active controller
    When the current leader receives the idempotent batch beginning at sequence 0
    Then the complete idempotent Produce response has base offset 0 and append timestamp -1
    When the current leader receives the idempotent batch beginning at sequence 3
    Then the complete idempotent Produce response has base offset 3 and append timestamp -1
    And all idempotent replicas reach log end 6
    When the idempotent data leader completes controlled shutdown within thirty seconds
    And the current leader receives the idempotent batch beginning at sequence 3
    Then the complete idempotent Produce response has base offset 3 and append timestamp 1700000000000
    When the current leader receives the idempotent batch beginning at sequence 0
    Then the complete idempotent Produce response has base offset 0 and append timestamp 1700000000000
    When the current leader receives the idempotent batch beginning at sequence 6
    Then the complete idempotent Produce response has base offset 6 and append timestamp -1
    When the local idempotent batch bases are read after log end nine
    Then exactly 2 local logs contain batch bases zero, three, and six

  Scenario: a leader elected again continues from the batches it replicated
    Given topic idempotent-reelection pins broker one as leader with replicas one, two, and three
    When the current leader receives the idempotent batch beginning at sequence 0
    Then the complete idempotent Produce response has base offset 0 and append timestamp -1
    When broker 2 is elected as idempotent partition leader at the next epoch
    And the current leader receives the idempotent batch beginning at sequence 3
    Then the complete idempotent Produce response has base offset 3 and append timestamp -1
    And all idempotent replicas reach log end 6
    When broker 1 is elected as idempotent partition leader at the next epoch
    And the current leader receives the idempotent batch beginning at sequence 6
    Then the complete idempotent Produce response has base offset 6 and append timestamp -1
    When the current leader receives the idempotent batch beginning at sequence 3
    Then the complete idempotent Produce response has base offset 3 and append timestamp 1700000000000
    When the local idempotent batch bases are read after log end nine
    Then exactly 3 local logs contain batch bases zero, three, and six
