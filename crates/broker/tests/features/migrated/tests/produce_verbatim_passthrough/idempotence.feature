Feature: Idempotent sequence handling for verbatim producer batches
  Scenario: A retry reuses its assigned offset while a sequence gap is refused
    Given topic idem receives LZ4 batches from producer 9001
    When a producer batch starts at sequence 0 with record count 3
    Then the batch succeeds at base offset 0
    When a producer batch starts at sequence 3 with record count 2
    Then the batch succeeds at base offset 3
    When the partition settles at log end five
    And a producer batch starts at sequence 3 with record count 2
    Then the batch succeeds at base offset 3
    And the producer topic log end remains 5
    When a producer batch starts at sequence 99 with record count 1
    Then the batch is refused with OUT_OF_ORDER_SEQUENCE_NUMBER 45

  Scenario: Producer sequences wrap modulo 2^31 without weakening exact-retry matching
    Given topic idem-rollover receives LZ4 batches from producer 9002
    When the producer sends three records starting just before sequence rollover
    Then the batch succeeds at base offset 0
    When a producer batch starts at sequence 1 with record count 1
    Then the batch succeeds at base offset 3
    When a producer batch starts at sequence 1 with record count 1
    Then the batch succeeds at base offset 3
    And the producer topic log end remains 4
    When a producer batch starts at sequence 1 with record count 2
    Then the batch is refused with OUT_OF_ORDER_SEQUENCE_NUMBER 45
    When a producer batch starts at sequence 3 with record count 1
    Then the batch is refused with OUT_OF_ORDER_SEQUENCE_NUMBER 45
