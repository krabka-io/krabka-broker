Feature: Idempotent producer sequence enforcement
  Scenario: idempotent produce dedups duplicate batch
    Given an idempotent producer for topic idem with records a,b,c
    When the producer sends its initial sequence-zero batch
    Then produce response 1 succeeds at base offset 0
    When the identical producer batch is retried
    Then produce response 2 succeeds at base offset 0

  Scenario: out of order returns 45
    Given an idempotent producer for topic ooo with records x,y
    When the producer sends its initial sequence-zero batch
    Then produce response 1 succeeds at base offset 0
    When the producer skips to sequence ten with records x and y
    Then the sequence gap returns OUT_OF_ORDER_SEQUENCE_NUMBER
