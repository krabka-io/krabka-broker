Feature: Kafka producer sequence continuity and retry admission
  Sequences wrap modulo 2^31. A batch is a retry only when its entire sequence
  range matches a retained batch at the current epoch. Trunk additionally requires
  an unknown producer on a never-written log to begin at sequence zero.

  Scenario Outline: Sequence arithmetic wraps in the signed integer boundary cases
    Given a producer sequence number <sequence>
    When the producer sequence is incremented and decremented by <step>
    Then the incremented sequence is <incremented> and the decremented sequence is <decremented>

    Examples:
      | sequence | step | incremented | decremented |
      | 0 | 0 | 0 | 0 |
      | 5 | 3 | 8 | 2 |
      | 2147483647 | 1 | 0 | 2147483646 |
      | 2147483646 | 3 | 1 | 2147483643 |
      | 0 | 1 | 1 | 2147483647 |
      | 2 | 5 | 7 | 2147483645 |
      | 2147483647 | 2147483647 | 2147483646 | 0 |
      | -1 | 1 | 0 | 2147483646 |
      | -2147483648 | -1 | 2147483647 | 1 |
      | -2147483648 | -2147483648 | 0 | 0 |
      | 5 | -2147483648 | 5 | 5 |
      | -3 | 2147483647 | 2147483644 | 2147483646 |

  Scenario: Epoch and retained batch boundaries decide append, retry, fence or refusal (1: boundary no state, snapshot absent)
    Given producer snapshot absent with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 0, base sequence 17, and delta 0 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is append

  Scenario: Epoch and retained batch boundaries decide append, retry, fence or refusal (2: boundary older epoch, snapshot 2:6)
    Given producer snapshot 2:6 with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 1, base sequence 4, and delta 2 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is fenced

  Scenario: Epoch and retained batch boundaries decide append, retry, fence or refusal (3: boundary new epoch starts at zero, snapshot 2:6)
    Given producer snapshot 2:6 with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 3, base sequence 0, and delta 0 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is append

  Scenario: Epoch and retained batch boundaries decide append, retry, fence or refusal (4: boundary new epoch continues sequence, snapshot 2:6)
    Given producer snapshot 2:6 with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 3, base sequence 7, and delta 0 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is out-of-order

  Scenario: Epoch and retained batch boundaries decide append, retry, fence or refusal (5: boundary new epoch repeats old batch, snapshot 2:6)
    Given producer snapshot 2:6 with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 3, base sequence 4, and delta 2 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is out-of-order

  Scenario: Epoch and retained batch boundaries decide append, retry, fence or refusal (6: boundary next sequence, snapshot 2:6)
    Given producer snapshot 2:6 with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 7, and delta 0 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is append

  Scenario: Epoch and retained batch boundaries decide append, retry, fence or refusal (7: boundary newest retained retry, snapshot 2:6)
    Given producer snapshot 2:6 with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 4, and delta 2 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is duplicate-slot-4

  Scenario: Epoch and retained batch boundaries decide append, retry, fence or refusal (8: boundary earlier retained retry, snapshot 2:6)
    Given producer snapshot 2:6 with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 1, and delta 1 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is duplicate-slot-2

  Scenario: Epoch and retained batch boundaries decide append, retry, fence or refusal (9: boundary oldest retained retry, snapshot 2:6)
    Given producer snapshot 2:6 with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 0, and delta 0 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is duplicate-slot-1

  Scenario: Epoch and retained batch boundaries decide append, retry, fence or refusal (10: boundary partial retained batch, snapshot 2:6)
    Given producer snapshot 2:6 with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 1, and delta 0 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is out-of-order

  Scenario: Epoch and retained batch boundaries decide append, retry, fence or refusal (11: boundary sequence gap, snapshot 2:6)
    Given producer snapshot 2:6 with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 9, and delta 0 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is out-of-order

  Scenario: Epoch and retained batch boundaries decide append, retry, fence or refusal (12: boundary same base different delta, snapshot 2:6)
    Given producer snapshot 2:6 with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 4, and delta 3 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is out-of-order

  Scenario: Trunk requires unknown producers on empty logs to start at zero (1: boundary first append zero, snapshot absent)
    Given producer snapshot absent with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 0, and delta 0 is evaluated on a empty log under trunk rules
    Then producer admission is append

  Scenario: Trunk requires unknown producers on empty logs to start at zero (2: boundary first append nonzero, snapshot absent)
    Given producer snapshot absent with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 7, and delta 0 is evaluated on a empty log under trunk rules
    Then producer admission is out-of-order

  Scenario: Trunk requires unknown producers on empty logs to start at zero (3: boundary existing log without state, snapshot absent)
    Given producer snapshot absent with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 7, and delta 0 is evaluated on a nonempty log under trunk rules
    Then producer admission is append

  Scenario: Trunk requires unknown producers on empty logs to start at zero (4: boundary expired entry restarting at zero, snapshot absent)
    Given producer snapshot absent with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 0, and delta 0 is evaluated on a nonempty log under trunk rules
    Then producer admission is append

  Scenario: Trunk requires unknown producers on empty logs to start at zero (5: boundary released Kafka lacks trunk rule, snapshot absent)
    Given producer snapshot absent with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 7, and delta 0 is evaluated on a empty log under 4.3.1 rules
    Then producer admission is append

  Scenario: Trunk requires unknown producers on empty logs to start at zero (6: boundary known producer continues, snapshot 2:6)
    Given producer snapshot 2:6 with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 7, and delta 0 is evaluated on a empty log under trunk rules
    Then producer admission is append

  Scenario: Trunk requires unknown producers on empty logs to start at zero (7: boundary known producer skips sequence, snapshot 2:6)
    Given producer snapshot 2:6 with retained sequence ranges unused,0,1-2,3,4-6
    When a batch at epoch 2, base sequence 9, and delta 0 is evaluated on a empty log under trunk rules
    Then producer admission is out-of-order

  Scenario: Wrapping sequences preserve retry identity only while the batch is retained (1: boundary next sequence wraps to zero, snapshot 0:2147483647)
    Given producer snapshot 0:2147483647 with retained sequence ranges 2147483646-2147483647
    When a batch at epoch 0, base sequence 0, and delta 3 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is append

  Scenario: Wrapping sequences preserve retry identity only while the batch is retained (2: boundary retained batch before wrap, snapshot 0:2147483647)
    Given producer snapshot 0:2147483647 with retained sequence ranges 2147483646-2147483647
    When a batch at epoch 0, base sequence 2147483646, and delta 1 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is duplicate-slot-0

  Scenario: Wrapping sequences preserve retry identity only while the batch is retained (3: boundary retry after eviction, snapshot 0:2147483647)
    Given producer snapshot 0:2147483647 with retained sequence ranges empty
    When a batch at epoch 0, base sequence 2147483646, and delta 1 is evaluated on a nonempty log under 4.3.1 rules
    Then producer admission is out-of-order
