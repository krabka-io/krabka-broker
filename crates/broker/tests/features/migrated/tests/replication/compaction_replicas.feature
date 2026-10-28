Feature: Every replica retains an active producer's final batch header during compaction

  Scenario: every replica keeps the last batch of an active producer
    Given three compacted replicas use 100-byte segments so each record seals its own batch
    When the leader appends these keyed batches at the same current timestamp
      | id   | epoch | sequence | key | value |
      | 9201 | 0     | 0        | a   | p-a   |
      | 9201 | 0     | 1        | b   | p-b   |
      | -1   | -1    | -1       | a   | a-2   |
      | -1   | -1    | -1       | b   | b-3   |
      | -1   | -1    | -1       | c   | c-4   |
    Then all five compacted-topic appends succeed
    When every replica commits through offset five and compacts its own local log
    Then every compacted replica retains exactly these ordered producer headers and records
      | base | last | id   | epoch | sequence | records | key    | value  |
      | 1    | 1    | 9201 | 0     | 1        | empty   | absent | absent |
      | 2    | 2    | -1   | -1    | -1       | one     | a      | a-2    |
      | 3    | 3    | -1   | -1    | -1       | one     | b      | b-3    |
      | 4    | 4    | -1   | -1    | -1       | one     | c      | c-4    |
