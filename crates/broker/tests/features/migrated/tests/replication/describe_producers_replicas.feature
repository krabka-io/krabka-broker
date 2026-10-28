Feature: Every replica describes the producer state recovered from its own replicated log

  Scenario: every replica describes the producers of its log
    Given three replicas accept transactional Produce v11 without partition verification
    When the leader appends these producer batches and transaction markers in order
      | id   | epoch | sequence | records | timestamp     | transaction   | marker | marker_epoch_delta | transaction_version | coordinator_epoch |
      | 9001 | 0     | 0        | 3       | 1700000000000 | ordinary      | none   | 0                  | 0                   | -1                |
      | 9001 | 0     | 3        | 2       | 1700000000001 | ordinary      | none   | 0                  | 0                   | -1                |
      | 9002 | 2     | 0        | 2       | 1700000000002 | transactional | commit | 0                  | 1                   | 17                |
      | 9003 | 4     | 0        | 1       | 1700000000003 | transactional | abort  | 1                  | 2                   | 17                |
      | 9004 | 0     | 0        | 2       | 1700000000004 | transactional | none   | 0                  | 0                   | -1                |
    Then all seven producer append and marker operations succeed
    When every replica reaches log end twelve and answers DescribeProducers
    Then all three complete DescribeProducers responses contain these ordered rows
      | id   | epoch | sequence | timestamp          | coordinator_epoch | transaction_start |
      | 9001 | 0     | 4        | 1700000000001      | -1                | -1                |
      | 9002 | 2     | 1        | commit_marker_time | 17                | -1                |
      | 9003 | 5     | -1       | abort_marker_time  | 17                | -1                |
      | 9004 | 0     | 1        | 1700000000004      | -1                | 10                |
