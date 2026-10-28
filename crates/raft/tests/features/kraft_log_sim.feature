Feature: KRaft consensus over real on-disk logs
  Scenario: Committed logs agree byte for byte
    Given three disk-backed voters have replicated 5 records
    Then the committed batches and raw bytes agree on every voter

  Scenario: A divergent follower truncates and reconverges
    Given three disk-backed voters have replicated 3 records
    When a follower receives a conflicting epoch seven tail
    And the leader writes authoritative records and the follower reconnects
    Then the follower's log and epoch match the leader after truncation

  Scenario: Committed watermarks fit in every voter log
    Given three disk-backed voters have replicated 3 records
    Then the committed high watermark is at least 3 and within every log
