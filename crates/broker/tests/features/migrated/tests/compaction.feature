Feature: Compaction removes stale values from sealed segments

  Scenario: compaction dedupes via native client
    Given a compacted topic compacted with segment size 256 and a one-second cleaner
    When rounds 0 through 9 write values vROUND-KEY for keys k1,k2,k3 and a compaction pass completes
    And round 10 writes final values for keys k1,k2,k3, followed by 8 padding records and another compaction pass
    And the compacted log is fetched from offset zero
    Then the surviving non-padding keys are k1,k2,k3, each retains round 10, and rounds 0 through 9 are absent
