Feature: Every independently bootstrapped topic manager learns each metadata partition

  Scenario: every rlmm catches up on the soak contracts cluster
    Given three brokers independently bootstrap topic-backed RLMMs from their own listeners with three metadata partitions, RF3 and min ISR2, and create three RF3 tiered data partitions with 1-KiB segments
    When each data partition receives sixty 200-byte LZ4 records
    Then within ninety seconds every broker lists at least one remote segment for each data partition
