Feature: Consumer proactively validates committed positions after a leader epoch advance
  Scenario: consumer proactively validates and surfaces truncation
    Given group proactive-grp consumed and committed all four epoch 0 records from proactive-trunc
    When the leader log is truncated to offset 2
    Then the leader log end offset is exactly 2
    When the same partition leader is published at epoch 1
    Given a None-reset consumer has inherited the committed epoch 0 position 4 and completed assignment
    When the consumer polls across the leader epoch advance for up to 15 seconds
    Then poll delivers no records and reports LogTruncation for position 4 with safe offset 2
    And the truncating poll issued at least one OffsetForLeaderEpoch request
