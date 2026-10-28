Feature: tests transactions txn uncommitted writes

  Scenario: an end txn is answered only once its commit decision is replicated
    Given a three-replica transaction-state partition holds the decision while a different broker leads an RF1 data partition containing transactional a,b,c
    When the coordinator relay is cut and EndTxn appends one local PrepareCommit state record
    Then EndTxn remains unanswered for two seconds
    And the transaction-state high watermark remains at its previous log end
    When the coordinator crashes with the unreplicated commit
    Then the pending EndTxn request is lost without a response
    When a follower becomes transaction coordinator and the producer retries EndTxn
    Then EndTxn succeeds with the same producer id and an epoch increment of one
    When the data leader appends plain z and a committed consumer reads through it
    Then the committed consumer sees exactly a,b,c,z

  Scenario: a transaction completes only once its markers are committed
    Given the coordinator alone replicates transaction state and leads an RF2 data partition whose follower has transactional a,b,c
    When the coordinator relay is cut and EndTxn appends a local data COMMIT marker at offset 3
    Then EndTxn remains unanswered for two seconds
    And DescribeTransactions remains PrepareCommit with its data partition 0 awaiting its marker
    When the coordinator crashes with the unreplicated commit
    Then the pending EndTxn request is lost without a response
    When the data follower takes leadership and the healed coordinator restarts to retry its marker
    And the data leader appends plain z and a committed consumer reads through it
    Then the committed consumer sees exactly a,b,c,z
    And DescribeTransactions reports CompleteCommit with no outstanding partitions

  Scenario: group coordinator writes are answered only once committed
    Given three-replica offsets and transaction state hold stable group offset 5 and an open transaction registered for that group, with ISR scan one hour and heartbeat timeout ten minutes
    When an offsets follower that is neither coordinator crashes while remaining in ISR
    And TxnOffsetCommit proposes partition 0 offset 7 with generation -1 and an empty member id
    Then the transactional offset response contains only partition 0 with COORDINATOR_NOT_AVAILABLE 15
    When OffsetDelete requests the group's partition 0 offset
    Then the full OffsetDelete response is COORDINATOR_NOT_AVAILABLE 15 with default remaining fields
