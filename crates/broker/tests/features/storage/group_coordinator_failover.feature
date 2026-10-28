Feature: Group coordinator failover replays acknowledged writes and preserves member identities

  Scenario: Only acknowledged group offsets survive replication-barrier coordinator failover
    Given a three-replica offsets partition coordinates a joined KIP-848 group and a classic group, while their data topic is on another broker and ISR shrink is disabled for one hour
    When the KIP-848 member commits offset 5 and the classic member commits offset 7 before the replication barrier
    Then both complete commit responses succeed and the offsets high watermark equals the acknowledged log end
    When the coordinator's replication relay is cut and both members submit newer offsets 9 and 11 while a read is sent before either commit completes
    Then the read returns exactly acknowledged offsets 5 and 7 while both newer commit tasks are still pending
    When both newer commits reach their write timeout and the client reads offsets again
    Then both complete timeout responses are COORDINATOR_NOT_AVAILABLE 15, the read still returns 5 and 7 and the high watermark is unchanged
    When the old coordinator crashes and the clients retry both survivors until one serves both existing members
    Then the new offsets leader preserves offsets 5 and 7, the KIP-848 epoch and classic generation, and every error during the move is 14, 15 or 16
    When the clients send offset fetch and both member heartbeats to the other surviving broker
    Then all three complete responses from the other survivor are NOT_COORDINATOR 16
    When both original members commit offsets 13 and 17 to the new coordinator at their original epoch and generation
    Then both complete recommit responses succeed and offset fetch returns exactly 13 and 17
