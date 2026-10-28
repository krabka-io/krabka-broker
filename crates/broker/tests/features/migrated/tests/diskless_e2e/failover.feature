Feature: Recover acknowledged diskless batches from WAL voters

  Scenario: diskless acks all append survives the acking broker
    Given a three-voter diskless quorum with 24 acknowledged records and object flushing parked for one hour
    When every follower checkpoints the acknowledged range through offset 24 within one minute
    Then every follower's durable WAL range is exactly 0 through 24
    When the accepting leader serves 24 acknowledged records within one minute
    Then the leader serves the produced 24 payloads and each voter's WAL holds identical batch bytes
    When both surviving candidates' canonical partition logs are truncated to offset zero
    Then every candidate's canonical log end is zero before the accepting broker crashes
    When the accepting broker crashes and the surviving majority promotes a new partition leader
    Then the promoted partition leader differs from the accepting broker
    When the promoted leader serves 24 records within two minutes
    Then the promoted leader serves all 24 produced records in the accepting leader's exact batch bytes
    And the diskless failover cluster is shut down
