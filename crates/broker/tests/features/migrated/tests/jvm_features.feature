Feature: Kafka JVM feature and dynamic quorum administration
  Scenario: kafka features describe and round trip
    Given a standalone broker finalizes the latest-release feature defaults
    When the Kafka 4.3.1 feature tool describes finalized features
    Then feature description succeeds and contains these finalized levels
      | feature             | level   |
      | metadata.version    | present |
      | group.version       | 1       |
      | transaction.version | 2       |
    When the feature tool requests downgrade of transaction.version to level 1
    Then the feature mutation succeeds and transaction.version reads finalized level 1
    When the feature tool requests upgrade of transaction.version to level 2
    Then the feature mutation succeeds and transaction.version reads finalized level 2
    When the feature tool requests upgrade of kraft.version to level 1
    Then the feature mutation succeeds and kraft.version reads finalized level 1
    When the feature tool requests downgrade of kraft.version to level 0
    Then the irreversible kraft.version downgrade is refused
    Given controller observer 2 with directory AAAAAAAAAAAAAAAAAAAAAg has fetched kraft.version 1
    When the quorum tool adds controller observer 2 and describes quorum status
    Then controller addition succeeds and quorum status names leader 1 and voters 1 and 2 with their configured endpoints
    When the quorum tool removes controller 2 using directory AAAAAAAAAAAAAAAAAAAAAg
    Then controller removal succeeds leaving one committed voter
    When the quorum tool removes the last controller 1 using directory AAAAAAAAAAAAAAAAAAAAAQ
    Then last-voter removal fails with VoterNotFoundException

  Scenario: kafka features describes and round trips elr
    Given a standalone broker finalizes the latest-release feature defaults
    When the Kafka 4.3.1 feature tool describes finalized features
    Then feature description succeeds and contains these finalized levels
      | feature                         | level |
      | eligible.leader.replicas.version | 1     |
    When the feature tool requests downgrade of eligible.leader.replicas.version to level 0
    Then the feature mutation succeeds and eligible.leader.replicas.version is disabled by an absent or zero level
    When the feature tool requests upgrade of eligible.leader.replicas.version to level 1
    Then the feature mutation succeeds and eligible.leader.replicas.version reads finalized level 1
