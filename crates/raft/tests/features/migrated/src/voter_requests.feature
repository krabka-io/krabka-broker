Feature: Kafka voter identity, admission and reconfiguration responses
  Cluster identity takes precedence over leadership and voter validation. Add and
  remove requests preserve Kafka error text; updates also validate leader epoch,
  feature ranges and controller endpoints before accepting a voter mutation.

  Scenario: An undefined directory uses Kafka's undefined replica-key marker
    Given voter 3 has a undefined directory identity
    When the Kafka replica key is formatted
    Then the formatted replica key is ReplicaKey(id=3, directoryId=<undefined>)

  Scenario: A real directory is encoded in Kafka base64 form
    Given voter 3 has a defined directory identity
    When the Kafka replica key is formatted
    Then the formatted replica key is ReplicaKey(id=3, directoryId=CQkJCQkJCQkJCQkJCQkJCQ)

  Scenario: AddVoter rejects cluster leadership and voter defects in Kafka order (1: boundary foreign cluster on follower, leader false)
    Given the local quorum is at epoch 7 with leadership false
    And an AddVoter request names cluster foreign, voter 3, directory defined, and listener controller
    When Kafka AddVoter admission checks are applied
    Then voter request admission returns INCONSISTENT_CLUSTER_ID with message The given id "cluster-b" doesn't match the cluster id "cluster-a"

  Scenario: AddVoter rejects cluster leadership and voter defects in Kafka order (2: boundary follower, leader false)
    Given the local quorum is at epoch 7 with leadership false
    And an AddVoter request names cluster absent, voter 3, directory defined, and listener controller
    When Kafka AddVoter admission checks are applied
    Then voter request admission returns NOT_LEADER_OR_FOLLOWER with message absent

  Scenario: AddVoter rejects cluster leadership and voter defects in Kafka order (3: boundary invalid voter on follower, leader false)
    Given the local quorum is at epoch 7 with leadership false
    And an AddVoter request names cluster absent, voter -1, directory defined, and listener controller
    When Kafka AddVoter admission checks are applied
    Then voter request admission returns NOT_LEADER_OR_FOLLOWER with message absent

  Scenario: AddVoter rejects cluster leadership and voter defects in Kafka order (4: boundary negative voter, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an AddVoter request names cluster absent, voter -1, directory defined, and listener controller
    When Kafka AddVoter admission checks are applied
    Then voter request admission returns INVALID_REQUEST with message Add voter request didn't include a valid voter

  Scenario: AddVoter rejects cluster leadership and voter defects in Kafka order (5: boundary undefined directory, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an AddVoter request names cluster absent, voter 3, directory undefined, and listener controller
    When Kafka AddVoter admission checks are applied
    Then voter request admission returns INVALID_REQUEST with message Add voter request didn't include a valid voter

  Scenario: AddVoter rejects cluster leadership and voter defects in Kafka order (6: boundary no listeners, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an AddVoter request names cluster absent, voter 3, directory defined, and listener absent
    When Kafka AddVoter admission checks are applied
    Then voter request admission returns INVALID_REQUEST with message Add voter request didn't include a valid voter

  Scenario: AddVoter rejects cluster leadership and voter defects in Kafka order (7: boundary missing controller listener, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an AddVoter request names cluster absent, voter 3, directory defined, and listener PLAINTEXT
    When Kafka AddVoter admission checks are applied
    Then voter request admission returns INVALID_REQUEST with message Add voter request didn't include the endpoint for the default listener CONTROLLER

  Scenario: AddVoter rejects cluster leadership and voter defects in Kafka order (8: boundary explicit local cluster, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an AddVoter request names cluster local, voter 3, directory defined, and listener controller
    When Kafka AddVoter admission checks are applied
    Then voter request admission returns NONE with message absent

  Scenario: AddVoter rejects cluster leadership and voter defects in Kafka order (9: boundary no cluster identity, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an AddVoter request names cluster absent, voter 3, directory defined, and listener controller
    When Kafka AddVoter admission checks are applied
    Then voter request admission returns NONE with message absent

  Scenario: RemoveVoter validates cluster and leadership before the voter key (1: boundary foreign cluster, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And a RemoveVoter request names cluster foreign, voter 2, and directory defined
    When Kafka RemoveVoter admission checks are applied
    Then voter request admission returns INCONSISTENT_CLUSTER_ID with message The given id "cluster-b" doesn't match the cluster id "cluster-a"

  Scenario: RemoveVoter validates cluster and leadership before the voter key (2: boundary follower with invalid voter, leader false)
    Given the local quorum is at epoch 7 with leadership false
    And a RemoveVoter request names cluster absent, voter -1, and directory defined
    When Kafka RemoveVoter admission checks are applied
    Then voter request admission returns NOT_LEADER_OR_FOLLOWER with message absent

  Scenario: RemoveVoter validates cluster and leadership before the voter key (3: boundary negative voter, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And a RemoveVoter request names cluster absent, voter -1, and directory defined
    When Kafka RemoveVoter admission checks are applied
    Then voter request admission returns INVALID_REQUEST with message Remove voter request didn't include a valid voter

  Scenario: RemoveVoter validates cluster and leadership before the voter key (4: boundary undefined directory, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And a RemoveVoter request names cluster absent, voter 2, and directory undefined
    When Kafka RemoveVoter admission checks are applied
    Then voter request admission returns INVALID_REQUEST with message Remove voter request didn't include a valid voter

  Scenario: RemoveVoter validates cluster and leadership before the voter key (5: boundary valid local voter, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And a RemoveVoter request names cluster local, voter 2, and directory defined
    When Kafka RemoveVoter admission checks are applied
    Then voter request admission returns NONE with message absent

  Scenario: UpdateVoter validates epochs feature ranges and controller endpoints (1: boundary foreign cluster, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an UpdateVoter request names cluster foreign, epoch 7, directory defined, kraft minimum 0, kraft maximum 1, and listener CONTROLLER
    When Kafka UpdateVoter admission checks are applied
    Then UpdateVoter admission returns INCONSISTENT_CLUSTER_ID

  Scenario: UpdateVoter validates epochs feature ranges and controller endpoints (2: boundary old epoch, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an UpdateVoter request names cluster absent, epoch 6, directory defined, kraft minimum 0, kraft maximum 1, and listener CONTROLLER
    When Kafka UpdateVoter admission checks are applied
    Then UpdateVoter admission returns FENCED_LEADER_EPOCH

  Scenario: UpdateVoter validates epochs feature ranges and controller endpoints (3: boundary unknown epoch, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an UpdateVoter request names cluster absent, epoch 8, directory defined, kraft minimum 0, kraft maximum 1, and listener CONTROLLER
    When Kafka UpdateVoter admission checks are applied
    Then UpdateVoter admission returns UNKNOWN_LEADER_EPOCH

  Scenario: UpdateVoter validates epochs feature ranges and controller endpoints (4: boundary follower, leader false)
    Given the local quorum is at epoch 7 with leadership false
    And an UpdateVoter request names cluster absent, epoch 7, directory defined, kraft minimum 0, kraft maximum 1, and listener CONTROLLER
    When Kafka UpdateVoter admission checks are applied
    Then UpdateVoter admission returns NOT_LEADER_OR_FOLLOWER

  Scenario: UpdateVoter validates epochs feature ranges and controller endpoints (5: boundary undefined directory, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an UpdateVoter request names cluster absent, epoch 7, directory undefined, kraft minimum 0, kraft maximum 1, and listener CONTROLLER
    When Kafka UpdateVoter admission checks are applied
    Then UpdateVoter admission returns INVALID_REQUEST

  Scenario: UpdateVoter validates epochs feature ranges and controller endpoints (6: boundary negative feature minimum, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an UpdateVoter request names cluster absent, epoch 7, directory defined, kraft minimum -1, kraft maximum 1, and listener CONTROLLER
    When Kafka UpdateVoter admission checks are applied
    Then UpdateVoter admission returns INVALID_REQUEST

  Scenario: UpdateVoter validates epochs feature ranges and controller endpoints (7: boundary inverted feature range, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an UpdateVoter request names cluster absent, epoch 7, directory defined, kraft minimum 2, kraft maximum 1, and listener CONTROLLER
    When Kafka UpdateVoter admission checks are applied
    Then UpdateVoter admission returns INVALID_REQUEST

  Scenario: UpdateVoter validates epochs feature ranges and controller endpoints (8: boundary missing controller endpoint, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an UpdateVoter request names cluster absent, epoch 7, directory defined, kraft minimum 0, kraft maximum 1, and listener PLAINTEXT
    When Kafka UpdateVoter admission checks are applied
    Then UpdateVoter admission returns INVALID_REQUEST

  Scenario: UpdateVoter validates epochs feature ranges and controller endpoints (9: boundary valid update, leader true)
    Given the local quorum is at epoch 7 with leadership true
    And an UpdateVoter request names cluster absent, epoch 7, directory defined, kraft minimum 0, kraft maximum 1, and listener CONTROLLER
    When Kafka UpdateVoter admission checks are applied
    Then UpdateVoter admission returns NONE

  Scenario Outline: Reconfiguration outcomes preserve Kafka response codes and messages
    Given an AddVoter operation for voter 3 ends with <outcome>
    When the reconfiguration result is translated into a Kafka response
    Then the reconfiguration response is <code> with message <message>

    Examples:
      | outcome | code | message |
      | committed | NONE | absent |
      | not-leader | NOT_LEADER_OR_FOLLOWER | absent |
      | candidate-lag-10 | REQUEST_TIMED_OUT | Aborted add voter operation for ReplicaKey(id=3, directoryId=CQkJCQkJCQkJCQkJCQkJCQ) since it is lagging behind |
      | pending-change | REQUEST_TIMED_OUT | Request timed out waiting for leader to handle previous voter change request |
      | duplicate-voter | DUPLICATE_VOTER | The voter id for ReplicaKey(id=3, directoryId=CQkJCQkJCQkJCQkJCQkJCQ) is already part of the set of voters |
      | unknown-voter | VOTER_NOT_FOUND | Cannot remove voter ReplicaKey(id=3, directoryId=CQkJCQkJCQkJCQkJCQkJCQ) from the set of voters |
      | rejected-empty-quorum | INVALID_REQUEST | empty voter set |
