Feature: Kafka voter identity, admission and reconfiguration responses

  Rule: A replica key names its directory in Kafka form

    Scenario: An undefined directory uses Kafka's undefined replica-key marker
      Given voter 3 has a undefined directory identity
      When the Kafka replica key is formatted
      Then the formatted replica key is ReplicaKey(id=3, directoryId=<undefined>)

    Scenario: A real directory is encoded in Kafka base64 form
      Given voter 3 has a defined directory identity
      When the Kafka replica key is formatted
      Then the formatted replica key is ReplicaKey(id=3, directoryId=CQkJCQkJCQkJCQkJCQkJCQ)

  Rule: AddVoter checks the cluster, then leadership, then the voter

    Scenario Outline: AddVoter rejects cluster leadership and voter defects in Kafka order
      Given the local quorum is at epoch 7 with leadership <leader>
      And an AddVoter request names cluster <cluster>, voter <voter>, directory <directory>, and listener <listener>
      When Kafka AddVoter admission checks are applied
      Then voter request admission returns <code> with message <message>

      Examples:
        | case                               | leader | cluster | voter | directory | listener   | code                    | message                                                                                      |
        | a foreign cluster on a follower    | false  | foreign | 3     | defined   | controller | INCONSISTENT_CLUSTER_ID | The given id "cluster-b" doesn't match the cluster id "cluster-a"                            |
        | a follower                         | false  | absent  | 3     | defined   | controller | NOT_LEADER_OR_FOLLOWER  | absent                                                                                       |
        | an invalid voter on a follower     | false  | absent  | -1    | defined   | controller | NOT_LEADER_OR_FOLLOWER  | absent                                                                                       |
        | a negative voter                   | true   | absent  | -1    | defined   | controller | INVALID_REQUEST         | Add voter request didn't include a valid voter                                               |
        | an undefined directory             | true   | absent  | 3     | undefined | controller | INVALID_REQUEST         | Add voter request didn't include a valid voter                                               |
        | no listeners                       | true   | absent  | 3     | defined   | absent     | INVALID_REQUEST         | Add voter request didn't include a valid voter                                               |
        | a missing controller listener      | true   | absent  | 3     | defined   | PLAINTEXT  | INVALID_REQUEST         | Add voter request didn't include the endpoint for the default listener CONTROLLER            |
        | an explicit local cluster          | true   | local   | 3     | defined   | controller | NONE                    | absent                                                                                       |
        | no cluster identity                | true   | absent  | 3     | defined   | controller | NONE                    | absent                                                                                       |

  Rule: RemoveVoter checks the cluster and leadership before the voter key

    Scenario Outline: RemoveVoter validates cluster and leadership before the voter key
      Given the local quorum is at epoch 7 with leadership <leader>
      And a RemoveVoter request names cluster <cluster>, voter <voter>, and directory <directory>
      When Kafka RemoveVoter admission checks are applied
      Then voter request admission returns <code> with message <message>

      Examples:
        | case                               | leader | cluster | voter | directory | code                    | message                                                           |
        | a foreign cluster                  | true   | foreign | 2     | defined   | INCONSISTENT_CLUSTER_ID | The given id "cluster-b" doesn't match the cluster id "cluster-a" |
        | a follower with an invalid voter   | false  | absent  | -1    | defined   | NOT_LEADER_OR_FOLLOWER  | absent                                                            |
        | a negative voter                   | true   | absent  | -1    | defined   | INVALID_REQUEST         | Remove voter request didn't include a valid voter                 |
        | an undefined directory             | true   | absent  | 2     | undefined | INVALID_REQUEST         | Remove voter request didn't include a valid voter                 |
        | a valid local voter                | true   | local   | 2     | defined   | NONE                    | absent                                                            |

  Rule: UpdateVoter checks epochs, feature ranges and the controller endpoint

    Scenario Outline: UpdateVoter validates epochs feature ranges and controller endpoints
      Given the local quorum is at epoch 7 with leadership <leader>
      And an UpdateVoter request names cluster <cluster>, epoch <epoch>, directory <directory>, kraft minimum <minimum>, kraft maximum <maximum>, and listener <listener>
      When Kafka UpdateVoter admission checks are applied
      Then UpdateVoter admission returns <code>

      Examples:
        | case                              | leader | cluster | epoch | directory | minimum | maximum | listener   | code                    |
        | a foreign cluster                 | true   | foreign | 7     | defined   | 0       | 1       | CONTROLLER | INCONSISTENT_CLUSTER_ID |
        | an old epoch                      | true   | absent  | 6     | defined   | 0       | 1       | CONTROLLER | FENCED_LEADER_EPOCH     |
        | an unknown epoch                  | true   | absent  | 8     | defined   | 0       | 1       | CONTROLLER | UNKNOWN_LEADER_EPOCH    |
        | a follower                        | false  | absent  | 7     | defined   | 0       | 1       | CONTROLLER | NOT_LEADER_OR_FOLLOWER  |
        | an undefined directory            | true   | absent  | 7     | undefined | 0       | 1       | CONTROLLER | INVALID_REQUEST         |
        | a negative feature minimum        | true   | absent  | 7     | defined   | -1      | 1       | CONTROLLER | INVALID_REQUEST         |
        | an inverted feature range         | true   | absent  | 7     | defined   | 2       | 1       | CONTROLLER | INVALID_REQUEST         |
        | a missing controller endpoint     | true   | absent  | 7     | defined   | 0       | 1       | PLAINTEXT  | INVALID_REQUEST         |
        | a valid update                    | true   | absent  | 7     | defined   | 0       | 1       | CONTROLLER | NONE                    |

  Rule: A reconfiguration outcome keeps its Kafka response code and message

    Scenario Outline: Reconfiguration outcomes preserve Kafka response codes and messages
      Given an AddVoter operation for voter 3 ends with <outcome>
      When the reconfiguration result is translated into a Kafka response
      Then the reconfiguration response is <code> with message <message>

      Examples:
        | case                          | outcome | code | message |
        | a committed change            | committed | NONE | absent |
        | a leader that lost leadership | not-leader | NOT_LEADER_OR_FOLLOWER | absent |
        | a candidate lagging behind    | candidate-lag-10 | REQUEST_TIMED_OUT | Aborted add voter operation for ReplicaKey(id=3, directoryId=CQkJCQkJCQkJCQkJCQkJCQ) since it is lagging behind |
        | a pending voter change        | pending-change | REQUEST_TIMED_OUT | Request timed out waiting for leader to handle previous voter change request |
        | a duplicate voter             | duplicate-voter | DUPLICATE_VOTER | The voter id for ReplicaKey(id=3, directoryId=CQkJCQkJCQkJCQkJCQkJCQ) is already part of the set of voters |
        | an unknown voter              | unknown-voter | VOTER_NOT_FOUND | Cannot remove voter ReplicaKey(id=3, directoryId=CQkJCQkJCQkJCQkJCQkJCQ) from the set of voters |
        | an empty voter set            | rejected-empty-quorum | INVALID_REQUEST | empty voter set |

  Rule: UNSUPPORTED_VERSION names the voter change and the finalized kraft.version

    Scenario Outline: A voter change the finalized kraft.version cannot carry is refused by name
      Given a voter change <operation> for voter 3 is refused at kraft.version <level>
      When the reconfiguration result is translated into a Kafka response
      Then the reconfiguration response is UNSUPPORTED_VERSION with message <message>

      Examples:
        | case                             | operation   | level | message                                                                       |
        | an AddVoter at kraft.version 0   | AddVoter    | 0     | Cluster doesn't support adding voter because the kraft.version feature is 0   |
        | a RemoveVoter at kraft.version 0 | RemoveVoter | 0     | Cluster doesn't support removing voter because the kraft.version feature is 0 |
        | a finalized level above one      | AddVoter    | 2     | Cluster doesn't support adding voter because the kraft.version feature is 2   |
