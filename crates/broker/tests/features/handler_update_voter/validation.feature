Feature: UpdateRaftVoter refusals preserve current leader information
  Scenario Outline: Invalid voter fields retain Kafka error codes and leader hints
    Given an authorized UpdateRaftVoter request names voter 2 and the current cluster and leader epoch
    And the voter update contains <malformation>
    When the broker handles the version 0 voter update
    Then the voter update returns error <code> and still identifies the current leader and epoch
    Examples:
      | malformation                    | code |
      | another cluster ID              | 104  |
      | an epoch one behind             | 74   |
      | an epoch one ahead              | 75   |
      | a zero voter directory ID       | 42   |
      | no listeners                    | 42   |
      | an inverted kraft.version range | 42   |
      | only a PLAINTEXT listener        | 42   |
