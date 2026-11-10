Feature: Finalized feature levels gate broker behavior

  Rule: share.version 1 turns on share groups and share.version 2 the share-group dead-letter queue

    Scenario Outline: The share-group gates follow the finalized share.version
      Given an image finalizes share.version at <level>
      When the broker reads its share-group gates
      Then the open share-group gates are <gates>

      Examples:
        | case                                | level | gates                        |
        | an unfinalized share.version is off | none  | none                         |
        | level 0 is off                      | 0     | none                         |
        | level 1 turns on share groups       | 1     | share-groups                 |
        | level 2 adds the dead-letter queue  | 2     | share-groups-and-dead-letter |

  Rule: CIDR ACL hosts need metadata.version 4.4-IV1, and an image without one is judged at the bootstrap level

    Scenario Outline: The CIDR host gate follows the finalized metadata.version
      Given an image finalizes metadata.version at <level>
      When the broker reads its CIDR ACL host gate
      Then CIDR ACL hosts are <support>

      Examples:
        | case                                    | level | support  |
        | an unfinalized image uses the bootstrap | none  | accepted |
        | 4.4-IV0 refuses them                    | 31    | refused  |
        | 4.4-IV1 admits them                     | 32    | accepted |

  Rule: A metadata.version gate refuses only an image finalized below the level it needs

    Scenario Outline: A metadata.version gate compares the finalized level with its own
      Given an image finalizes metadata.version at <level>
      When an API that needs metadata.version 11 checks its gate
      Then the gate answers <outcome>

      Examples:
        | case                                    | level | outcome             |
        | an image without the feature passes     | none  | ok                  |
        | one level below the requirement refuses | 10    | unsupported-version |
        | the required level passes               | 11    | ok                  |
        | a level above the requirement passes    | 12    | ok                  |
