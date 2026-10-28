Feature: ShareGroupHeartbeat validates first requests and creates share groups only for joins
  Scenario Outline: Malformed member identity join subscription and epoch fields cannot create a group
    Given an authorized share coordinator and a joining member-1 heartbeat for group g
    And the share heartbeat field <field> is <value>
    When the share coordinator handles the member heartbeat
    Then the malformed share heartbeat returns code 42 message <message> and creates no share group
    Examples:
      | field        | value         | message                                                 |
      | member ID    | empty         | null                                                    |
      | member ID    | 37 characters | null                                                    |
      | member ID    | blank         | MemberId can't be empty.                                |
      | group ID     | empty         | GroupId can't be empty.                                 |
      | group ID     | space-and-tab | GroupId can't be empty.                                 |
      | rack ID      | empty         | RackId can't be empty.                                  |
      | subscription | null          | SubscribedTopicNames must be set in first request.       |
      | subscription | empty         | SubscribedTopicNames must be set in first request.       |
      | member epoch | -2            | MemberEpoch is invalid.                                 |

  Scenario Outline: Only a join can create a share group and other group kinds remain distinct
    Given the share coordinator contains a classic group a consumer group and a streams marker
    And share member m1 sends epoch <epoch> for group <group>
    When the share coordinator handles the member heartbeat
    Then the share coordinator returns error <code> message <message> and creates only accepted joins
    Examples:
      | epoch | group            | code | message                                     |
      | 3     | absent-heartbeat | 69   | Share group absent-heartbeat not found.      |
      | -1    | absent-leave     | 69   | Group absent-leave not found.                |
      | 0     | classic          | 69   | Group classic is not a share group.         |
      | 3     | consumer         | 69   | Group consumer is not a share group.        |
      | -1    | streams          | 69   | Group streams is not a share group.         |
      | 0     | joined           | 0    | accepted                                    |
