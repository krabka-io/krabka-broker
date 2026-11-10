Feature: ShareGroupHeartbeat gates on share.version and ACLs, validates first requests and creates share groups only for joins

  Rule: A share.version of 0 refuses every heartbeat before the group ACL is checked

    Scenario Outline: A heartbeat gets UNSUPPORTED_VERSION while share groups are off
      Given a principal holds <grants>
      And share.version is finalized at 0
      When a member sends a first heartbeat for group g subscribing to topic-a
      Then the heartbeat is refused with UNSUPPORTED_VERSION
      Examples:
        | case                                   | grants                                  |
        | a principal without group Read         | no ACLs                                 |
        | a principal with every needed ACL      | Read on group g and Describe on topic-a |

  Rule: A heartbeat needs Read on the group

    Scenario: A principal without group Read is refused
      Given a principal holds no ACLs
      When a member sends a first heartbeat for group g subscribing to topic-a
      Then the heartbeat is refused with GROUP_AUTHORIZATION_FAILED

  Rule: The member id is checked before the subscribed topics are authorized

    Scenario: An empty member id is invalid even with an undescribable topic
      Given a principal holds Read on group g
      When a member with an empty member id sends a heartbeat at epoch 5 for group g subscribing to topic-b
      Then the heartbeat is refused with INVALID_REQUEST

  Rule: A subscribed topic the principal cannot describe refuses the whole heartbeat

    Scenario: One undescribable topic refuses the heartbeat and adds no member
      Given a principal holds Read on group g and Describe on topic-a
      When a member sends a first heartbeat for group g subscribing to topic-a and topic-b
      Then the heartbeat is refused with TOPIC_AUTHORIZATION_FAILED
      And group g has 0 members

    Scenario: Describable topics admit the member
      Given a principal holds Read on group g and Describe on topic-a
      And topic-a exists
      When a member sends a first heartbeat for group g subscribing to topic-a
      Then the heartbeat is accepted
      And group g has 1 member

  Rule: A malformed first heartbeat is invalid and creates no group

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

  Rule: Only a join creates a share group and a group of another kind stays distinct

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
