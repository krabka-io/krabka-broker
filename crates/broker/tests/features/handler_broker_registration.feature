Feature: External broker feature compatibility and log directory identity

  Rule: A registering broker must support every finalized feature level

    Scenario Outline: Finalized feature levels constrain broker registration
      Given the cluster has finalized metadata.version 25 and group.version 1
      And the registering broker advertises feature ranges <ranges>
      When registration checks feature compatibility with finalized levels
      Then registration validation returns error code <code>

      Examples:
        | case                                                         | ranges                                                    | code |
        | every finalized level is supported                           | metadata.version:7:25;group.version:0:1                   | 0    |
        | a metadata.version range below the finalized level           | metadata.version:7:24;group.version:0:1                   | 35   |
        | an unfinalized feature counts as level 0                     | metadata.version:7:25;group.version:0:1;share.version:1:1 | 35   |
        | an unfinalized feature inside a range from 0                 | metadata.version:7:25;group.version:0:1;share.version:0:1 | 0    |
        | no metadata.version range                                    | group.version:0:1                                         | 119  |
        | no feature ranges at all                                     | none                                                      | 119  |
        | a finalized feature the broker does not advertise            | metadata.version:7:25                                     | 35   |
        | an unsupported feature precedes the missing metadata.version | group.version:2:3                                         | 35   |

  Rule: A registering broker names fresh log directory IDs or ones it already owns

    Scenario Outline: Directory IDs must be fresh or already owned by this broker
      Given broker 1 owns directory 500 and broker 2 owns directory 600 at epoch 10
      And broker 2 requests directory IDs <directories>
      When registration checks directory ownership and reserved IDs
      Then registration validation returns error code <code>

      Examples:
        | case                                 | directories | code |
        | one fresh directory                  | 1000        | 0    |
        | several fresh directories            | 1000,1001   | 0    |
        | no directory                         | none        | 119  |
        | the reserved zero ID                 | 0           | 119  |
        | the highest reserved ID              | 99          | 119  |
        | the first unreserved ID              | 100         | 0    |
        | a duplicated directory               | 1000,1000   | 119  |
        | a directory another broker owns      | 1000,500    | 119  |
        | a directory this broker already owns | 600         | 0    |

  Rule: A failed registration write answers NOT_CONTROLLER when another node may lead, INVALID_REGISTRATION when the metadata refuses the record, and UNKNOWN_SERVER_ERROR otherwise

    Scenario Outline: The registration answers Kafka's code for its failed controller write
      Given the controller write of a registration fails with <failure>
      When the controller answers the failed registration write
      Then registration validation returns error code <code>

      Examples:
        | case                                     | failure          | code |
        | a follower is not the leader             | not-leader       | 41   |
        | no leader is known                       | leader-unknown   | 41   |
        | a new leader has not committed its epoch | uncommitted-tail | 41   |
        | the metadata refuses the record          | metadata-refusal | 119  |
        | the controller shuts down                | shutdown         | -1   |
        | the change is rejected                   | change-rejected  | -1   |
