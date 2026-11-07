Feature: A broker meters three kinds of throttled traffic separately

  A broker holds one throttle for each kind of throttled traffic, and each
  takes its rate from one dynamic broker config:

  - leader replication traffic: leader.replication.throttled.rate (KIP-73)
  - follower replication traffic: follower.replication.throttled.rate (KIP-73)
  - log directory move traffic: replica.alter.log.dirs.io.max.bytes.per.second (KIP-113)

  The broker's throttle refresh, in crates/broker/src/throttle/refresh.rs, sets
  each rate from the metadata image, and its tests check the config names.

  Rule: A broker starts with every throttle lifted

    Scenario Outline: A new broker meters none of its throttled traffic
      When <traffic> traffic asks for 1000000000 bytes
      Then <traffic> traffic is granted 1000000000 bytes

      Examples:
        | case                    | traffic              |
        | leader replication      | leader replication   |
        | follower replication    | follower replication |
        | log directory move      | log directory move   |

  Rule: Each kind of traffic draws on its own throttle

    Scenario: Spent leader replication leaves follower replication its own burst
      Given leader replication traffic has spent a throttle of 1000 bytes per second
      And follower replication traffic is throttled at 1000 bytes per second
      When follower replication traffic asks for 5000 bytes
      Then follower replication traffic is granted 1000 bytes
