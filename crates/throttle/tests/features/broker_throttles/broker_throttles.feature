Feature: A broker meters three kinds of throttled traffic separately

  A broker holds one replication quota for each kind of throttled traffic, and
  each takes its bound from one dynamic broker config, or from the cluster-wide
  default broker config when the broker sets none:

  - leader replication traffic: leader.replication.throttled.rate (KIP-73)
  - follower replication traffic: follower.replication.throttled.rate (KIP-73)
  - log directory move traffic: replica.alter.log.dirs.io.max.bytes.per.second (KIP-113)

  Each config is a long of at least 0, and an unset config leaves the traffic
  unthrottled, as Kafka's default of Long.MAX_VALUE does. The broker's throttle
  refresh, in crates/broker/src/throttle/refresh.rs, sets each bound from the
  metadata image, and its tests check the config names.

  Rule: A broker starts with every throttle lifted

    Scenario Outline: A new broker meters none of its throttled traffic
      When <traffic> traffic sends 1000000000 bytes
      Then the <traffic> quota is not exceeded

      Examples:
        | case                 | traffic              |
        | leader replication   | leader replication   |
        | follower replication | follower replication |
        | log directory move   | log directory move   |

  Rule: Each kind of traffic is measured by its own quota

    Scenario: Leader replication over its quota leaves follower replication open
      Given leader replication traffic is throttled at 1000 bytes per second
      And follower replication traffic is throttled at 1000 bytes per second
      When leader replication traffic sends 5000 bytes
      Then the follower replication quota is not exceeded
