Feature: The replication quota tracks throttled replica traffic

  KIP-73. Before a leader reads the partitions listed in
  leader.replication.throttled.replicas for a follower, it asks whether its
  leader replication quota is exceeded. After the read it records the bytes it
  sent, whatever the quota holds. A follower does the same for its follower
  replication quota. This is Kafka's ReplicationQuotaManager: isQuotaExceeded
  before the read, record after it.

  A throttle rate gives the quota a burst of one second of traffic. A broker
  records replication traffic against Kafka's default replication quota window
  of 11 seconds: replication.quota.window.num 11 times
  replication.quota.window.size.seconds 1.

  Rule: Traffic inside the burst leaves the quota open

    Scenario: A leader that sends less than one second of traffic stays under its quota
      Given leader replication traffic is throttled at 1000 bytes per second
      When leader replication traffic sends 999 bytes
      Then the leader replication quota is not exceeded

  Rule: Spending the whole burst exceeds the quota

    Scenario: A leader that sends exactly one second of traffic exceeds its quota
      Given leader replication traffic is throttled at 1000 bytes per second
      When leader replication traffic sends 1000 bytes
      Then the leader replication quota is exceeded

  Rule: Traffic past the balance is charged in full and keeps the quota exceeded until the rate repays it

    Scenario Outline: The quota stays exceeded for the deficit over the rate
      Given leader replication traffic is throttled at <rate> bytes per second
      When leader replication traffic sends <sent> bytes
      Then the leader replication quota stays exceeded for <delay> ms

      Examples:
        | case                               | rate | sent | delay |
        | one byte past the burst            | 1000 | 1001 | 1     |
        | half a burst past the burst        | 1000 | 1500 | 500   |
        | a faster rate repays a larger debt | 4000 | 8000 | 1000  |

  Rule: Replication debt never outlasts one replication quota window

    Scenario: A batch far beyond the burst throttles for one quota window at most
      Given leader replication traffic is throttled at 1000 bytes per second
      When leader replication traffic sends 20000 bytes
      Then the leader replication quota stays exceeded for 11000 ms

  Rule: Replica traffic without a throttle rate is never over quota

    Scenario: A leader with no throttle rate never exceeds its quota
      When leader replication traffic sends 1000000000 bytes
      Then the leader replication quota is not exceeded
