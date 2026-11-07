Feature: The replication quota measures throttled traffic over its sample window

  KIP-73 and KIP-113. Before a broker reads throttled replica traffic, it asks
  whether the quota is exceeded. After the read it records the bytes it moved,
  whatever the quota holds. This is Kafka's ReplicationQuotaManager:
  isQuotaExceeded before the read, record after it.

  The quota measures a SimpleRate: the bytes recorded in the samples it keeps,
  divided by the time since the oldest sample started, and never by less than
  one sample window. The quota is exceeded while that rate is above the bound.
  A broker keeps Kafka's default replication quota window: 11 samples
  (replication.quota.window.num) of 1 second
  (replication.quota.window.size.seconds) each. A sample with no traffic in the
  last 11 seconds is forgotten.

  Rule: Traffic up to the bound leaves the quota open

    Scenario Outline: One second of traffic at most stays within the quota
      Given leader replication traffic is throttled at 1000 bytes per second
      When leader replication traffic sends <sent> bytes
      Then the leader replication quota is not exceeded

      Examples:
        | case                          | sent |
        | less than one second's worth  | 999  |
        | exactly one second's worth    | 1000 |

  Rule: Traffic above the bound exceeds the quota

    Scenario: A leader that sends one byte more than one second's worth exceeds its quota
      Given leader replication traffic is throttled at 1000 bytes per second
      When leader replication traffic sends 1001 bytes
      Then the leader replication quota is exceeded

  Rule: The quota stays exceeded until the measured rate falls back to the bound

    Scenario Outline: The quota reopens once the elapsed time covers the traffic at the bound
      Given leader replication traffic is throttled at <rate> bytes per second
      When leader replication traffic sends <sent> bytes
      Then the leader replication quota stays exceeded for <delay> ms

      Examples:
        | case                                | rate | sent | delay |
        | half a second over                  | 1000 | 1500 | 1500  |
        | three seconds of traffic at once    | 1000 | 3000 | 3000  |
        | a faster bound reopens sooner       | 4000 | 8000 | 2000  |

  Rule: Recorded traffic is forgotten after the quota window

    Scenario: A batch far beyond the bound throttles for one quota window at most
      Given leader replication traffic is throttled at 1000 bytes per second
      When leader replication traffic sends 20000 bytes
      Then the leader replication quota stays exceeded for 11000 ms

  Rule: A bound of zero bytes per second admits no recorded traffic

    Scenario: A zero bound is open until traffic is recorded
      When leader replication traffic is throttled at 0 bytes per second
      Then the leader replication quota is not exceeded

    Scenario: A zero bound is exceeded by a single byte
      Given leader replication traffic is throttled at 0 bytes per second
      When leader replication traffic sends 1 bytes
      Then the leader replication quota stays exceeded for 11000 ms

  Rule: Replica traffic without a throttle rate is never over quota

    Scenario: A leader with no throttle rate never exceeds its quota
      When leader replication traffic sends 1000000000 bytes
      Then the leader replication quota is not exceeded
