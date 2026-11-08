Feature: A throttle rate change applies to the traffic already measured

  An operator changes a throttle rate with kafka-configs, for example to speed
  up a reassignment, and the broker applies the new dynamic broker config to
  the running quota. Kafka's ReplicationQuotaManager.updateQuota swaps the
  quota bound and keeps the recorded samples, so the new bound judges the
  traffic already measured.

  Rule: Raising the rate judges the measured traffic against the new bound

    Scenario: A leader over its quota is under the raised bound at once
      Given leader replication traffic is throttled at 1000 bytes per second
      And leader replication traffic has sent 1500 bytes
      When the leader replication throttle rate changes to 2000 bytes per second
      Then the leader replication quota is not exceeded

  Rule: Lowering the rate judges the measured traffic against the new bound

    Scenario: A leader within its quota is over the lowered bound until the rate falls
      Given leader replication traffic is throttled at 1000 bytes per second
      And leader replication traffic has sent 1000 bytes
      When the leader replication throttle rate changes to 500 bytes per second
      Then the leader replication quota stays exceeded for 2000 ms

  Rule: Removing the throttle rate lifts the throttle at once

    Scenario: A leader over its quota is unthrottled once its throttle is removed
      Given leader replication traffic is throttled at 1000 bytes per second
      And leader replication traffic has sent 1500 bytes
      When the leader replication throttle is removed
      Then the leader replication quota is not exceeded
