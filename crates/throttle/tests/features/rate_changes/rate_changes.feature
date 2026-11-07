Feature: A throttle rate change applies to later traffic

  An operator changes a throttle rate with kafka-configs, for example to speed
  up a reassignment, and the broker applies the new dynamic broker config to
  the running bucket. The change does not reset what the bucket has already
  metered. Kafka's ReplicationQuotaManager.updateQuota swaps the quota bound
  and keeps the recorded samples.

  Rule: A rate change keeps the balance already there

    Scenario: Raising the rate does not refill a spent throttle
      Given log directory move traffic has spent a throttle of 1000 bytes per second
      And the log directory move throttle rate has been changed to 2000 bytes per second
      When log directory move traffic asks for 5000 bytes
      Then log directory move traffic is granted 0 bytes

  Rule: The new rate refills the bucket from the change on

    Scenario: A spent throttle refills at the raised rate
      Given log directory move traffic has spent a throttle of 1000 bytes per second
      And the log directory move throttle rate has been changed to 2000 bytes per second
      And 250 ms have passed
      When log directory move traffic asks for 5000 bytes
      Then log directory move traffic is granted 500 bytes

  Rule: Lowering the rate caps the balance at the new burst

    Scenario: A full throttle keeps only one second of the lowered rate
      Given log directory move traffic is throttled at 1000 bytes per second
      And the log directory move throttle rate has been changed to 200 bytes per second
      When log directory move traffic asks for 5000 bytes
      Then log directory move traffic is granted 200 bytes

  Rule: A rate change keeps the replication debt

    Scenario: A leader in debt repays it at the new rate
      Given leader replication traffic is throttled at 1000 bytes per second
      And leader replication traffic has sent 1500 bytes
      When the leader replication throttle rate changes to 500 bytes per second
      Then the leader replication quota stays exceeded for 1000 ms

  Rule: Removing the throttle rate lifts the throttle at once

    Scenario: A leader in debt is under quota once its throttle is removed
      Given leader replication traffic is throttled at 1000 bytes per second
      And leader replication traffic has sent 1500 bytes
      When the leader replication throttle is removed
      Then the leader replication quota is not exceeded
