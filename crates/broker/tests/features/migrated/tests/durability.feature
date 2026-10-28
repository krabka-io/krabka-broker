Feature: tests durability

  Scenario: idempotent retry reappends after truncation instead of stalling
    Then durability: idempotent retry reappends after truncation instead of stalling

  Scenario: disk backed restart recovers idempotent producer state
    Then durability: disk backed restart recovers idempotent producer state

  Scenario: acks one returns quickly on rf1 broker
    Then durability: acks one returns quickly on rf1 broker

  Scenario: acks all returns quickly on rf1 broker
    Then durability: acks all returns quickly on rf1 broker

  Scenario: consumer clamps at hw when followers lag
    Then durability: consumer clamps at hw when followers lag

  Scenario: read committed under rf1 unchanged
    Then durability: read committed under rf1 unchanged

  Scenario: acks all completes via isr shrink when follower dead
    Then durability: acks all completes via isr shrink when follower dead

  Scenario: acks all stalled follower wait lands in remote time not local time
    Then durability: acks all stalled follower wait lands in remote time not local time

  Scenario: acks all waits for every partition at once not one after another
    Then durability: acks all waits for every partition at once not one after another
