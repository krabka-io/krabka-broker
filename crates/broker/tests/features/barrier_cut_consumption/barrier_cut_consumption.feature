Feature: An ordinary Kafka consumer reads a published cut
  The barrier coordinator publishes every cut as a record of the
  __barrier_state topic, so a client in any language reads a cut with a plain
  Kafka consumer and the persisted cut format, and needs none of the barrier
  requests. KFC-4 is the specification:
  docs/KFCs/KFC-4-cross-topic-snapshots.md.

  The broker here runs without an authorizer, so the consumer needs no ACL.
  On a cluster with an authorizer, it needs Read on the __barrier_state topic
  and Read on the consumer group it joins.

  Rule: The cut record holds the offsets that the trigger answered with

    Scenario: A consumer of __barrier_state reads the cut that a trigger returned
      Given the orders topic has 2 partitions
      And an operator has defined a barrier group over the orders topic
      And the operator has triggered a cut of the group
      When a plain Kafka consumer reads __barrier_state from the beginning
      Then the cut record it reads holds the epoch and the offsets of the trigger response
