Feature: Operators define barrier groups
  A barrier group is a named set of topics that the coordinator cuts across
  at one epoch. Defining a group creates it, or replaces the definition of a
  group that is already live. KFC-4 is the specification:
  docs/KFCs/KFC-4-cross-topic-snapshots.md.

  Rule: Defining a group that is already live replaces its definition

    Scenario: Redefining a group over a second topic adds that topic
      Given an operator has defined a barrier group over the orders topic
      When the operator defines the group again over the orders and payments topics
      Then the group cuts across orders and payments

  Rule: A group may name a topic that does not exist yet

    Scenario: A group over a topic nobody has created is accepted
      When an operator defines a barrier group over a topic that does not exist
      Then the tool reports the group as defined

  Rule: A group defined without a cadence is cut on demand only

    Scenario: A group with no cadence is described as on demand only
      Given an operator has defined a barrier group without a cadence
      When the operator describes the group
      Then the group is described as cut on demand only

    Scenario: A cadence in minutes reaches the broker in milliseconds
      Given an operator has defined a barrier group with a cadence of 5m
      When the operator describes the group
      Then the group is described as cut every 300000ms

  Rule: A cadence names its unit

    Scenario: A cadence written as a bare number is refused before anything is sent
      When an operator tries to define a barrier group with a cadence of 90
      Then the tool refuses the command line

  Rule: A group keeps 100 cuts unless the operator names another number

    Scenario: A group defined without a retention keeps 100 cuts
      Given an operator has defined a barrier group without a retention
      When the operator describes the group
      Then the group is described as retaining 100 cuts

  Rule: A group retains at least one cut

    Scenario: A group that retains one cut is accepted
      When an operator defines a barrier group that retains 1 cut
      Then the tool reports the group as defined

    Scenario: A group that retains no cuts is refused
      When an operator tries to define a barrier group that retains 0 cuts
      Then the broker refuses the definition as an out-of-range setting

  Rule: A group names each topic once

    Scenario: A group that names the orders topic twice is refused
      When an operator tries to define a barrier group over the orders topic twice
      Then the broker refuses the definition as a malformed request

  Rule: A group that has never been cut has no last epoch

    Scenario: A group that was never cut reports no last epoch
      Given an operator has defined a barrier group over the orders topic
      When the operator describes the group
      Then the group is described with no last epoch yet

    Scenario: A group cut once reports epoch 1 as its last epoch
      Given a barrier group over the orders topic has been cut once
      When the operator describes the group
      Then the group is described with last epoch 1

  Rule: Deleting a group takes its cuts with it

    Scenario: The cuts of a deleted group can no longer be listed
      Given a barrier group over the orders topic has been cut once
      And the operator has deleted the group
      When the operator lists the cuts of the group
      Then the broker refuses the request as an unknown group
