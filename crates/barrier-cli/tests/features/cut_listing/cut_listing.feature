Feature: Operators read back the cuts a group retains
  A group keeps its newest cuts up to its retention, and listing prints them
  with the offsets each one names. KFC-4 is the specification:
  docs/KFCs/KFC-4-cross-topic-snapshots.md.

  Rule: Cuts are listed oldest first

    Scenario: Three cuts are listed in epoch order
      Given a barrier group over the orders topic has been cut 3 times
      When the operator lists the cuts of the group
      Then the listed epochs are 1, 2 and 3

    Scenario: Asking for one cut returns the oldest retained cut
      Given a barrier group over the orders topic has been cut 3 times
      When the operator lists 1 cut of the group
      Then the only listed epoch is 1

  Rule: A listing from an epoch includes that epoch

    Scenario: Listing from epoch 2 starts at epoch 2
      Given a barrier group over the orders topic has been cut 3 times
      When the operator lists the cuts of the group from epoch 2
      Then the listed epochs are 2 and 3

  Rule: Retention drops the oldest cuts beyond the limit

    Scenario: A third cut drops the oldest of a group that retains two
      Given a barrier group over the orders topic that retains 2 cuts has been cut 3 times
      When the operator lists the cuts of the group
      Then the listed epochs are 2 and 3

    Scenario: A group cut exactly as often as it retains keeps every cut
      Given a barrier group over the orders topic that retains 2 cuts has been cut 2 times
      When the operator lists the cuts of the group
      Then the listed epochs are 1 and 2
