Feature: Operators verify a cut against the log
  A cut is a list of offsets, and it is worth what the markers behind it are.
  Verifying reads the log at every offset a cut names and checks that each
  holds that cut's marker. A cut the log does not back ends on exit code 3,
  which a runbook branches on. KFC-4 is the specification:
  docs/KFCs/KFC-4-cross-topic-snapshots.md.

  Rule: A cut verifies when every offset it names holds its marker

    Scenario: A fresh cut across three partitions verifies
      Given the orders topic has 3 partitions
      And a barrier group over the orders topic has been cut once
      When the operator verifies cut 1 of the group
      Then the cut verifies with 3 markers in the log

  Rule: A cut whose marker has left the log does not match the log

    Scenario: A cut whose marker in one partition was deleted does not verify
      Given the orders topic has 3 partitions
      And a barrier group over the orders topic has been cut once
      And the records of orders partition 1 up to the marker have been deleted
      When the operator verifies cut 1 of the group
      Then the cut does not match the log because orders partition 1 no longer holds the marker

  Rule: Every offset the log does not back is reported, not only the first

    Scenario: Two deleted markers of three are both reported
      Given the orders topic has 3 partitions
      And a barrier group over the orders topic has been cut once
      And the records of orders partitions 0 and 2 up to the marker have been deleted
      When the operator verifies cut 1 of the group
      Then the tool reports 2 of 3 offsets as wrong

  Rule: A marker of another epoch does not back a cut

    Scenario: A later cut's marker at the same offset does not back the earlier cut
      Given a barrier group over the orders topic has been cut once
      And the orders topic has been created again and cut since
      When the operator verifies cut 1 of the group
      Then the cut does not match the log because the marker carries epoch 2

  Rule: An epoch the group never took is refused, not reported as a mismatch

    Scenario: Verifying an epoch beyond the newest cut is refused
      Given a barrier group over the orders topic has been cut once
      When the operator tries to verify cut 99 of the group
      Then the tool refuses because the group has no retained cut at epoch 99
