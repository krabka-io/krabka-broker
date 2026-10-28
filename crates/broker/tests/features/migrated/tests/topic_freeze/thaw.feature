Feature: Signed two-person approval lifts a topic freeze
  Scenario: a thaw restores writes
    Given Alice froze orders after one record and Bob and Carol are independent approvers
    When Alice proposes thawing the literal orders scope
    And approver bob approves Alice's thaw proposal
    Then the thaw proposal holds 1 independent approvals
    When approver carol approves Alice's thaw proposal
    Then the thaw proposal holds 2 independent approvals
    When Alice submits a signed thaw naming the approved proposal
    Then the approved thaw succeeds and the freeze registry becomes empty
    When clients write to thawed orders and its unfrozen control
    Then orders accepts its second record and the control accepts its first
