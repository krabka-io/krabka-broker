Feature: Lifting a freeze spends an approved proposal for exactly that scope

  The two-person rule that approves a proposal is in two_person_rule/. A thaw
  also needs a signature, and signing/ covers a thaw that carries none.

  Rule: A proposal two approvers approved lifts the freeze it names

    Scenario: An approved thaw lifts the freeze on the payments topic
      Given alice has frozen the payments topic
      And alice holds a proposal to thaw literal:payments with 2 approvals
      When alice thaws the payments topic with that proposal
      Then the registry holds no freeze

    Scenario Outline: A thaw before the second approval reports that an approval is missing
      Given alice has frozen the payments topic
      And alice holds a proposal to thaw literal:payments with <approvals> approvals
      When alice tries to thaw the payments topic with that proposal
      Then the tool reports a missing approval
      And the registry still holds the freeze on the payments topic

      Examples:
        | case            | approvals |
        | nobody approved | 0         |
        | one approval    | 1         |

  Rule: A thaw proposal names its target with the pattern type of the freeze

    Scenario: A proposal that names the bare topic does not authorize its thaw
      Given alice has frozen the payments topic
      And alice holds a proposal to thaw payments with 2 approvals
      When alice tries to thaw the payments topic with that proposal
      Then the tool reports a missing approval

  Rule: A thaw lifts only the registry entry it names exactly

    Scenario: Thawing one topic under a frozen prefix leaves the prefix frozen
      Given alice has frozen every topic starting with tenant-a.
      And alice holds a proposal to thaw literal:tenant-a.orders with 2 approvals
      When alice tries to thaw the tenant-a.orders topic with that proposal
      Then the tool explains that the registry holds no freeze on the tenant-a.orders topic
      And the registry still holds the freeze on every topic starting with tenant-a.

  Rule: A spent proposal cannot be reused

    Scenario: A proposal that already lifted a freeze cannot lift the next one
      Given alice has lifted a freeze on the payments topic with an approved proposal
      And alice has refrozen the payments topic
      When alice tries to thaw the payments topic with the same proposal
      Then the tool reports a missing approval
