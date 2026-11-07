Feature: A break-glass proposal needs two different people

  The broker enforces the rule, and
  crates/broker/tests/features/migrated/tests/break_glass specifies it. These
  scenarios are what an operator sees through the tool. The cluster requires
  two approvals, and alice, bob and carol are its approvers.

  Rule: An approval counts only from someone other than the proposer

    Scenario: A second approver signs the proposal
      Given alice has proposed deleting the doomed topic
      When bob approves the proposal signed with bob's key
      Then the proposal holds 1 approval, signed by bob

    Scenario: The proposer cannot approve their own proposal
      Given alice has proposed deleting the doomed topic
      When alice tries to approve the proposal
      Then the tool reports a refusal
      And the proposal holds no approval

  Rule: One approver counts once

    Scenario: The same approver approving twice is refused
      Given alice has proposed deleting the doomed topic
      And bob has approved the proposal
      When bob tries to approve the proposal again
      Then the tool reports a refusal
      And the proposal holds 1 approval, from bob

  Rule: A withdrawn proposal authorizes nothing

    Scenario: A withdrawn proposal cannot be approved
      Given alice has proposed deleting the doomed topic
      And alice has withdrawn the proposal
      When bob tries to approve the proposal
      Then the tool reports a refusal

  Rule: A proposal lives as long as the operator asks, up to the cluster's cap

    Scenario Outline: The cluster caps how long a proposal stays open
      Given the cluster caps a proposal's lifetime at 1 hour
      When alice proposes deleting the doomed topic for <lifetime>
      Then the proposal expires <expiry> after it opened

      Examples:
        | case               | lifetime    | expiry     |
        | within the cap     | 30m         | 30 minutes |
        | above the cap      | 2h          | 1 hour     |
        | no lifetime given  | the default | 1 hour     |
        | zero takes the cap | 0           | 1 hour     |

  Rule: A lifetime needs a unit

    Scenario: A lifetime with no unit is refused before it reaches the broker
      When alice tries to propose deleting the doomed topic for 30
      Then the tool refuses the command line
      And the cluster holds no proposal

  Rule: Each operator reaches the cluster as themselves

    @inprogress
    Scenario: An approver approves through a cluster that authenticates every operator
      Given alice has proposed deleting the doomed topic
      When bob approves the proposal by running krabka-guard as himself
      Then the proposal holds 1 approval, from bob
