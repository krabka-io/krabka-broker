Feature: Operators read back each proposal and the evidence behind its approvals

  Depends on: A break-glass proposal needs two different people - ../two_person_rule/two_person_rule.feature

  Rule: The report names the state of each proposal

    Scenario: A proposal nobody has acted on reads as open
      Given alice has proposed deleting the doomed topic
      When bob lists the proposals
      Then the report shows the proposal as open

    Scenario: A withdrawn proposal reads as withdrawn
      Given alice has proposed deleting the doomed topic
      And alice has withdrawn the proposal
      When bob lists the proposals
      Then the report shows the proposal as withdrawn

    Scenario: A proposal that lifted a freeze reads as consumed
      Given alice has lifted a freeze on the payments topic with an approved proposal
      When bob lists the proposals
      Then the report shows the proposal as consumed

  Rule: Each approval shows the evidence of who made it

    Scenario: A signed approval names the key that signed it
      Given alice has proposed deleting the doomed topic
      And bob has approved the proposal signed with bob's key
      When carol lists the proposals
      Then the report shows the approval from bob as signed by bob-yubi

    Scenario: An unsigned approval says it is unsigned
      Given alice has proposed deleting the doomed topic
      And bob has approved the proposal
      When carol lists the proposals
      Then the report shows the approval from bob as unsigned
