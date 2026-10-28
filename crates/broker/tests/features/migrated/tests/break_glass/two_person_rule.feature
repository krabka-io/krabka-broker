Feature: Principal-bound break-glass approvals and consumption

  Scenario: a proposal two approvals and the transition they authorize
    Given a two-approval broker with users alice,bob,carol,mallory, topic doomed, and signed actions none
    When alice proposes action 6 on doomed with lifetime 0 ms
    Then the proposal response has error 0
    When bob approves the current proposal without a signature
    Then the approval is exactly success with 1 approvals held and 2 required
    When carol approves the current proposal without a signature
    Then the approval is exactly success with 2 approvals held and 2 required
    When alice describes the current proposal
    Then the stored proposal holds 2 approvals and is unconsumed
    And the proposer is alice and unsigned approvals name bob,carol in order
    When alice deletes the topic
    Then the deletion has error 0 and topic existence is false
    When alice describes the current proposal
    Then only the stored proposal consumption timestamp changes to a nonzero value

  Scenario: one principal cannot stand in for two
    Given a two-approval broker with users alice,bob,carol,mallory, topic doomed, and signed actions none
    When alice proposes action 6 on doomed with lifetime 0 ms
    Then the proposal response has error 0
    When bob approves the current proposal without a signature
    Then the approval response has error 0
    When bob approves the current proposal without a signature
    Then the approval response has error 1007
    And the approval retains 1 approvals and its error names principal bob
    When alice describes the current proposal
    Then the stored proposal holds 1 approvals and is unconsumed
    When alice deletes the topic
    Then the deletion has error 44 and topic existence is true

  Scenario: the proposer cannot approve their own proposal
    Given a two-approval broker with users alice,bob,carol,mallory, topic doomed, and signed actions none
    When alice proposes action 6 on doomed with lifetime 0 ms
    Then the proposal response has error 0
    When alice approves the current proposal without a signature
    Then the approval response has error 1007
    And the approval response holds 0 approvals
    When alice describes the current proposal
    Then the stored proposal holds 0 approvals and is unconsumed

  Scenario: a principal outside the approver set is refused
    Given a two-approval broker with users alice,bob,carol,mallory, topic doomed, and signed actions none
    When mallory proposes action 6 on doomed with lifetime 0 ms
    Then the proposal response has error 1008
    When alice proposes action 6 on doomed with lifetime 0 ms
    Then the proposal response has error 0
    When mallory approves the current proposal without a signature
    Then the approval response has error 1008
    And the approval retains 0 approvals and its error names principal mallory
    When alice describes the current proposal
    Then the stored proposal holds 0 approvals and is unconsumed

  Scenario: an approval joins to the login that made it
    Given a two-approval broker with users alice,bob,carol,mallory, topic doomed, and signed actions none
    When alice proposes action 6 on doomed with lifetime 0 ms
    Then the proposal response has error 0
    When bob approves the current proposal without a signature
    Then the approval is exactly success with 1 approvals held and 2 required
    When alice reads audit records through operation delete_topic.approved
    Then the delete_topic.approved privileged action is attributed to bob and joins a successful login by principal and peer
