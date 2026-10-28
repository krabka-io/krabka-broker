Feature: Principal-bound break-glass approvals and consumption

  Scenario: a signed action takes a signature on every approval
    Given a two-approval broker with users alice,bob,carol,mallory, topic doomed, and signed actions delete_topic
    When alice proposes action 6 on doomed with lifetime 0 ms
    Then the proposal response has error 0
    When bob approves the current proposal without a signature
    Then the approval response has error 1010
    When bob approves with their signature first byte XOR 1
    Then the approval response has error 1009
    When alice describes the current proposal
    Then the stored proposal holds 0 approvals and is unconsumed
    When bob approves with their operator signature
    Then the approval response has error 0
    When carol approves with their operator signature
    Then the approval response has error 0
    When alice describes the current proposal
    Then the stored proposal holds 2 approvals and is unconsumed
    And stored approvals carry nonempty signatures and the operator key ids for bob,carol in order
    When alice deletes the topic
    Then the deletion has error 0 and topic existence is false
