Feature: Principal-bound break-glass approvals and consumption

  Scenario: an expired proposal authorizes nothing
    Given a two-approval broker with users alice,bob,carol,mallory, topic doomed, and signed actions none
    When alice proposes action 6 on doomed with lifetime 500 ms
    Then the proposal response has error 0
    When the current proposal expiry passes with safety margin 400 ms
    And bob approves the current proposal without a signature
    Then the approval response has error 44
    And the approval error message contains expired
    When alice proposes action 6 on doomed with lifetime 3000 ms
    Then the proposal response has error 0
    When bob approves the current proposal without a signature
    Then the approval response has error 0
    When carol approves the current proposal without a signature
    Then the approval response has error 0
    When alice describes the current proposal
    Then the stored proposal holds 2 approvals and is unconsumed
    When the current proposal expiry passes with safety margin 400 ms
    And alice deletes the topic
    Then the deletion has error 44 and topic existence is true
    When alice describes the current proposal
    Then the stored proposal holds 2 approvals and is unconsumed
