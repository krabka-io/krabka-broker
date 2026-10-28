Feature: SASL credentials accompany transactional coordinator connections
  Scenario: sasl authenticated transactional flow commits
    Given a SASL PLAIN broker authenticates alice and hosts sasl-txn
    When Alice initializes a transactional producer and commits records a,b,c with group offsets
    And an authenticated read_committed verifier polls the committed transaction
    Then the authenticated transaction exposes records a,b,c to read_committed
