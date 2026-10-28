Feature: Transaction state follows coordinator partition leadership

  Scenario: a transaction commits through the next coordinator
    Given a three-replica transaction coordinator with an acknowledged open a,b,c transaction
    When the transaction coordinator stops and surviving replicas elect new state and data leaders
    And the producer retries its commit through the new transaction coordinator
    Then the recovered commit succeeds and advances the producer epoch by one
    When a nontransactional z is appended and a committed reader reads through it
    Then the committed reader sees a,b,c,z in order after coordinator recovery
