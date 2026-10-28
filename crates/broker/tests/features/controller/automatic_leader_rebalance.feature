Feature: Automatic leader rebalance restores a preferred replica after it rejoins

  Scenario: A rejoined preferred replica regains leadership through the background ticker
    Given three static plaintext voters enable automatic leader rebalance every second and foo-rebalance partition 0 is led by preferred replica 1 with replicas 1 and 2
    When preferred broker 1 stops and rejoins its static quorum until its replica returns to the ISR
    Then the background rebalance ticker restores broker 1 as foo-rebalance partition 0 leader without an operator RPC
