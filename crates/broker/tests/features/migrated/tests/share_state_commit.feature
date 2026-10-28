Feature: Share-state replies require replicated state

  Scenario: a share state request is answered only when committed
    Given orders has a three-replica share-state partition whose ISR never shrinks during the scenario
    When the coordinator initializes state epoch 0 and writes start offset 0 with all replicas available
    Then both complete share-state responses report success with no error message
    When a follower crashes while staying in the ISR and the coordinator writes offset 10, reads, initializes epoch 1 and deletes state
    Then all four complete responses report coordinator unavailable code 15 and The request timed out.
