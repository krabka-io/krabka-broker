Feature: tests diskless e2e batched

  Scenario: the krabka consumer reads every batched record from earliest
    Then diskless e2e batched: the krabka consumer reads every batched record from earliest

  Scenario: a follower restarted under load rejoins the isr
    Then diskless e2e batched: a follower restarted under load rejoins the isr
