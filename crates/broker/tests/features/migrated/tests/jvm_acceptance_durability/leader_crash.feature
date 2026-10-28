@docker
Feature: Replicated topics remain readable after losing a leader during writes
  Scenario: Committed reads recover after partition leader loss during a producer burst
    Given a three replica topic and consumer coordinator are ready in a cluster with accelerated failure detection
    When a hundred record all acknowledgements producer races with the partition leader shutting down after fifty milliseconds
    And a committed consumer reads one record through a surviving broker after election settles
    Then the surviving cluster serves at least one committed record
