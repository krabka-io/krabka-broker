Feature: Frequent rolls allow steady-state logs to be reclaimed

  Scenario: the soak settings reclaim what kafkas defaults let climb
    Given one broker retains Kafka default rolls and another rolls, snapshots and retains metadata every 100 milliseconds
    When each broker receives two halves of 300 offset commits spaced 10 milliseconds apart with sizes sampled every 20 commits
    Then offsets and metadata both grow at defaults and their median rise with reclamation is less than one quarter of the default rise
