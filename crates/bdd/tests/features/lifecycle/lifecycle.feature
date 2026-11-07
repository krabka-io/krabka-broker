Feature: Lifecycle tags keep unfinished scenarios out of the gating run

  Scenario: A finished scenario runs
    When a scenario reaches its outcome
    Then the outcome holds

  @inprogress
  Scenario: A scenario in progress is ignored rather than run
    When a scenario reaches its outcome
    Then the behavior it documents is not built yet
