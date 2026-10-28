Feature: tests feature finalization

  Scenario: an unstable metadata version is refused by default
    Then feature finalization: an unstable metadata version is refused by default

  Scenario: finalizes metadata version and surfaces in api versions
    Then feature finalization: finalizes metadata version and surfaces in api versions

  Scenario: share version two is refused by default
    Then feature finalization: share version two is refused by default

  Scenario: share version two is finalizable under unstable feature versions
    Then feature finalization: share version two is finalizable under unstable feature versions

  Scenario: rejects unsupported feature
    Then feature finalization: rejects unsupported feature

  Scenario: rejects level above supported max
    Then feature finalization: rejects level above supported max

  Scenario: validate only does not persist
    Then feature finalization: validate only does not persist

  Scenario: rejects level below min floor
    Then feature finalization: rejects level below min floor
