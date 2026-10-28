Feature: Trunk topic defaults remain unavailable in released mode
  A stored cluster default cannot activate an unknown trunk topic key on a
  released broker, but becomes effective once unstable APIs are enabled.

  Scenario: A trunk broker synonym reaches topic defaults only after opt-in
    Given cluster-default broker key log.remote.copy.lag.ms is 5000 for topic key remote.copy.lag.ms
    When topic defaults are inherited in released and unstable API modes
    Then released mode inherits no trunk default and unstable mode inherits 5000
