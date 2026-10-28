Feature: tests snapshot

  Scenario: public snapshot codec round trips metadata
    Then snapshot: public snapshot codec round trips metadata

  Scenario: the snapshot image keeps the quorum controls
    Then snapshot: the snapshot image keeps the quorum controls

  Scenario: snapshot then restart recovers image
    Then snapshot: snapshot then restart recovers image
