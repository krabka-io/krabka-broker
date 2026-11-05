Feature: MirrorMaker topic configuration replay compatibility
  MirrorMaker replays a source topic's entire property set in one alteration.
  Every released Kafka key remains accepted without opt-in.

  Scenario: Every released Kafka property survives both individual and whole-map validation
    Given the topic properties captured from released Apache Kafka 4.3.1
    When the broker validates each released property and the complete released alteration
    Then every captured released property and the complete alteration are accepted

  Scenario: Released 4.4 topic properties require no unstable API opt-in
    Given the Kafka trunk topic properties together with the released property corpus
    And topic alteration unstable API versions are false
    When the broker validates trunk properties and their complete MirrorMaker alteration
    Then every trunk property and the complete opted-in alteration preserve their values
    And the release validator accepts the complete released 4.4 alteration

  Scenario: Opting in admits the complete trunk property set without changing its values
    Given the Kafka trunk topic properties together with the released property corpus
    And topic alteration unstable API versions are true
    When the broker validates trunk properties and their complete MirrorMaker alteration
    Then every trunk property and the complete opted-in alteration preserve their values
    And the release validator accepts the complete released 4.4 alteration
