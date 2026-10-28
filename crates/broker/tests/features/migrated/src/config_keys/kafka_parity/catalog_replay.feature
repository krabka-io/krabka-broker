Feature: MirrorMaker topic configuration replay compatibility
  MirrorMaker replays a source topic's entire property set in one alteration.
  Every released Kafka key remains accepted, while trunk keys require opt-in.

  Scenario: Every released Kafka property survives both individual and whole-map validation
    Given the topic properties captured from released Apache Kafka 4.3.1
    When the broker validates each released property and the complete released alteration
    Then every captured released property and the complete alteration are accepted

  Scenario: Trunk topic properties are unknown without unstable API opt-in
    Given the Kafka trunk topic properties together with the released property corpus
    And topic alteration unstable API versions are false
    When the broker validates trunk properties and their complete MirrorMaker alteration
    Then every trunk key is refused with Kafka's exact unknown-topic-config message
    And the release-only validator refuses an alteration containing trunk properties

  Scenario: Opting in admits the complete trunk property set without changing its values
    Given the Kafka trunk topic properties together with the released property corpus
    And topic alteration unstable API versions are true
    When the broker validates trunk properties and their complete MirrorMaker alteration
    Then every trunk property and the complete opted-in alteration preserve their values
    And the release-only validator refuses an alteration containing trunk properties
