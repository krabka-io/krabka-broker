Feature: Published consensus fault demonstrations
  The playground and generated documentation consume a catalogue of deterministic
  partition, reordered-delivery and duplicate-message recordings.

  Background:
    Given the built-in catalogue of consensus fault demonstrations

  Scenario: The catalogue contains all three curated fault demonstrations
    When the operator generates the curated consensus traces
    Then the catalogue contains exactly three demonstrations

  Scenario: Every demonstration exposes a recorded event timeline
    When the operator generates the curated consensus traces
    Then every curated demonstration records at least one event

  Scenario: The partition demonstration ends after leadership reconverges
    When the operator generates the curated consensus traces
    Then the healed partition demonstration finishes with exactly one leader
