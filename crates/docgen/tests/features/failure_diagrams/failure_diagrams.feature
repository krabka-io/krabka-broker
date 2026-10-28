Feature: Generated consensus failure documentation
  Background:
    Given the deterministic consensus failure traces

  Scenario: Every failure trace explains its invariant and outcome
    When the failure trace slideshow is rendered
    Then the slideshow includes reordered and duplicate delivery with invariants and outcomes

  Scenario: Every failure trace has a generated message diagram
    When the failure trace slideshow is rendered
    Then three generated diagrams and the split-brain contrast are present
