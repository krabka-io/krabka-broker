Feature: Generated consensus failure documentation
  Scenario: Every failure trace explains its invariant and outcome
    Given the deterministic consensus failure traces
    When the failure trace slideshow is rendered
    Then the slideshow includes reordered and duplicate delivery with invariants and outcomes

  Scenario: Every failure trace has a generated message diagram
    Given the deterministic consensus failure traces
    When the failure trace slideshow is rendered
    Then three generated diagrams and the split-brain contrast are present
