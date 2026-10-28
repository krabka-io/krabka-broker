Feature: Formatting feature overrides survive broker boot
  Scenario: standalone format feature overrides surface in api versions
    Given a standalone log formatted with transaction version 1 and group version 0
    When the broker boots from the formatted voter and feature records
    And a client discovers the booted broker feature levels
    Then feature discovery succeeds
    And transaction version remains finalized at 1
    And group version 0 is omitted from finalized features
    And metadata version uses the latest production default
