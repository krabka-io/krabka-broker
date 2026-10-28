Feature: tests jvm acceptance reassign tool output

  Scenario: tests: each of the three election outcomes is read off its own line
    Then jvm acceptance reassign tool output tests: each of the three election outcomes is read off its own line

  Scenario: tests: a hyphenated topic keeps its hyphens
    Then jvm acceptance reassign tool output tests: a hyphenated topic keeps its hyphens

  Scenario: tests: a cancel reports the partitions it cancelled
    Then jvm acceptance reassign tool output tests: a cancel reports the partitions it cancelled

  Scenario: tests: a one partition cancel is read out of the singular sentence
    Then jvm acceptance reassign tool output tests: a one partition cancel is read out of the singular sentence

  Scenario: tests: generate yields the current assignment and the proposed one
    Then jvm acceptance reassign tool output tests: generate yields the current assignment and the proposed one

  Scenario: tests: the partitions of a generated plan sort the same however they were printed
    Then jvm acceptance reassign tool output tests: the partitions of a generated plan sort the same however they were printed

  Scenario: tests: every document the tools read round trips through its own parser
    Then jvm acceptance reassign tool output tests: every document the tools read round trips through its own parser
