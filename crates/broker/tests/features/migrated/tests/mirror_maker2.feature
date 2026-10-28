Feature: tests mirror maker2

  Scenario: mirror maker2 migrates a kafka cluster onto krabka
    Then mirror maker2: mirror maker2 migrates a kafka cluster onto krabka

  Scenario: tests: a produced line parses back into the same record
    Then mirror maker2 tests: a produced line parses back into the same record

  Scenario: tests: a line that is not a record is not read as one
    Then mirror maker2 tests: a line that is not a record is not read as one

  Scenario: tests: a record without headers parses as the tools render it
    Then mirror maker2 tests: a record without headers parses as the tools render it

  Scenario: tests: each mirror maker internal topic is recognised by its shape
    Then mirror maker2 tests: each mirror maker internal topic is recognised by its shape

  Scenario: tests: the partition count is read out of a topic description
    Then mirror maker2 tests: the partition count is read out of a topic description

  Scenario: tests: a config value is not taken from the synonyms field
    Then mirror maker2 tests: a config value is not taken from the synonyms field

  Scenario: tests: a groups committed offset is read off its own row
    Then mirror maker2 tests: a groups committed offset is read off its own row

  Scenario: tests: an uncommitted group reads as no offset rather than zero
    Then mirror maker2 tests: an uncommitted group reads as no offset rather than zero

  Scenario: tests: a log excerpt keeps the cause a long shutdown pushed out of the tail
    Then mirror maker2 tests: a log excerpt keeps the cause a long shutdown pushed out of the tail

  Scenario: tests: a log excerpt is bounded however long the log is
    Then mirror maker2 tests: a log excerpt is bounded however long the log is

  Scenario: tests: an empty log excerpts to a count of none
    Then mirror maker2 tests: an empty log excerpts to a count of none

  Scenario: tests: a container state is read out of the inspect rendering
    Then mirror maker2 tests: a container state is read out of the inspect rendering

  Scenario: tests: the bridge gateway falls back to the subnets first address
    Then mirror maker2 tests: the bridge gateway falls back to the subnets first address
