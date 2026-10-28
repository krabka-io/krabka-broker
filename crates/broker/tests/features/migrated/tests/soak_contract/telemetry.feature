Feature: tests soak contract telemetry

  Scenario Outline: tests: a size reads in bytes
    Then soak contract telemetry tests: a size reads in bytes for <example>

    Examples:
      | example         |
      | mebibytes       |
      | gibibytes       |
      | kibibytes       |
      | kilobytes       |
      | upper kilobytes |
      | megabytes       |
      | bytes           |
      | bare number     |
      | padded          |
      | no number       |
      | unknown unit    |
      | two points      |
      | placeholder     |

  Scenario Outline: tests: a percentage reads as a number
    Then soak contract telemetry tests: a percentage reads as a number for <example>

    Examples:
      | example       |
      | plain         |
      | over one core |
      | zero          |
      | placeholder   |
      | no sign       |

  Scenario: tests: docker stats reads by container name
    Then soak contract telemetry tests: docker stats reads by container name

  Scenario: tests: a body reads as controller progress
    Then soak contract telemetry tests: a body reads as controller progress

  Scenario: tests: a body without the series reads as unknown
    Then soak contract telemetry tests: a body without the series reads as unknown

  Scenario Outline: tests: a gauge is its first sample
    Then soak contract telemetry tests: a gauge is its first sample for <example>

    Examples:
      | example      |
      | integer      |
      | negative     |
      | float        |
      | labelled     |
      | prefix only  |
      | comment      |
      | not a number |
      | absent       |

  Scenario Outline: tests: the raft state is the label that reads one
    Then soak contract telemetry tests: the raft state is the label that reads one for <example>

    Examples:
      | example     |
      | follower    |
      | none set    |
      | broker only |

  Scenario: tests: rows fold into peaks gaps and controller changes
    Then soak contract telemetry tests: rows fold into peaks gaps and controller changes

  Scenario: tests: lag before the first record is not counted
    Then soak contract telemetry tests: lag before the first record is not counted

  Scenario: tests: an unanswering node still gets a row for every sample
    Then soak contract telemetry tests: an unanswering node still gets a row for every sample

  Scenario: tests: a cluster of no nodes starts and stops
    Then soak contract telemetry tests: a cluster of no nodes starts and stops
